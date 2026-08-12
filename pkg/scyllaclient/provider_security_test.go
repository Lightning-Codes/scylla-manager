// Copyright (C) 2026 ScyllaDB

package scyllaclient

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestCachedProviderInvalidateClosesIdleTransport(t *testing.T) {
	closed := make(chan struct{}, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			select {
			case closed <- struct{}{}:
			default:
			}
		}
	}
	server.Start()
	defer server.Close()

	transport := DefaultTransport()
	response, err := (&http.Client{Transport: transport}).Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()

	client := &Client{config: Config{Transport: transport}}
	provider := NewCachedProvider(func(context.Context, uuid.UUID) (*Client, error) {
		return client, nil
	}, time.Hour, log.NewDevelopment())
	clusterID := uuid.MustRandom()
	if _, err := provider.Client(context.Background(), clusterID); err != nil {
		t.Fatal(err)
	}
	provider.Invalidate(clusterID)

	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("invalidating cached client did not close its idle transport")
	}
}

func TestCachedProviderInvalidateRevokesOutstandingClient(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	transport := DefaultTransport()
	config := TestConfig([]string{"127.0.0.1"}, "token")
	config.Transport = transport
	client, err := NewClient(config, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}
	provider := NewCachedProvider(func(context.Context, uuid.UUID) (*Client, error) {
		return client, nil
	}, time.Hour, log.NewDevelopment())
	clusterID := uuid.MustRandom()
	outstanding, err := provider.Client(context.Background(), clusterID)
	if err != nil {
		t.Fatal(err)
	}
	provider.Invalidate(clusterID)

	req, err := http.NewRequest(http.MethodGet, server.URL, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	_, err = outstanding.transport.RoundTrip(req)
	if !errors.Is(err, ErrClientClosed) {
		t.Fatalf("outstanding client was not revoked: %v", err)
	}
	if requests != 0 {
		t.Fatalf("revoked client reached the network %d times", requests)
	}
}

func TestCachedProviderInvalidateRetainsStableCell(t *testing.T) {
	clusterID := uuid.MustRandom()
	provider := NewCachedProvider(func(context.Context, uuid.UUID) (*Client, error) {
		return &Client{}, nil
	}, time.Hour, log.NewDevelopment())
	cell := provider.getClientTTL(clusterID)
	provider.Invalidate(clusterID)
	if got := provider.getClientTTL(clusterID); got != cell {
		t.Fatal("invalidation detached the per-cluster cache cell")
	}
}

func TestCachedProviderRevokesEveryReplacedGeneration(t *testing.T) {
	newClient := func() *Client {
		transport := newRevocableTransport(http.DefaultTransport)
		return &Client{transport: transport}
	}
	var generations []*Client
	provider := NewCachedProvider(func(context.Context, uuid.UUID) (*Client, error) {
		client := newClient()
		generations = append(generations, client)
		return client, nil
	}, time.Nanosecond, log.NewDevelopment())
	id := uuid.MustRandom()
	first, err := provider.Client(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	second, err := provider.Client(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if first == second || len(generations) != 2 {
		t.Fatal("client generation was not replaced")
	}
	provider.Invalidate(id)

	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	for i, client := range []*Client{first, second} {
		if _, err := client.transport.RoundTrip(req); !errors.Is(err, ErrClientClosed) {
			t.Fatalf("generation %d was not revoked: %v", i, err)
		}
	}
}

func generationTestClient(generation uuid.UUID) *Client {
	return &Client{
		config:    Config{ConnectionGeneration: generation},
		transport: newRevocableTransport(http.DefaultTransport),
	}
}

func generationEpochTestClient(generation uuid.UUID, epoch int64) *Client {
	client := generationTestClient(generation)
	client.config.ConnectionLifecycleEpoch = epoch
	return client
}

func TestCachedProviderHigherEpochCannotReopenRetiredClusterID(t *testing.T) {
	id, a, d, c := uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom()
	provider := NewCachedProvider(nil, time.Hour, log.NewDevelopment())
	provider.ResetClusterEpoch(id, a, 1)
	provider.RevokeClusterEpoch(id, d, 2)
	called := false
	_, err := provider.ClientForGenerationValidatedEpoch(context.Background(), id, c, 3,
		func() (*Client, error) {
			called = true
			return generationEpochTestClient(c, 3), nil
		}, func() error { return nil })
	if !errors.Is(err, ErrClientClosed) || called {
		t.Fatalf("retired cluster ID was reopened: called=%v err=%v", called, err)
	}
}

func TestCachedProviderDelayedLowerEpochValidationCannotResurrect(t *testing.T) {
	id, a, d := uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom()
	provider := NewCachedProvider(nil, time.Hour, log.NewDevelopment())
	provider.ResetClusterEpoch(id, a, 1)
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	result := make(chan error, 1)
	go func() {
		_, err := provider.ClientForGenerationValidatedEpoch(context.Background(), id, a, 1,
			func() (*Client, error) { return generationEpochTestClient(a, 1), nil },
			func() error {
				startedOnce.Do(func() { close(started) })
				<-release
				return nil
			})
		result <- err
	}()
	<-started
	revoked := make(chan struct{})
	go func() {
		provider.RevokeClusterEpoch(id, d, 2)
		close(revoked)
	}()
	for i := 0; i < 1000 && provider.generationAllowed(id, a); i++ {
		time.Sleep(time.Microsecond)
	}
	close(release)
	if err := <-result; !errors.Is(err, ErrClientClosed) {
		t.Fatalf("pre-delete epoch validation survived tombstone: %v", err)
	}
	<-revoked
}

func TestCachedProviderIndeterminateDeleteCanHealFromFreshOldPointerProof(t *testing.T) {
	id, a, candidateD := uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom()
	provider := NewCachedProvider(nil, time.Hour, log.NewDevelopment())
	provider.ResetClusterEpoch(id, a, 1)
	provider.ProvisionallyRevokeCluster(id, candidateD)
	if provider.generationAllowed(id, a) {
		t.Fatal("provisional delete did not fail closed")
	}
	if _, err := provider.ClientForGenerationValidatedEpoch(context.Background(), id, a, 1,
		func() (*Client, error) { return generationEpochTestClient(a, 1), nil },
		func() error { return nil },
	); err != nil {
		t.Fatalf("fresh authoritative old-pointer proof did not heal not-applied delete: %v", err)
	}
	if !provider.generationAllowed(id, a) {
		t.Fatal("healed generation remained blocked")
	}
}

func TestCachedProviderRetiresGenerationBeforeFirstAcquisition(t *testing.T) {
	id, generation := uuid.MustRandom(), uuid.MustRandom()
	provider := NewCachedProvider(nil, time.Hour, log.NewDevelopment())
	provider.ResetCluster(id, generation)
	provider.InvalidateGeneration(id, generation)
	called := false
	_, err := provider.ClientForGeneration(context.Background(), id, generation, func() (*Client, error) {
		called = true
		return generationTestClient(generation), nil
	})
	if !errors.Is(err, ErrClientClosed) {
		t.Fatalf("retired generation was accepted: %v", err)
	}
	if called {
		t.Fatal("retired generation invoked its client constructor")
	}
}

func TestCachedProviderRevokeClusterWinsPausedConstruction(t *testing.T) {
	id, generation := uuid.MustRandom(), uuid.MustRandom()
	provider := NewCachedProvider(nil, time.Hour, log.NewDevelopment())
	provider.ResetCluster(id, generation)
	constructorStarted := make(chan struct{})
	releaseConstructor := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := provider.ClientForGeneration(context.Background(), id, generation, func() (*Client, error) {
			close(constructorStarted)
			<-releaseConstructor
			return generationTestClient(generation), nil
		})
		result <- err
	}()
	<-constructorStarted
	revoked := make(chan struct{})
	go func() {
		provider.RevokeCluster(id, uuid.MustRandom())
		close(revoked)
	}()
	// RevokeCluster publishes the lifecycle tombstone before waiting for the
	// in-progress generation cell.
	for i := 0; i < 1000 && provider.generationAllowed(id, generation); i++ {
		time.Sleep(time.Microsecond)
	}
	if provider.generationAllowed(id, generation) {
		t.Fatal("cluster revocation tombstone was not published")
	}
	close(releaseConstructor)
	if err := <-result; !errors.Is(err, ErrClientClosed) {
		t.Fatalf("paused constructor survived deletion: %v", err)
	}
	<-revoked
}

func TestCachedProviderResetCannotReopenRevokedID(t *testing.T) {
	id := uuid.MustRandom()
	oldGeneration, newGeneration := uuid.MustRandom(), uuid.MustRandom()
	provider := NewCachedProvider(nil, time.Hour, log.NewDevelopment())
	provider.ResetCluster(id, oldGeneration)
	provider.RevokeCluster(id, uuid.MustRandom())
	provider.ResetCluster(id, newGeneration)

	oldCalled := false
	_, err := provider.ClientForGeneration(context.Background(), id, oldGeneration, func() (*Client, error) {
		oldCalled = true
		return generationTestClient(oldGeneration), nil
	})
	if !errors.Is(err, ErrClientClosed) || oldCalled {
		t.Fatalf("old lifecycle was re-authorized: called=%v err=%v", oldCalled, err)
	}
	newCalled := false
	if _, err := provider.ClientForGeneration(context.Background(), id, newGeneration, func() (*Client, error) {
		newCalled = true
		return generationTestClient(newGeneration), nil
	}); !errors.Is(err, ErrClientClosed) || newCalled {
		t.Fatalf("revoked ID was reopened: called=%v err=%v", newCalled, err)
	}
}

func TestCachedProviderStaleValidationCannotResurrectRevokedCluster(t *testing.T) {
	id, generation := uuid.MustRandom(), uuid.MustRandom()
	provider := NewCachedProvider(nil, time.Hour, log.NewDevelopment())
	provider.ResetCluster(id, generation)
	validationStarted := make(chan struct{})
	releaseValidation := make(chan struct{})
	var validationStartedOnce sync.Once
	result := make(chan error, 1)
	go func() {
		_, err := provider.ClientForGenerationValidated(context.Background(), id, generation,
			func() (*Client, error) { return generationTestClient(generation), nil },
			func() error {
				validationStartedOnce.Do(func() { close(validationStarted) })
				<-releaseValidation
				return nil // a stale authoritative read that started before delete
			},
		)
		result <- err
	}()
	<-validationStarted
	revoked := make(chan struct{})
	go func() {
		provider.RevokeCluster(id, uuid.MustRandom())
		close(revoked)
	}()
	for i := 0; i < 1000 && provider.generationAllowed(id, generation); i++ {
		time.Sleep(time.Microsecond)
	}
	close(releaseValidation)
	if err := <-result; !errors.Is(err, ErrClientClosed) {
		t.Fatalf("stale validation resurrected deleted generation: %v", err)
	}
	<-revoked
	if provider.generationAllowed(id, generation) {
		t.Fatal("deleted lifecycle was re-authorized")
	}
}

func TestCachedProviderAuthoritativeValidationObservesRemoteRotation(t *testing.T) {
	id, a, b := uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom()
	provider := NewCachedProvider(nil, time.Hour, log.NewDevelopment())
	provider.ResetCluster(id, a) // replica started while A was active
	var mu sync.Mutex
	active := b // another replica committed B
	validate := func(generation uuid.UUID) func() error {
		return func() error {
			mu.Lock()
			defer mu.Unlock()
			if active != generation {
				return errors.New("generation is not active")
			}
			return nil
		}
	}
	if _, err := provider.ClientForGenerationValidated(context.Background(), id, b,
		func() (*Client, error) { return generationTestClient(b), nil }, validate(b)); err != nil {
		t.Fatalf("remote generation B was not authorized: %v", err)
	}
	if _, err := provider.ClientForGenerationValidated(context.Background(), id, a,
		func() (*Client, error) { return generationTestClient(a), nil }, validate(a)); err == nil {
		t.Fatal("remote rotation still handed out generation A")
	}
	mu.Lock()
	active = uuid.Nil // durable remote deletion tombstone is not A or B
	mu.Unlock()
	if _, err := provider.ClientForGenerationValidated(context.Background(), id, b,
		func() (*Client, error) { return generationTestClient(b), nil }, validate(b)); err == nil {
		t.Fatal("remote deletion still handed out generation B")
	}
}
