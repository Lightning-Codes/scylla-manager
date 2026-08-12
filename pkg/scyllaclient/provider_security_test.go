// Copyright (C) 2026 ScyllaDB

package scyllaclient

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
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
