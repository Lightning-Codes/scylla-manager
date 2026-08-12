// Copyright (C) 2026 ScyllaDB

package cluster

import (
	"context"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/secrets"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func fullTestBundle(t *testing.T, id, generation uuid.UUID, marker string) *secrets.ConnectionBundle {
	t.Helper()
	ca, err := os.ReadFile("testdata/cluster.crt")
	if err != nil {
		t.Fatal(err)
	}
	key, err := os.ReadFile("testdata/cluster.key")
	if err != nil {
		t.Fatal(err)
	}
	return &secrets.ConnectionBundle{
		ClusterID:                 id,
		Generation:                generation,
		LifecycleEpoch:            1,
		Host:                      marker + ".host",
		KnownHosts:                []string{marker + ".known-1", marker + ".known-2"},
		Port:                      len(marker) + 10000,
		AuthToken:                 marker + ".token",
		ForceTLSDisabled:          false,
		ForceNonSSLSessionPort:    marker == "new",
		CQLUsername:               marker + ".user",
		CQLPassword:               marker + ".password",
		CQLClientCertificate:      slices.Clone(ca),
		CQLClientPrivateKey:       slices.Clone(key),
		CQLCA:                     slices.Clone(ca),
		CQLServerName:             marker + ".cql.internal",
		AlternatorAccessKeyID:     marker + ".access",
		AlternatorSecretAccessKey: marker + ".secret",
		AlternatorCA:              slices.Clone(ca),
		AlternatorServerName:      marker + ".alternator.internal",
		AgentCA:                   slices.Clone(ca),
		AgentServerName:           marker + ".agent.internal",
	}
}

func TestCommitConnectionGenerationStagesBeforeCAS(t *testing.T) {
	id, oldGeneration, candidateGeneration := uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom()
	candidate := &Cluster{ID: id}
	bundle := fullTestBundle(t, id, candidateGeneration, "candidate")
	connectionRepository := securityMemoryStore{}
	sentinel := errors.New("pointer CAS rejected")
	var events []string
	s := &Service{
		stageConnectionBundle: func(_ context.Context, b *secrets.ConnectionBundle) error {
			events = append(events, "stage")
			return connectionRepository.Put(b)
		},
		swapConnectionGeneration: func(_ context.Context, c *Cluster, expected uuid.UUID, create bool) error {
			events = append(events, "CAS")
			if expected != oldGeneration || create {
				t.Fatalf("unexpected CAS inputs: expected=%s create=%v", expected, create)
			}
			stored := secrets.NewConnectionBundle(id)
			stored.Generation = candidateGeneration
			if err := connectionRepository.Get(stored); err != nil {
				t.Fatalf("candidate was not staged before CAS: %v", err)
			}
			if c.ConnectionGeneration != candidateGeneration {
				t.Fatalf("candidate row did not point to staged generation: %s", c.ConnectionGeneration)
			}
			return sentinel
		},
	}
	err := s.commitConnectionGeneration(context.Background(), candidate, bundle, oldGeneration, false)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected pointer failure, got %v", err)
	}
	if !slices.Equal(events, []string{"stage", "CAS"}) {
		t.Fatalf("unsafe write ordering: %v", events)
	}
	stored := secrets.NewConnectionBundle(id)
	stored.Generation = candidateGeneration
	if err := connectionRepository.Get(stored); err != nil {
		t.Fatalf("orphan candidate must be retained after CAS failure: %v", err)
	}
}

func TestCommitConnectionGenerationStageFailureDoesNotTouchPointer(t *testing.T) {
	sentinel := errors.New("stage failed")
	swapCalled := false
	s := &Service{
		stageConnectionBundle: func(context.Context, *secrets.ConnectionBundle) error { return sentinel },
		swapConnectionGeneration: func(context.Context, *Cluster, uuid.UUID, bool) error {
			swapCalled = true
			return nil
		},
	}
	id, generation := uuid.MustRandom(), uuid.MustRandom()
	err := s.commitConnectionGeneration(context.Background(), &Cluster{ID: id}, fullTestBundle(t, id, generation, "candidate"), uuid.MustRandom(), false)
	if !errors.Is(err, sentinel) || swapCalled {
		t.Fatalf("stage failure touched pointer: swap=%v err=%v", swapCalled, err)
	}
}

func TestClassifyConnectionGenerationCAS(t *testing.T) {
	a, b, c := uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom()
	timeoutErr := context.DeadlineExceeded
	readErr := errors.New("serial read unavailable")
	tests := []struct {
		name    string
		applied bool
		casErr  error
		active  uuid.UUID
		readErr error
		want    error
	}{
		{name: "applied", applied: true},
		{name: "timeout applied resolved", casErr: timeoutErr, active: b},
		{name: "timeout not applied resolved", casErr: timeoutErr, active: a, want: timeoutErr},
		{name: "CAS conflict", active: c, want: ErrConnectionCommitConflict},
		{name: "timeout other generation", casErr: timeoutErr, active: c, want: ErrConnectionCommitIndeterminate},
		{name: "timeout read unavailable", casErr: timeoutErr, readErr: readErr, want: ErrConnectionCommitIndeterminate},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := classifyConnectionGenerationCAS(b, a, tc.applied, tc.casErr, tc.active, tc.readErr)
			if tc.want == nil && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("expected %v, got %v", tc.want, err)
			}
		})
	}
}

func TestConnectionResolutionIgnoresCancelledRequest(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()
	s := &Service{timeoutConfig: scyllaclient.TimeoutConfig{Timeout: time.Second}}
	ctx, cancel := s.connectionResolutionContext(parent)
	defer cancel()
	if err := ctx.Err(); err != nil {
		t.Fatalf("resolution inherited cancelled request: %v", err)
	}
}

func TestPostCommitContextRepinsCommittedGenerationAndDetachesCancellation(t *testing.T) {
	a := uuid.MustRandom()
	b := uuid.MustRandom()
	parent, cancelParent := context.WithCancel(WithExpectedConnectionGeneration(context.Background(), a))
	cancelParent()

	s := &Service{timeoutConfig: scyllaclient.TimeoutConfig{Timeout: time.Second}}
	ctx, cancel := s.postCommitConnectionContext(parent, b)
	defer cancel()
	if err := ctx.Err(); err != nil {
		t.Fatalf("post-commit context inherited request cancellation: %v", err)
	}
	got, ok := ExpectedConnectionGeneration(ctx)
	if !ok || got != b {
		t.Fatalf("post-commit context generation = %s, %t; expected %s", got, ok, b)
	}
}

func TestPostCommitReconciliationUsesCommittedGenerationForListenerAndSession(t *testing.T) {
	a := uuid.MustRandom()
	b := uuid.MustRandom()
	id := uuid.MustRandom()
	parent, cancelParent := context.WithCancel(WithExpectedConnectionGeneration(context.Background(), a))
	cancelParent()

	var listenerCalled, sessionCalled bool
	sessionSentinel := errors.New("session seam reached")
	s := &Service{
		timeoutConfig: scyllaclient.TimeoutConfig{Timeout: time.Second},
		onChangeListener: func(ctx context.Context, change Change) error {
			listenerCalled = true
			assertPostCommitGeneration(t, ctx, b)
			if change.ID != id || change.Type != Update {
				t.Fatalf("unexpected change: %+v", change)
			}
			return nil
		},
		postCommitSession: func(ctx context.Context, clusterID uuid.UUID) (gocqlx.Session, error) {
			sessionCalled = true
			assertPostCommitGeneration(t, ctx, b)
			if clusterID != id {
				t.Fatalf("session cluster ID = %s, expected %s", clusterID, id)
			}
			return gocqlx.Session{}, sessionSentinel
		},
	}
	listenerErr, sessionErr := s.reconcileCommittedConnection(parent, b, Change{ID: id, Type: Update})
	if listenerErr != nil {
		t.Fatal(listenerErr)
	}
	if !errors.Is(sessionErr, sessionSentinel) {
		t.Fatalf("session result = %v, expected %v", sessionErr, sessionSentinel)
	}
	if !listenerCalled || !sessionCalled {
		t.Fatalf("post-commit callbacks: listener=%t session=%t", listenerCalled, sessionCalled)
	}
}

func TestPostCommitDeletionDetachesCancellationAndUsesTombstoneGeneration(t *testing.T) {
	a := uuid.MustRandom()
	tombstone := uuid.MustRandom()
	id := uuid.MustRandom()
	parent, cancelParent := context.WithCancel(WithExpectedConnectionGeneration(context.Background(), a))
	cancelParent()

	listenerCalled := false
	s := &Service{
		timeoutConfig: scyllaclient.TimeoutConfig{Timeout: time.Second},
		onChangeListener: func(ctx context.Context, change Change) error {
			listenerCalled = true
			assertPostCommitGeneration(t, ctx, tombstone)
			if change.ID != id || change.Type != Delete {
				t.Fatalf("unexpected change: %+v", change)
			}
			return nil
		},
	}
	if err := s.notifyCommittedChange(parent, tombstone, Change{ID: id, Type: Delete}); err != nil {
		t.Fatal(err)
	}
	if !listenerCalled {
		t.Fatal("post-commit deletion listener was not called")
	}
}

func assertPostCommitGeneration(t *testing.T, ctx context.Context, expected uuid.UUID) {
	t.Helper()
	if err := ctx.Err(); err != nil {
		t.Fatalf("post-commit callback inherited request cancellation: %v", err)
	}
	got, ok := ExpectedConnectionGeneration(ctx)
	if !ok || got != expected {
		t.Fatalf("callback generation = %s, %t; expected %s", got, ok, expected)
	}
}

type blockingBundleStore struct {
	mu      sync.Mutex
	values  map[string][]byte
	block   string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingBundleStore) Put(v store.Entry) error {
	b, err := v.MarshalBinary()
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.values[securityStoreKey(v)] = slices.Clone(b)
	s.mu.Unlock()
	return nil
}

func (s *blockingBundleStore) Get(v store.Entry) error {
	key := securityStoreKey(v)
	if key == s.block {
		s.once.Do(func() { close(s.entered) })
		<-s.release
	}
	s.mu.Lock()
	b, ok := s.values[key]
	b = slices.Clone(b)
	s.mu.Unlock()
	if !ok {
		return util.ErrNotFound
	}
	return v.UnmarshalBinary(b)
}

func (s *blockingBundleStore) Check(v store.Entry) (bool, error) {
	s.mu.Lock()
	_, ok := s.values[securityStoreKey(v)]
	s.mu.Unlock()
	return ok, nil
}
func (s *blockingBundleStore) Delete(v store.Entry) error { return errors.New("not implemented") }
func (s *blockingBundleStore) DeleteAll(uuid.UUID) error  { return errors.New("not implemented") }

func TestConnectionSnapshotReaderSeesExactOldOrNewGeneration(t *testing.T) {
	id, a, b := uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom()
	store := &blockingBundleStore{
		values:  make(map[string][]byte),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	oldBundle := fullTestBundle(t, id, a, "old")
	newBundle := fullTestBundle(t, id, b, "new")
	if err := store.Put(oldBundle); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(newBundle); err != nil {
		t.Fatal(err)
	}
	store.block = securityStoreKey(oldBundle)
	s := &Service{connectionBundleStore: store}
	oldRow := &Cluster{ID: id, ConnectionGeneration: a}
	oldDone := make(chan error, 1)
	go func() { oldDone <- s.hydrateClusterConnection(oldRow) }()
	<-store.entered
	newRow := &Cluster{ID: id, ConnectionGeneration: b}
	if err := s.hydrateClusterConnection(newRow); err != nil {
		t.Fatal(err)
	}
	close(store.release)
	if err := <-oldDone; err != nil {
		t.Fatal(err)
	}
	assertBundleMarker := func(name string, row *Cluster, generation uuid.UUID) {
		t.Helper()
		if row.ConnectionGeneration != generation ||
			row.Host != name+".host" ||
			!slices.Equal(row.KnownHosts, []string{name + ".known-1", name + ".known-2"}) ||
			row.Port != len(name)+10000 ||
			row.AuthToken != name+".token" ||
			row.ForceNonSSLSessionPort != (name == "new") ||
			row.Username != name+".user" || row.Password != name+".password" ||
			len(row.SSLUserCertFile) == 0 || len(row.SSLUserKeyFile) == 0 ||
			len(row.CQLCAFile) == 0 || row.CQLServerName != name+".cql.internal" ||
			row.AlternatorAccessKeyID != name+".access" || row.AlternatorSecretAccessKey != name+".secret" ||
			len(row.AlternatorCAFile) == 0 || row.AlternatorServerName != name+".alternator.internal" ||
			len(row.AgentCAFile) == 0 || row.AgentServerName != name+".agent.internal" {
			t.Fatalf("mixed %s snapshot: %#v", name, row)
		}
	}
	assertBundleMarker("old", oldRow, a)
	assertBundleMarker("new", newRow, b)
}

func TestMissingActiveGenerationBundleFailsClosed(t *testing.T) {
	id, generation := uuid.MustRandom(), uuid.MustRandom()
	connectionStore := securityMemoryStore{}
	s := &Service{connectionBundleStore: connectionStore}
	row := &Cluster{ID: id, ConnectionGeneration: generation}
	err := s.hydrateClusterConnection(row)
	if !errors.Is(err, ErrConnectionGenerationMismatch) {
		t.Fatalf("missing active bundle did not fail closed: %v", err)
	}
}

func TestRuntimeNilGenerationFailsClosed(t *testing.T) {
	svc := &Service{}
	err := svc.hydrateClusterConnection(&Cluster{ID: uuid.MustRandom()})
	if !errors.Is(err, ErrConnectionGenerationMismatch) {
		t.Fatalf("nil runtime pointer was not blocked: %v", err)
	}
}

type atomicBundleRepository struct {
	mu      sync.Mutex
	active  uuid.UUID
	bundles map[uuid.UUID][]byte
}

func newAtomicBundleRepository(active uuid.UUID) *atomicBundleRepository {
	return &atomicBundleRepository{active: active, bundles: make(map[uuid.UUID][]byte)}
}

func (r *atomicBundleRepository) stage(_ context.Context, b *secrets.ConnectionBundle) error {
	data, err := b.MarshalBinary()
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.bundles[b.Generation]; ok && !slices.Equal(existing, data) {
		return ErrConnectionCommitConflict
	}
	r.bundles[b.Generation] = slices.Clone(data)
	return nil
}

func (r *atomicBundleRepository) swap(_ context.Context, c *Cluster, expected uuid.UUID, _ bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active != expected {
		return ErrConnectionCommitConflict
	}
	if _, ok := r.bundles[c.ConnectionGeneration]; !ok {
		return errors.New("pointer selected an unstaged generation")
	}
	r.active = c.ConnectionGeneration
	return nil
}

func (r *atomicBundleRepository) snapshot() (uuid.UUID, []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.active, slices.Clone(r.bundles[r.active])
}

func TestConcurrentUpdateAndDeleteHaveExactlyOneCompleteWinner(t *testing.T) {
	id, a, b, deleted := uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom()
	repo := newAtomicBundleRepository(a)
	updateBundle := fullTestBundle(t, id, b, "updated")
	updateBundle.PreviousGeneration = a
	deleteBundle := deletedConnectionBundle(id, deleted, a, 2)
	services := []*Service{
		{stageConnectionBundle: repo.stage, swapConnectionGeneration: repo.swap},
		{stageConnectionBundle: repo.stage, swapConnectionGeneration: repo.swap},
	}
	type mutation struct {
		cluster *Cluster
		bundle  *secrets.ConnectionBundle
	}
	mutations := []mutation{
		{cluster: &Cluster{ID: id}, bundle: updateBundle},
		{cluster: &Cluster{ID: id, ConnectionDeleted: true}, bundle: deleteBundle},
	}
	start := make(chan struct{})
	results := make(chan error, len(mutations))
	for i := range mutations {
		go func(i int) {
			<-start
			results <- services[i].commitConnectionGeneration(context.Background(), mutations[i].cluster, mutations[i].bundle, a, false)
		}(i)
	}
	close(start)
	var succeeded, conflicted int
	for range mutations {
		err := <-results
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrConnectionCommitConflict):
			conflicted++
		default:
			t.Fatalf("unexpected mutation result: %v", err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("winner count: success=%d conflict=%d", succeeded, conflicted)
	}
	active, raw := repo.snapshot()
	selected := secrets.NewConnectionBundle(id)
	selected.Generation = active
	if err := selected.UnmarshalBinary(raw); err != nil {
		t.Fatalf("active pointer did not select a complete immutable row: %v", err)
	}
	if active == b && (selected.Deleted || selected.AuthToken != "updated.token") {
		t.Fatalf("update winner was mixed: %#v", selected)
	}
	if active == deleted && !selected.Deleted {
		t.Fatalf("delete winner was not an empty tombstone: %#v", selected)
	}
}

func TestCrashAfterDeleteStageBeforePointerCASLeavesOldActive(t *testing.T) {
	id, a, deleted := uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom()
	repo := newAtomicBundleRepository(a)
	crash := errors.New("simulated process crash before CAS")
	svc := &Service{
		stageConnectionBundle: repo.stage,
		swapConnectionGeneration: func(context.Context, *Cluster, uuid.UUID, bool) error {
			return crash
		},
	}
	bundle := deletedConnectionBundle(id, deleted, a, 2)
	err := svc.commitConnectionGeneration(context.Background(), &Cluster{ID: id, ConnectionDeleted: true}, bundle, a, false)
	if !errors.Is(err, crash) {
		t.Fatalf("unexpected simulated crash result: %v", err)
	}
	active, _ := repo.snapshot()
	if active != a {
		t.Fatalf("staged tombstone became active before CAS: %s", active)
	}
	repo.mu.Lock()
	_, staged := repo.bundles[deleted]
	repo.mu.Unlock()
	if !staged {
		t.Fatal("ambiguous staged tombstone was rolled back")
	}
}

func TestDeletedGenerationCASAmbiguityClassification(t *testing.T) {
	a, deleted := uuid.MustRandom(), uuid.MustRandom()
	timeout := context.DeadlineExceeded
	if err := classifyConnectionGenerationCAS(deleted, a, false, timeout, deleted, nil); err != nil {
		t.Fatalf("applied tombstone timeout was not resolved as committed: %v", err)
	}
	if err := classifyConnectionGenerationCAS(deleted, a, false, timeout, a, nil); !errors.Is(err, timeout) {
		t.Fatalf("not-applied tombstone timeout was not resolved as old-active: %v", err)
	}
	if err := classifyConnectionGenerationCAS(deleted, a, false, timeout, uuid.Nil, errors.New("serial unavailable")); !errors.Is(err, ErrConnectionCommitIndeterminate) {
		t.Fatalf("unavailable tombstone resolver was not indeterminate: %v", err)
	}
}
