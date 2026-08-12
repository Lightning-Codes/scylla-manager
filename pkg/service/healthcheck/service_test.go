// Copyright (C) 2026 ScyllaDB

package healthcheck

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/secrets"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestCQLPingFailsClosedWhenAuthenticationIsRequired(t *testing.T) {
	clusterID := uuid.MustRandom()
	svc := &Service{
		secretsStore: failingSecretsStore{err: util.ErrNotFound},
		clusterProvider: func(context.Context, uuid.UUID) (*cluster.Cluster, error) {
			return &cluster.Cluster{AuthToken: "set"}, nil
		},
		logger: log.NewDevelopment(),
	}
	ni := strictTestCQLNodeConfig(t, scyllaclient.NodeInfo{CqlPasswordProtected: true})

	if _, verified, err := svc.pingCQLVerified(context.Background(), clusterID, "127.0.0.1", time.Millisecond, ni); !errors.Is(err, util.ErrNotFound) || verified {
		t.Fatalf("required CQL authentication fell back to an unauthenticated ping: verified=%v err=%v", verified, err)
	}
}

func strictTestCQLNodeConfig(t *testing.T, nodeInfo scyllaclient.NodeInfo) configcache.NodeConfig {
	t.Helper()
	nodeInfo.ClientEncryptionEnabled = true
	ca, err := os.ReadFile("../cluster/testdata/cluster.crt")
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.MustRandom()
	secretsStore := testTLSStore{}
	trust := secrets.NewCQLTLSTrust(id)
	trust.CA, trust.ServerName = ca, "cql.internal"
	if err := secretsStore.Put(trust); err != nil {
		t.Fatal(err)
	}
	nc, err := configcache.NewNodeConfig(&cluster.Cluster{ID: id}, &nodeInfo, secretsStore, "127.0.0.1", "dc", "rack")
	if err != nil {
		t.Fatal(err)
	}
	return nc
}

type testTLSStore map[string][]byte

func (s testTLSStore) Put(v store.Entry) error {
	id, key := v.Key()
	b, err := v.MarshalBinary()
	if err == nil {
		s[id.String()+key] = b
	}
	return err
}
func (s testTLSStore) Get(v store.Entry) error {
	id, key := v.Key()
	return v.UnmarshalBinary(s[id.String()+key])
}
func (testTLSStore) Check(store.Entry) (bool, error) { return false, nil }
func (testTLSStore) Delete(store.Entry) error        { return nil }
func (testTLSStore) DeleteAll(uuid.UUID) error       { return nil }

func TestCQLPingFailsClosedOnSecretsStoreError(t *testing.T) {
	clusterID := uuid.MustRandom()
	storeErr := errors.New("secrets store unavailable")
	svc := &Service{
		secretsStore: failingSecretsStore{err: storeErr},
		clusterProvider: func(context.Context, uuid.UUID) (*cluster.Cluster, error) {
			return &cluster.Cluster{AuthToken: "set"}, nil
		},
		logger: log.NewDevelopment(),
	}
	ni := strictTestCQLNodeConfig(t, scyllaclient.NodeInfo{CqlPasswordProtected: false})

	_, verified, err := svc.pingCQLVerified(context.Background(), clusterID, "127.0.0.1", time.Millisecond, ni)
	if !errors.Is(err, storeErr) || !strings.Contains(err.Error(), "load CQL credentials") || verified {
		t.Fatalf("CQL ping did not fail closed on store error: verified=%v err=%v", verified, err)
	}
}

func TestCQLAuthenticationVerifiedForClientCertificate(t *testing.T) {
	ni := configcache.NodeConfig{NodeInfo: &scyllaclient.NodeInfo{ClientEncryptionRequireAuth: true}}
	if !cqlAuthenticationVerified(ni, true, nil) {
		t.Fatal("successful mutually authenticated CQL TLS was not reported as authentication verified")
	}
	if cqlAuthenticationVerified(ni, false, nil) {
		t.Fatal("client-certificate authentication without verified TLS was reported as verified")
	}
	if cqlAuthenticationVerified(ni, true, errors.New("TLS handshake failed")) {
		t.Fatal("failed mutually authenticated CQL TLS was reported as authentication verified")
	}
}

func TestHealthProbesNeverLoadCredentialsForPlaintextEndpoints(t *testing.T) {
	clusterID := uuid.MustRandom()
	panicStore := panicGetSecretsStore{}
	svc := &Service{
		secretsStore: panicStore,
		clusterProvider: func(context.Context, uuid.UUID) (*cluster.Cluster, error) {
			return &cluster.Cluster{}, nil
		},
		logger: log.NewDevelopment(),
	}

	if _, verified, err := svc.pingCQLVerified(context.Background(), clusterID, "127.0.0.1", time.Millisecond, configcache.NodeConfig{
		NodeInfo: &scyllaclient.NodeInfo{CqlPasswordProtected: true},
	}); err == nil || verified || !strings.Contains(err.Error(), "refusing to transmit") {
		t.Fatalf("plaintext CQL authentication did not fail before loading credentials: verified=%v err=%v", verified, err)
	}
	if _, err := svc.pingAlternator(context.Background(), clusterID, "127.0.0.1", time.Millisecond, configcache.NodeConfig{
		NodeInfo: &scyllaclient.NodeInfo{AlternatorPort: "8000", AlternatorEnforceAuthorization: true},
	}); err == nil || !strings.Contains(err.Error(), "refusing to transmit") {
		t.Fatalf("plaintext Alternator authentication did not fail before loading credentials: %v", err)
	}
}

func TestPlaintextCQLProbeNeverLoadsOptionalStoredCredentials(t *testing.T) {
	svc := &Service{
		secretsStore: panicGetSecretsStore{},
		clusterProvider: func(context.Context, uuid.UUID) (*cluster.Cluster, error) {
			return &cluster.Cluster{}, nil
		},
		logger: log.NewDevelopment(),
	}
	_, _, _ = svc.pingCQLVerified(context.Background(), uuid.MustRandom(), "127.0.0.1", time.Millisecond, configcache.NodeConfig{
		NodeInfo: &scyllaclient.NodeInfo{NativeTransportPort: "9042"},
	})
}

type failingSecretsStore struct {
	err error
}

type panicGetSecretsStore struct{ failingSecretsStore }

func (panicGetSecretsStore) Get(store.Entry) error {
	panic("plaintext health probe attempted to load credentials")
}

func (failingSecretsStore) Put(store.Entry) error           { return nil }
func (s failingSecretsStore) Get(store.Entry) error         { return s.err }
func (failingSecretsStore) Check(store.Entry) (bool, error) { return false, nil }
func (failingSecretsStore) Delete(store.Entry) error        { return nil }
func (failingSecretsStore) DeleteAll(uuid.UUID) error       { return nil }
