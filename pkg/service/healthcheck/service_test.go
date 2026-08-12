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
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestCQLPingFailsClosedWhenAuthenticationIsRequired(t *testing.T) {
	clusterID := uuid.MustRandom()
	svc := &Service{
		clusterProvider: func(context.Context, uuid.UUID) (*cluster.Cluster, error) {
			return &cluster.Cluster{AuthToken: "set"}, nil
		},
		logger: log.NewDevelopment(),
	}
	ni := strictTestCQLNodeConfig(t, scyllaclient.NodeInfo{CqlPasswordProtected: true})

	if _, verified, err := svc.pingCQLVerified(context.Background(), clusterID, "127.0.0.1", time.Millisecond, ni); err == nil || !strings.Contains(err.Error(), "active connection generation has none") || verified {
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
	c := &cluster.Cluster{
		ID:                   id,
		ConnectionGeneration: uuid.MustRandom(),
		CQLCAFile:            ca,
		CQLServerName:        "cql.internal",
	}
	nc, err := configcache.NewNodeConfig(c, &nodeInfo, "127.0.0.1", "dc", "rack")
	if err != nil {
		t.Fatal(err)
	}
	return nc
}

func TestCQLPingUsesOnlySnapshotCredentials(t *testing.T) {
	clusterID := uuid.MustRandom()
	svc := &Service{
		clusterProvider: func(context.Context, uuid.UUID) (*cluster.Cluster, error) {
			return &cluster.Cluster{AuthToken: "set"}, nil
		},
		logger: log.NewDevelopment(),
	}
	ni := strictTestCQLNodeConfig(t, scyllaclient.NodeInfo{CqlPasswordProtected: false})

	_, verified, err := svc.pingCQLVerified(context.Background(), clusterID, "127.0.0.1", time.Millisecond, ni)
	if verified {
		t.Fatalf("CQL ping without snapshot credentials reported authentication verified: err=%v", err)
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
	svc := &Service{
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
		clusterProvider: func(context.Context, uuid.UUID) (*cluster.Cluster, error) {
			return &cluster.Cluster{}, nil
		},
		logger: log.NewDevelopment(),
	}
	_, _, _ = svc.pingCQLVerified(context.Background(), uuid.MustRandom(), "127.0.0.1", time.Millisecond, configcache.NodeConfig{
		NodeInfo: &scyllaclient.NodeInfo{NativeTransportPort: "9042"},
	})
}
