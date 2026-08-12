// Copyright (C) 2026 ScyllaDB

package configcache

import (
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestStrictProtocolTLSConfig(t *testing.T) {
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
		AlternatorCAFile:     ca,
		AlternatorServerName: "alternator.internal",
	}
	ni := &scyllaclient.NodeInfo{ClientEncryptionEnabled: true, AlternatorHTTPSPort: "8043"}

	cqlConfig, err := newCQLTLSConfigIfEnabled(c, ni, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if cqlConfig.InsecureSkipVerify || cqlConfig.ServerName != "cql.internal" || cqlConfig.RootCAs == nil {
		t.Fatal("CQL TLS config is not strict")
	}
	altConfig, err := newAlternatorTLSConfigIfEnabled(c, ni, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if altConfig.InsecureSkipVerify || altConfig.ServerName != "alternator.internal" || altConfig.RootCAs == nil {
		t.Fatal("Alternator TLS config is not strict")
	}
}

func TestTLSConfigFailsClosedWithoutTrust(t *testing.T) {
	c := &cluster.Cluster{ID: uuid.MustRandom()}
	ni := &scyllaclient.NodeInfo{ClientEncryptionEnabled: true}
	if _, err := newCQLTLSConfigIfEnabled(c, ni, "127.0.0.1"); !errors.Is(err, util.ErrNotFound) {
		t.Fatalf("expected missing trust error, got %v", err)
	}
}

func TestNodeConfigRejectsAuthenticatedPlaintextAndTLSDowngrade(t *testing.T) {
	for name, tc := range map[string]struct {
		c  *cluster.Cluster
		ni *scyllaclient.NodeInfo
	}{
		"CQL password over plaintext": {
			c:  &cluster.Cluster{},
			ni: &scyllaclient.NodeInfo{CqlPasswordProtected: true},
		},
		"CQL forced downgrade": {
			c:  &cluster.Cluster{ForceTLSDisabled: true},
			ni: &scyllaclient.NodeInfo{ClientEncryptionEnabled: true},
		},
		"Alternator authorization over plaintext": {
			c: &cluster.Cluster{},
			ni: &scyllaclient.NodeInfo{
				AlternatorPort:                 "8000",
				AlternatorEnforceAuthorization: true,
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewNodeConfig(tc.c, tc.ni, "127.0.0.1", "dc", "rack"); err == nil {
				t.Fatal("unsafe node configuration was published")
			}
		})
	}
}
