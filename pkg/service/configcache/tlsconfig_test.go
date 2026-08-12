// Copyright (C) 2026 ScyllaDB

package configcache

import (
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/secrets"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type memoryStore map[string][]byte

func memoryKey(v store.Entry) string {
	id, key := v.Key()
	return id.String() + "/" + key
}

func (s memoryStore) Put(v store.Entry) error {
	b, err := v.MarshalBinary()
	if err == nil {
		s[memoryKey(v)] = append([]byte(nil), b...)
	}
	return err
}

func (s memoryStore) Get(v store.Entry) error {
	b, ok := s[memoryKey(v)]
	if !ok {
		return util.ErrNotFound
	}
	return v.UnmarshalBinary(b)
}

func (s memoryStore) Check(v store.Entry) (bool, error) { _, ok := s[memoryKey(v)]; return ok, nil }
func (s memoryStore) Delete(v store.Entry) error        { delete(s, memoryKey(v)); return nil }
func (s memoryStore) DeleteAll(uuid.UUID) error         { return nil }

func TestStrictProtocolTLSConfig(t *testing.T) {
	ca, err := os.ReadFile("../cluster/testdata/cluster.crt")
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.MustRandom()
	s := memoryStore{}
	cql := secrets.NewCQLTLSTrust(id)
	cql.CA, cql.ServerName = ca, "cql.internal"
	alt := secrets.NewAlternatorTLSTrust(id)
	alt.CA, alt.ServerName = ca, "alternator.internal"
	if err := s.Put(cql); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(alt); err != nil {
		t.Fatal(err)
	}
	c := &cluster.Cluster{ID: id}
	ni := &scyllaclient.NodeInfo{ClientEncryptionEnabled: true, AlternatorHTTPSPort: "8043"}

	cqlConfig, err := newCQLTLSConfigIfEnabled(c, ni, s, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if cqlConfig.InsecureSkipVerify || cqlConfig.ServerName != "cql.internal" || cqlConfig.RootCAs == nil {
		t.Fatal("CQL TLS config is not strict")
	}
	altConfig, err := newAlternatorTLSConfigIfEnabled(c, ni, s, "127.0.0.1")
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
	if _, err := newCQLTLSConfigIfEnabled(c, ni, memoryStore{}, "127.0.0.1"); !errors.Is(err, util.ErrNotFound) {
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
			if _, err := NewNodeConfig(tc.c, tc.ni, panicConfigStore{}, "127.0.0.1", "dc", "rack"); err == nil {
				t.Fatal("unsafe node configuration was published")
			}
		})
	}
}

type panicConfigStore struct{}

func (panicConfigStore) Put(store.Entry) error           { panic("unexpected secret Put") }
func (panicConfigStore) Get(store.Entry) error           { panic("unsafe cache attempted to load a secret") }
func (panicConfigStore) Check(store.Entry) (bool, error) { panic("unexpected secret Check") }
func (panicConfigStore) Delete(store.Entry) error        { panic("unexpected secret Delete") }
func (panicConfigStore) DeleteAll(uuid.UUID) error       { panic("unexpected secret DeleteAll") }
