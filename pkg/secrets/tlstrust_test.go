// Copyright (C) 2026 ScyllaDB

package secrets

import (
	"os"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestTLSTrustRoundTrip(t *testing.T) {
	ca, err := os.ReadFile("../service/cluster/testdata/cluster.crt")
	if err != nil {
		t.Fatal(err)
	}

	in := NewCQLTLSTrust(uuid.MustRandom())
	in.CA = ca
	in.ServerName = "scylla.internal"
	data, err := in.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	out := NewCQLTLSTrust(in.ClusterID)
	if err := out.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
	if string(out.CA) != string(in.CA) || out.ServerName != in.ServerName {
		t.Fatalf("round trip mismatch")
	}
	if _, key := out.Key(); key != "cql_tls_trust" {
		t.Fatalf("unexpected key %q", key)
	}
}

func TestTLSTrustRejectsIncompleteOrInvalidInput(t *testing.T) {
	for _, trust := range []*TLSTrust{
		{Protocol: CQLProtocol, CA: []byte("not PEM"), ServerName: "scylla.internal"},
		{Protocol: CQLProtocol, CA: []byte("not PEM")},
		{Protocol: CQLProtocol, ServerName: "scylla.internal"},
	} {
		if _, err := trust.MarshalBinary(); err == nil {
			t.Fatal("expected validation error")
		}
	}
}
