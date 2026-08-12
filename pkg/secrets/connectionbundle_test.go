// Copyright (C) 2026 ScyllaDB

package secrets

import (
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestConnectionBundleRoundTripAndGenerationKey(t *testing.T) {
	id, generation := uuid.MustRandom(), uuid.MustRandom()
	previous := uuid.MustRandom()
	ca, err := os.ReadFile("../service/cluster/testdata/cluster.crt")
	if err != nil {
		t.Fatal(err)
	}
	key, err := os.ReadFile("../service/cluster/testdata/cluster.key")
	if err != nil {
		t.Fatal(err)
	}
	in := &ConnectionBundle{
		ClusterID:                 id,
		Generation:                generation,
		LifecycleEpoch:            1,
		PreviousGeneration:        previous,
		Host:                      "coordinator.internal",
		KnownHosts:                []string{"10.0.0.1", "10.0.0.2"},
		Port:                      10001,
		AuthToken:                 "agent-token",
		ForceNonSSLSessionPort:    true,
		CQLUsername:               "cql-user",
		CQLPassword:               "cql-password",
		CQLClientCertificate:      ca,
		CQLClientPrivateKey:       key,
		CQLCA:                     ca,
		CQLServerName:             "cql.internal",
		AlternatorAccessKeyID:     "access",
		AlternatorSecretAccessKey: "secret",
		AlternatorCA:              ca,
		AlternatorServerName:      "alternator.internal",
		AgentCA:                   ca,
		AgentServerName:           "agent.internal",
	}
	data, err := in.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	out := NewConnectionBundle(id)
	out.Generation = generation
	if err := out.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
	if out.PreviousGeneration != previous ||
		out.AuthToken != in.AuthToken || out.CQLPassword != in.CQLPassword || out.AlternatorSecretAccessKey != in.AlternatorSecretAccessKey ||
		out.CQLServerName != in.CQLServerName || out.AgentServerName != in.AgentServerName ||
		!slices.Equal(out.KnownHosts, in.KnownHosts) || !slices.Equal(out.CQLClientPrivateKey, in.CQLClientPrivateKey) {
		t.Fatal("complete connection tuple did not round trip")
	}
	partition, gotKey := out.Key()
	if want := "connection_bundle/" + generation.String(); gotKey != want {
		t.Fatalf("generation key = %q, want %q", gotKey, want)
	}
	if partition != id {
		t.Fatalf("bundle logical partition = %s, want %s", partition, id)
	}
}

func TestConnectionBundleRejectsGenerationMismatch(t *testing.T) {
	id := uuid.MustRandom()
	in := minimalValidConnectionBundle(t, id, uuid.MustRandom())
	data, err := in.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	out := NewConnectionBundle(id)
	out.Generation = uuid.MustRandom()
	if err := out.UnmarshalBinary(data); err == nil {
		t.Fatal("mismatched immutable generation was accepted")
	}
}

func TestConnectionBundleRejectsLogicalClusterMismatch(t *testing.T) {
	id, other, generation := uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom()
	in := minimalValidConnectionBundle(t, id, generation)
	data, err := in.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	out := NewConnectionBundle(other)
	out.Generation = generation
	if err := out.UnmarshalBinary(data); err == nil || !strings.Contains(err.Error(), "cluster ID mismatch") {
		t.Fatalf("expected embedded logical ID rejection, got %v", err)
	}
}

func minimalValidConnectionBundle(t *testing.T, id, generation uuid.UUID) *ConnectionBundle {
	t.Helper()
	ca, err := os.ReadFile("../service/cluster/testdata/cluster.crt")
	if err != nil {
		t.Fatal(err)
	}
	return &ConnectionBundle{
		ClusterID:       id,
		Generation:      generation,
		LifecycleEpoch:  1,
		Host:            "agent.internal",
		AuthToken:       "agent-token",
		AgentCA:         ca,
		AgentServerName: "agent.internal",
	}
}

func TestDeletedConnectionBundleContainsNoOperationalMaterial(t *testing.T) {
	id, generation := uuid.MustRandom(), uuid.MustRandom()
	tombstone := &ConnectionBundle{ClusterID: id, Generation: generation, LifecycleEpoch: 2, Deleted: true}
	if _, err := tombstone.MarshalBinary(); err != nil {
		t.Fatalf("minimal tombstone rejected: %v", err)
	}
	tombstone.AuthToken = "must-not-survive"
	if _, err := tombstone.MarshalBinary(); err == nil {
		t.Fatal("credential-bearing tombstone was accepted")
	}
}
