// Copyright (C) 2026 ScyllaDB

package backup

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func generationProvider(t *testing.T, generation uuid.UUID) scyllaclient.ProviderFunc {
	t.Helper()
	return func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
		cfg := scyllaclient.TestConfig([]string{"127.0.0.1"}, "token")
		cfg.ConnectionGeneration = generation
		return scyllaclient.NewClient(cfg, log.NewDevelopment())
	}
}

func TestBackupRejectsTargetDerivedFromAnotherGeneration(t *testing.T) {
	a, b := uuid.MustRandom(), uuid.MustRandom()
	svc := &Service{scyllaClient: generationProvider(t, b), logger: log.NewDevelopment()}
	target := Target{
		connectionGeneration: a,
		liveNodes: scyllaclient.NodeStatusInfoSlice{
			{Addr: "192.0.2.10", HostID: "old-generation-host"},
		},
	}
	err := svc.Backup(context.Background(), uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom(), target)
	if !errors.Is(err, cluster.ErrConnectionCommitConflict) {
		t.Fatalf("B credentials could be paired with A target: %v", err)
	}
}

func TestBackupSizeRejectsHostsDerivedFromAnotherGeneration(t *testing.T) {
	a, b := uuid.MustRandom(), uuid.MustRandom()
	svc := &Service{scyllaClient: generationProvider(t, b), logger: log.NewDevelopment()}
	target := Target{
		connectionGeneration: a,
		liveNodes: scyllaclient.NodeStatusInfoSlice{
			{Addr: "192.0.2.10", HostID: "old-generation-host"},
		},
	}
	_, err := svc.GetTargetSize(context.Background(), uuid.MustRandom(), target)
	if !errors.Is(err, cluster.ErrConnectionCommitConflict) {
		t.Fatalf("B credentials could be paired with A hosts while sizing: %v", err)
	}
}

func TestValidationRejectsTargetDerivedFromAnotherGeneration(t *testing.T) {
	a, b := uuid.MustRandom(), uuid.MustRandom()
	svc := &Service{scyllaClient: generationProvider(t, b), logger: log.NewDevelopment()}
	target := ValidationTarget{
		connectionGeneration: a,
		liveNodes: scyllaclient.NodeStatusInfoSlice{
			{Addr: "192.0.2.10", HostID: "old-generation-host"},
		},
	}
	err := svc.Validate(context.Background(), uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom(), target)
	if !errors.Is(err, cluster.ErrConnectionCommitConflict) {
		t.Fatalf("B credentials could be paired with A validation target: %v", err)
	}
}

func TestBackupRejectsNilTargetAndClientGenerations(t *testing.T) {
	svc := &Service{scyllaClient: generationProvider(t, uuid.Nil), logger: log.NewDevelopment()}
	err := svc.Backup(context.Background(), uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom(), Target{})
	if !errors.Is(err, cluster.ErrConnectionCommitConflict) {
		t.Fatalf("nil target and client generations were accepted: %v", err)
	}
}
