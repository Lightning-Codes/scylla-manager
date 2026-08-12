// Copyright (C) 2026 ScyllaDB

package repair

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestRepairRejectsTargetDerivedFromAnotherGeneration(t *testing.T) {
	a, b := uuid.MustRandom(), uuid.MustRandom()
	svc := &Service{
		logger: log.NewDevelopment(),
		scyllaClient: func(context.Context, uuid.UUID) (*scyllaclient.Client, error) {
			cfg := scyllaclient.TestConfig([]string{"127.0.0.1"}, "token")
			cfg.ConnectionGeneration = b
			return scyllaclient.NewClient(cfg, log.NewDevelopment())
		},
	}
	err := svc.Repair(context.Background(), uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom(), Target{
		connectionGeneration: a,
	})
	if !errors.Is(err, cluster.ErrConnectionCommitConflict) {
		t.Fatalf("B credentials could be paired with A repair target: %v", err)
	}
}
