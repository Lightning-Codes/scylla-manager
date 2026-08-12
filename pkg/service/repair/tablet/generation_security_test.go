// Copyright (C) 2026 ScyllaDB

package tablet

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestTabletRepairRejectsTargetDerivedFromAnotherGeneration(t *testing.T) {
	a, b := uuid.MustRandom(), uuid.MustRandom()
	cfg := scyllaclient.TestConfig([]string{"127.0.0.1"}, "token")
	cfg.ConnectionGeneration = b
	client, err := scyllaclient.NewClient(cfg, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	err = validateTargetGeneration(Target{
		KsTabs:               map[string][]string{"old": {"table"}},
		connectionGeneration: a,
	}, client)
	if !errors.Is(err, cluster.ErrConnectionCommitConflict) {
		t.Fatalf("B credentials could be paired with A tablet target: %v", err)
	}
}
