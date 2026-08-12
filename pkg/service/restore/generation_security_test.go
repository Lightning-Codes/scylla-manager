// Copyright (C) 2026 ScyllaDB

package restore

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type generationCheckingRepair struct {
	want        uuid.UUID
	getCalls    int
	repairCalls int
	sentinel    error
}

func (m *generationCheckingRepair) GetTarget(ctx context.Context, _ uuid.UUID, _ json.RawMessage) (repair.Target, error) {
	m.getCalls++
	got, ok := cluster.ExpectedConnectionGeneration(ctx)
	if !ok || got != m.want {
		return repair.Target{}, errors.Errorf("repair handoff generation: got %s present=%v, want %s", got, ok, m.want)
	}
	return repair.Target{}, m.sentinel
}

func (m *generationCheckingRepair) Repair(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, repair.Target) error {
	m.repairCalls++
	return nil
}

func TestRestoreRepairHandoffRetainsWorkerGeneration(t *testing.T) {
	a, b := uuid.MustRandom(), uuid.MustRandom()
	sentinel := errors.New("stop after generation check")
	mock := &generationCheckingRepair{want: a, sentinel: sentinel}
	w := &tablesWorker{
		worker: worker{
			connectionGeneration: a,
			run:                  &Run{ClusterID: uuid.MustRandom()},
		},
		repairSvc: mock,
	}
	// Simulate a caller context that was repinned to B after the restore worker
	// captured A. The nested handoff must restore A before any repair resource
	// can be acquired.
	ctx := cluster.WithExpectedConnectionGeneration(context.Background(), b)
	err := w.stageRepair(ctx)
	if !errors.Is(err, sentinel) {
		t.Fatalf("repair handoff did not remain on A: %v", err)
	}
	if mock.getCalls != 1 || mock.repairCalls != 0 {
		t.Fatalf("unexpected repair operations: target=%d repair=%d", mock.getCalls, mock.repairCalls)
	}
}
