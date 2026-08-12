// Copyright (C) 2026 ScyllaDB

package configcache

import (
	"context"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// ValidateExpectedConnectionGeneration rejects a NodeConfig set assembled
// from any generation other than the one pinned in ctx. Call it immediately
// after ReadAll in workflows that already acquired an Agent client.
func ValidateExpectedConnectionGeneration(ctx context.Context, configs map[string]NodeConfig) error {
	expected, ok := cluster.ExpectedConnectionGeneration(ctx)
	if !ok {
		return nil
	}
	if expected == uuid.Nil {
		return errors.Wrap(cluster.ErrConnectionCommitConflict, "expected connection generation is nil")
	}
	for host, cfg := range configs {
		if cfg.ConnectionGeneration == uuid.Nil || cfg.ConnectionGeneration != expected {
			return errors.Wrapf(cluster.ErrConnectionCommitConflict,
				"node %s uses generation %s, expected %s", host, cfg.ConnectionGeneration, expected)
		}
	}
	return nil
}
