// Copyright (C) 2026 ScyllaDB

package cluster

import (
	"context"

	"github.com/scylladb/scylla-manager/v3/pkg/secrets"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// CommitConnectionGenerationForTest exposes the storage commit boundary to
// external integration tests without widening the production API.
func (s *Service) CommitConnectionGenerationForTest(ctx context.Context, c *Cluster, bundle *secrets.ConnectionBundle, expected uuid.UUID, create bool) error {
	c.LifecycleEpoch = bundle.LifecycleEpoch
	c.ConnectionDeleted = bundle.Deleted
	if !create {
		expectedEpoch := bundle.LifecycleEpoch
		if bundle.Deleted {
			expectedEpoch--
		}
		c.SetExpectedLifecycleEpoch(expectedEpoch)
	}
	return s.commitConnectionGeneration(ctx, c, bundle, expected, create)
}

// ConnectionBundleForTest builds the exact immutable storage tuple used by
// production from a hydrated cluster snapshot.
func ConnectionBundleForTest(c *Cluster, generation uuid.UUID) *secrets.ConnectionBundle {
	return connectionBundleFromCluster(c, generation)
}
