// Copyright (C) 2026 ScyllaDB

package configcache

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestValidateExpectedConnectionGenerationRejectsMixedWorkflowAssembly(t *testing.T) {
	a, b := uuid.MustRandom(), uuid.MustRandom()
	ctx := cluster.WithExpectedConnectionGeneration(context.Background(), a)
	if err := ValidateExpectedConnectionGeneration(ctx, map[string]NodeConfig{
		"10.0.0.1": {ConnectionGeneration: a},
		"10.0.0.2": {ConnectionGeneration: a},
	}); err != nil {
		t.Fatalf("exact generation rejected: %v", err)
	}
	err := ValidateExpectedConnectionGeneration(ctx, map[string]NodeConfig{
		"10.0.0.1": {ConnectionGeneration: a},
		"10.0.0.2": {ConnectionGeneration: b},
	})
	if !errors.Is(err, cluster.ErrConnectionCommitConflict) {
		t.Fatalf("mixed workflow assembly was accepted: %v", err)
	}
}

func TestValidateExpectedConnectionGenerationRejectsNil(t *testing.T) {
	for name, generations := range map[string][2]uuid.UUID{
		"both nil": {uuid.Nil, uuid.Nil},
		"node nil": {uuid.MustRandom(), uuid.Nil},
	} {
		t.Run(name, func(t *testing.T) {
			expected, actual := generations[0], generations[1]
			ctx := cluster.WithExpectedConnectionGeneration(context.Background(), expected)
			err := ValidateExpectedConnectionGeneration(ctx, map[string]NodeConfig{"host": {ConnectionGeneration: actual}})
			if !errors.Is(err, cluster.ErrConnectionCommitConflict) {
				t.Fatalf("nil generation accepted: %v", err)
			}
		})
	}
}
