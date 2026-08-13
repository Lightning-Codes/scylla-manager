// Copyright (C) 2026 ScyllaDB

package restore

import (
	"context"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/util/query"
)

func TestInitUnitTableMetadataSchemaFileDoesNotRequireTargetCatalog(t *testing.T) {
	w := worker{cqlSchema: &query.DescribedSchema{}}
	units := []Unit{{
		Keyspace: "system_schema",
		Tables: []Table{
			{Table: "removed_between_scylla_releases"},
			{Table: "also_removed_between_scylla_releases"},
		},
	}}

	if err := w.initUnitTableMetadata(context.Background(), units); err != nil {
		t.Fatal(err)
	}
	for _, table := range units[0].Tables {
		if table.TombstoneGC != modeTimeout {
			t.Fatalf("table %q tombstone_gc = %q, expected %q", table.Table, table.TombstoneGC, modeTimeout)
		}
	}
}
