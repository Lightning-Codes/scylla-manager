// Copyright (C) 2026 ScyllaDB

//go:build all || integration

package restore

import (
	"context"
	"fmt"
	"testing"

	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestEnsureAlternatorBaseColumnsUsesSupportedIdempotentCQLIntegration(t *testing.T) {
	session := CreateScyllaManagerDBSession(t)
	keyspace := "restore_gsi_columns_" + uuid.NewTime().String()[:8]
	table := "events"

	if err := session.ExecStmt(fmt.Sprintf(
		"CREATE KEYSPACE %q WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}",
		keyspace,
	)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.ExecStmt(fmt.Sprintf("DROP KEYSPACE IF EXISTS %q", keyspace)); err != nil {
			t.Error(err)
		}
	})
	if err := session.ExecStmt(fmt.Sprintf("CREATE TABLE %q.%q (pk text PRIMARY KEY)", keyspace, table)); err != nil {
		t.Fatal(err)
	}

	columns := []alternatorBaseColumn{
		{keyspace: keyspace, table: table, name: "GSI1PK", cqlType: "text"},
		{keyspace: keyspace, table: table, name: "GSI2PK", cqlType: "decimal"},
	}
	changed, err := ensureAlternatorBaseColumns(context.Background(), session, columns)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("first reconciliation did not add columns")
	}

	changed, err = ensureAlternatorBaseColumns(context.Background(), session, columns)
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatal("second reconciliation changed an already-correct schema")
	}

	mismatched := []alternatorBaseColumn{{keyspace: keyspace, table: table, name: "GSI1PK", cqlType: "blob"}}
	if _, err := ensureAlternatorBaseColumns(context.Background(), session, mismatched); err == nil {
		t.Fatal("expected an existing-column type mismatch")
	}
}
