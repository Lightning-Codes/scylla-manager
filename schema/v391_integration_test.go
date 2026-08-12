// Copyright (C) 2026 ScyllaDB

//go:build all || integration

package schema_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	"github.com/scylladb/scylla-manager/v3/schema"
)

func TestV391DDLCanResumeAfterPartialApplicationAndRetry(t *testing.T) {
	session := db.CreateSessionWithoutMigration(t)
	defer session.Close()
	statements := v391Statements(t)

	// Simulate an interruption after the first schema mutation but before the
	// migration runner can persist progress.
	if err := executeV391Statements(t.Context(), session, statements[:1]); err != nil {
		t.Fatal(err)
	}

	// A full restart and another ordinary retry must both be harmless.
	if err := executeV391Statements(t.Context(), session, statements); err != nil {
		t.Fatal("resume v3.9.1 DDL:", err)
	}
	if err := executeV391Statements(t.Context(), session, statements); err != nil {
		t.Fatal("retry v3.9.1 DDL:", err)
	}

	assertV391Tables(t, session)
}

func TestV391DDLIsSafeUnderConcurrentReplay(t *testing.T) {
	session := db.CreateSessionWithoutMigration(t)
	defer session.Close()
	statements := v391Statements(t)

	const workers = 4
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			if err := executeV391Statements(t.Context(), session, statements); err != nil {
				errs <- fmt.Errorf("worker %d: %w", worker, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	if err := session.AwaitSchemaAgreement(t.Context()); err != nil {
		t.Fatal("await schema agreement:", err)
	}
	assertV391Tables(t, session)
}

func v391Statements(t *testing.T) []string {
	t.Helper()

	b, err := schema.Files.ReadFile("v3.9.1.cql")
	if err != nil {
		t.Fatal(err)
	}
	var statements []string
	for _, chunk := range strings.Split(string(b), ";") {
		if stmt := strings.TrimSpace(chunk); stmt != "" {
			statements = append(statements, stmt)
		}
	}
	if len(statements) != 2 {
		t.Fatalf("expected two v3.9.1 statements, got %d", len(statements))
	}
	return statements
}

func executeV391Statements(ctx context.Context, session gocqlx.Session, statements []string) error {
	for i, stmt := range statements {
		if err := session.ContextQuery(ctx, stmt, nil).ExecRelease(); err != nil {
			return fmt.Errorf("statement %d: %w", i+1, err)
		}
	}
	return nil
}

func assertV391Tables(t *testing.T, session gocqlx.Session) {
	t.Helper()

	for _, table := range []string{"secure_cluster", "secure_connection_bundle"} {
		var got string
		err := session.Query(
			"SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
			nil,
		).Bind("test_scylla_manager", table).Scan(&got)
		if err != nil {
			t.Errorf("inspect %s: %v", table, err)
			continue
		}
		if got != table {
			t.Errorf("expected table %q, got %q", table, got)
		}
	}
}
