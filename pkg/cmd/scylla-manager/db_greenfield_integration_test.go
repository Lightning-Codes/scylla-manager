// Copyright (C) 2026 ScyllaDB

//go:build all || integration

package main

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	serverconfig "github.com/scylladb/scylla-manager/v3/pkg/config/server"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
)

const greenfieldTestKeyspace = "test_scylla_manager"

func TestPrepareGreenfieldStoreSessionTrulyEmptyBootstrap(t *testing.T) {
	session := db.CreateSessionWithoutMigration(t)
	defer session.Close()

	marker, err := prepareGreenfieldStoreSession(t.Context(), session.Session, greenfieldTestKeyspace)
	if err != nil {
		t.Fatal(err)
	}
	assertGreenfieldMarker(t, marker, greenfieldBootstrapMigrating)
	assertStoredGreenfieldMarker(t, session, greenfieldBootstrapMigrating)

	tables, err := userTables(t.Context(), session.Session, greenfieldTestKeyspace)
	if err != nil {
		t.Fatal(err)
	}
	if !greenfieldMarkerTablesOnly(tables) {
		t.Fatalf("expected only bootstrap storage after initial prepare, got %v", tables)
	}
}

func TestPrepareGreenfieldStoreSessionRejectsReusedKeyspace(t *testing.T) {
	session := db.CreateSessionWithoutMigration(t)
	defer session.Close()

	db.ExecStmt(t, session, `CREATE TABLE scheduler_task (
		cluster_id uuid,
		type text,
		id uuid,
		PRIMARY KEY (cluster_id, type, id)
	)`)
	db.ExecStmt(t, session, "INSERT INTO scheduler_task (cluster_id, type, id) VALUES (uuid(), 'repair', uuid())")
	db.ExecStmt(t, session, `CREATE TABLE gocqlx_migrate (
		name text PRIMARY KEY,
		checksum text,
		done int
	)`)
	db.ExecStmt(t, session, "INSERT INTO gocqlx_migrate (name, checksum, done) VALUES ('001-init.cql', 'retained', 1)")

	_, err := prepareGreenfieldStoreSession(t.Context(), session.Session, greenfieldTestKeyspace)
	if err == nil {
		t.Fatal("expected a reused Manager metadata keyspace to be rejected")
	}
	if !strings.Contains(err.Error(), "contains unowned Manager schema") || !strings.Contains(err.Error(), "truly empty keyspace") {
		t.Fatalf("expected greenfield rejection and remediation, got %q", err)
	}

	tables, inventoryErr := userTables(t.Context(), session.Session, greenfieldTestKeyspace)
	if inventoryErr != nil {
		t.Fatal(inventoryErr)
	}
	if _, ok := tables[greenfieldBootstrapTable]; ok {
		t.Fatal("bootstrap marker table was created before the reused keyspace was rejected")
	}
	assertTableRowCount(t, session, "scheduler_task", 1)
	assertTableRowCount(t, session, "gocqlx_migrate", 1)
}

func TestPrepareGreenfieldStoreSessionRejectsSchemaObjectOnlyKeyspace(t *testing.T) {
	testCases := []struct {
		name       string
		createStmt string
		wantFamily string
	}{
		{
			name:       "user-defined type",
			createStmt: "CREATE TYPE retained_schedule (start_date timestamp)",
			wantFamily: "user-defined type",
		},
		{
			name:       "function",
			createStmt: "CREATE FUNCTION retained_function(v int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE lua AS 'return v'",
			wantFamily: "function",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			session := db.CreateSessionWithoutMigration(t)
			defer session.Close()
			db.ExecStmt(t, session, tc.createStmt)

			_, err := prepareGreenfieldStoreSession(t.Context(), session.Session, greenfieldTestKeyspace)
			if err == nil {
				t.Fatalf("expected %s-only keyspace to be rejected", tc.wantFamily)
			}
			if !strings.Contains(err.Error(), "contains unowned Manager schema") || !strings.Contains(err.Error(), tc.wantFamily) {
				t.Fatalf("expected %s rejection, got %q", tc.wantFamily, err)
			}
			tables, inventoryErr := userTables(t.Context(), session.Session, greenfieldTestKeyspace)
			if inventoryErr != nil {
				t.Fatal(inventoryErr)
			}
			if _, ok := tables[greenfieldBootstrapTable]; ok {
				t.Fatal("bootstrap marker table was created before schema-only keyspace rejection")
			}
		})
	}
}

func TestPrepareGreenfieldStoreSessionMarkerTableOnlyCrashState(t *testing.T) {
	session := db.CreateSessionWithoutMigration(t)
	defer session.Close()
	createGreenfieldBootstrapTable(t, session)

	marker, err := prepareGreenfieldStoreSession(t.Context(), session.Session, greenfieldTestKeyspace)
	if err != nil {
		t.Fatal(err)
	}
	assertGreenfieldMarker(t, marker, greenfieldBootstrapMigrating)
	assertStoredGreenfieldMarker(t, session, greenfieldBootstrapMigrating)
}

func TestPrepareGreenfieldStoreSessionCrashRestartFromMigratingMarker(t *testing.T) {
	session := db.CreateSessionWithoutMigration(t)
	defer session.Close()

	marker, err := prepareGreenfieldStoreSession(t.Context(), session.Session, greenfieldTestKeyspace)
	if err != nil {
		t.Fatal(err)
	}
	assertGreenfieldMarker(t, marker, greenfieldBootstrapMigrating)

	// Simulate a crash after the migration framework and an ordinary schema
	// migration have already mutated the keyspace.
	db.ExecStmt(t, session, `CREATE TABLE gocqlx_migrate (
		name text PRIMARY KEY,
		checksum text,
		done int
	)`)
	db.ExecStmt(t, session, "INSERT INTO gocqlx_migrate (name, checksum, done) VALUES ('001-init.cql', 'partial', 0)")
	db.ExecStmt(t, session, "CREATE TABLE scheduler_task (id uuid PRIMARY KEY)")

	marker, err = prepareGreenfieldStoreSession(t.Context(), session.Session, greenfieldTestKeyspace)
	if err != nil {
		t.Fatal(err)
	}
	assertGreenfieldMarker(t, marker, greenfieldBootstrapMigrating)
	assertStoredGreenfieldMarker(t, session, greenfieldBootstrapMigrating)
}

func TestPrepareGreenfieldStoreSessionFinalizedSecureRestart(t *testing.T) {
	session := db.CreateSessionWithoutMigration(t)
	defer session.Close()

	if _, err := prepareGreenfieldStoreSession(t.Context(), session.Session, greenfieldTestKeyspace); err != nil {
		t.Fatal(err)
	}
	db.ExecStmt(t, session, `CREATE TABLE secure_cluster (
		id uuid PRIMARY KEY,
		connection_generation uuid,
		connection_deleted boolean,
		lifecycle_epoch bigint
	)`)
	db.ExecStmt(t, session, `CREATE TABLE secure_connection_bundle (
		cluster_id uuid,
		key text,
		value blob,
		PRIMARY KEY (cluster_id, key)
	)`)
	if err := finalizeGreenfieldStoreSession(t.Context(), session.Session); err != nil {
		t.Fatal(err)
	}
	assertStoredGreenfieldMarker(t, session, greenfieldBootstrapReady)

	marker, err := prepareGreenfieldStoreSession(t.Context(), session.Session, greenfieldTestKeyspace)
	if err != nil {
		t.Fatal(err)
	}
	assertGreenfieldMarker(t, marker, greenfieldBootstrapReady)
	if err := finalizeGreenfieldStoreSession(t.Context(), session.Session); err != nil {
		t.Fatal("idempotent ready finalization:", err)
	}
}

func TestFinalizeGreenfieldStoreSessionRejectsInvalidPhase(t *testing.T) {
	session := db.CreateSessionWithoutMigration(t)
	defer session.Close()
	createGreenfieldBootstrapTable(t, session)
	db.ExecStmt(t, session, "INSERT INTO secure_manager_bootstrap (bootstrap_id, contract, bootstrap_phase) VALUES ('"+
		greenfieldBootstrapID+"', '"+greenfieldBootstrapContract+"', 'invalid')")

	if err := finalizeGreenfieldStoreSession(t.Context(), session.Session); err == nil {
		t.Fatal("invalid bootstrap phase was finalized")
	}
	marker, err := readGreenfieldBootstrapMarker(t.Context(), session.Session)
	if err != nil {
		t.Fatal(err)
	}
	if marker.Phase != "invalid" {
		t.Fatalf("invalid phase changed to %q", marker.Phase)
	}
}

func TestPrepareGreenfieldStoreSessionConcurrentPrepare(t *testing.T) {
	session := db.CreateSessionWithoutMigration(t)
	defer session.Close()

	const writers = 8
	start := make(chan struct{})
	results := make(chan greenfieldBootstrapMarker, writers)
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			marker, err := prepareGreenfieldStoreSession(t.Context(), session.Session, greenfieldTestKeyspace)
			if err != nil {
				errs <- err
				return
			}
			results <- marker
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Errorf("concurrent prepare failed: %v", err)
	}
	for marker := range results {
		assertGreenfieldMarker(t, marker, greenfieldBootstrapMigrating)
	}
	if t.Failed() {
		return
	}
	assertStoredGreenfieldMarker(t, session, greenfieldBootstrapMigrating)
	assertTableRowCount(t, session, greenfieldBootstrapTable, 1)
}

func TestInitializeManagerDatabaseRecoversDDLBeforeProgressCrash(t *testing.T) {
	session := db.CreateSessionWithoutMigration(t)
	marker, err := prepareGreenfieldStoreSession(t.Context(), session.Session, greenfieldTestKeyspace)
	if err != nil {
		t.Fatal(err)
	}
	if !marker.Claimed {
		t.Fatal("initial marker was not claimed")
	}

	// Simulate the exact gocqlx migration crash window: a non-idempotent DDL
	// has committed but the matching progress update has not.
	db.ExecStmt(t, session, "CREATE TABLE partial_repair_progress (id uuid PRIMARY KEY)")
	db.ExecStmt(t, session, "ALTER TABLE partial_repair_progress ADD first_token blob")
	db.ExecStmt(t, session, `CREATE TABLE gocqlx_migrate (
		name text PRIMARY KEY,
		checksum text,
		done int,
		start_time timestamp,
		end_time timestamp
	)`)
	db.ExecStmt(t, session, "INSERT INTO gocqlx_migrate (name, checksum, done) VALUES ('004-repair_run_progress_segment_error_start_tokens.cql', 'stale', 0)")
	session.Close()

	c := greenfieldIntegrationConfig()
	if err := initializeManagerDatabase(t.Context(), c, log.NewDevelopment()); err != nil {
		t.Fatal(err)
	}
	assertInitializedGreenfieldStore(t, c)

	session = openGreenfieldIntegrationSession(t, c)
	defer session.Close()
	var partial string
	err = session.Query(
		"SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
		nil,
	).Bind(greenfieldTestKeyspace, "partial_repair_progress").Scan(&partial)
	if err == nil {
		t.Fatal("partial pre-reset DDL survived clean replay")
	}
}

func TestInitializeManagerDatabaseRecoversResetCrashStates(t *testing.T) {
	t.Run("resetting before drop", func(t *testing.T) {
		session := db.CreateSessionWithoutMigration(t)
		if _, err := prepareGreenfieldStoreSession(t.Context(), session.Session, greenfieldTestKeyspace); err != nil {
			t.Fatal(err)
		}
		db.ExecStmt(t, session, "UPDATE secure_manager_bootstrap SET bootstrap_phase = 'resetting' WHERE bootstrap_id = '"+greenfieldBootstrapID+"'")
		db.ExecStmt(t, session, "CREATE TABLE partial_before_drop (id uuid PRIMARY KEY)")
		session.Close()

		c := greenfieldIntegrationConfig()
		if err := initializeManagerDatabase(t.Context(), c, log.NewDevelopment()); err != nil {
			t.Fatal(err)
		}
		assertInitializedGreenfieldStore(t, c)
	})

	t.Run("after drop before recreate", func(t *testing.T) {
		session := db.CreateSessionWithoutMigration(t)
		if _, err := prepareGreenfieldStoreSession(t.Context(), session.Session, greenfieldTestKeyspace); err != nil {
			t.Fatal(err)
		}
		session.Close()

		c := greenfieldIntegrationConfig()
		dropGreenfieldIntegrationKeyspace(t, c)
		if err := initializeManagerDatabase(t.Context(), c, log.NewDevelopment()); err != nil {
			t.Fatal(err)
		}
		assertInitializedGreenfieldStore(t, c)
	})

	t.Run("after recreate before marker", func(t *testing.T) {
		session := db.CreateSessionWithoutMigration(t)
		session.Close()

		c := greenfieldIntegrationConfig()
		if err := initializeManagerDatabase(t.Context(), c, log.NewDevelopment()); err != nil {
			t.Fatal(err)
		}
		assertInitializedGreenfieldStore(t, c)
	})
}

func TestInitializeManagerDatabaseNeverResetsReadyStore(t *testing.T) {
	session := db.CreateSessionWithoutMigration(t)
	session.Close()
	c := greenfieldIntegrationConfig()
	if err := initializeManagerDatabase(t.Context(), c, log.NewDevelopment()); err != nil {
		t.Fatal(err)
	}

	session = openGreenfieldIntegrationSession(t, c)
	db.ExecStmt(t, session, "CREATE TABLE ready_store_sentinel (id int PRIMARY KEY, value text)")
	db.ExecStmt(t, session, "INSERT INTO ready_store_sentinel (id, value) VALUES (1, 'preserved')")
	session.Close()

	if err := initializeManagerDatabase(t.Context(), c, log.NewDevelopment()); err != nil {
		t.Fatal(err)
	}
	session = openGreenfieldIntegrationSession(t, c)
	defer session.Close()
	var value string
	if err := session.Query("SELECT value FROM ready_store_sentinel WHERE id = 1", nil).Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "preserved" {
		t.Fatalf("ready store sentinel = %q", value)
	}
}

func greenfieldIntegrationConfig() serverconfig.Config {
	c := serverconfig.DefaultConfig()
	c.Database.Hosts = strings.Split(testconfig.ScyllaManagerDBCluster(), ",")
	c.Database.InitAddr = c.Database.Hosts[0]
	c.Database.Port = testconfig.CQLPort()
	c.Database.Keyspace = greenfieldTestKeyspace
	c.Database.ReplicationFactor = 1
	c.Database.Timeout = testconfig.CQLTimeout()
	c.Database.MigrateTimeout = 30 * time.Second
	return c
}

func openGreenfieldIntegrationSession(t *testing.T, c serverconfig.Config) gocqlx.Session {
	t.Helper()
	cluster := gocqlClusterConfigForDBInit(t.Context(), c, log.NewDevelopment())
	cluster.Keyspace = c.Database.Keyspace
	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func dropGreenfieldIntegrationKeyspace(t *testing.T, c serverconfig.Config) {
	t.Helper()
	cluster := gocqlClusterConfigForDBInit(t.Context(), c, log.NewDevelopment())
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.Query("DROP KEYSPACE IF EXISTS " + greenfieldTestKeyspace).Exec(); err != nil {
		t.Fatal(err)
	}
}

func assertInitializedGreenfieldStore(t *testing.T, c serverconfig.Config) {
	t.Helper()
	session := openGreenfieldIntegrationSession(t, c)
	defer session.Close()
	assertStoredGreenfieldMarker(t, session, greenfieldBootstrapReady)
	for _, table := range []string{"scheduler_task", "secure_cluster", "secure_connection_bundle"} {
		var got string
		if err := session.Query(
			"SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?",
			nil,
		).Bind(greenfieldTestKeyspace, table).Scan(&got); err != nil {
			t.Fatalf("missing fully migrated table %s: %v", table, err)
		}
	}
}

func createGreenfieldBootstrapTable(t *testing.T, session gocqlx.Session) {
	t.Helper()
	db.ExecStmt(t, session, `CREATE TABLE secure_manager_bootstrap (
		bootstrap_id text PRIMARY KEY,
		contract text,
		bootstrap_phase text
	)`)
}

func assertGreenfieldMarker(t *testing.T, marker greenfieldBootstrapMarker, wantPhase string) {
	t.Helper()
	if marker.Contract != greenfieldBootstrapContract || marker.Phase != wantPhase {
		t.Fatalf("unexpected bootstrap marker: %+v", marker)
	}
}

func assertStoredGreenfieldMarker(t *testing.T, session gocqlx.Session, wantPhase string) {
	t.Helper()
	marker, err := readGreenfieldBootstrapMarker(t.Context(), session.Session)
	if err != nil {
		t.Fatal(err)
	}
	assertGreenfieldMarker(t, marker, wantPhase)
}

func assertTableRowCount(t *testing.T, session gocqlx.Session, table string, want int) {
	t.Helper()
	var count int
	if err := session.Query("SELECT COUNT(*) FROM "+table, nil).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("expected %d row(s) in %s, got %d", want, table, count)
	}
}
