// Copyright (C) 2026 ScyllaDB

package backup

import (
	"archive/tar"
	"bytes"
	"context"
	"strings"
	"testing"
)

func legacySchemaTar(t *testing.T, files map[string]string) *bytes.Buffer {
	t.Helper()
	var b bytes.Buffer
	tw := tar.NewWriter(&b)
	for name, body := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return &b
}

func TestParseLegacySchemaArchive(t *testing.T) {
	const appCQL = "CREATE KEYSPACE alternator_app; CREATE TABLE alternator_app.app (pk text PRIMARY KEY);"
	schema, err := parseLegacySchemaArchive(context.Background(), legacySchemaTar(t, map[string]string{
		"system_auth.cql":    "CREATE KEYSPACE system_auth;",
		"alternator_app.cql": appCQL,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(schema) != 1 {
		t.Fatalf("got %d schema rows, expected exactly one user keyspace", len(schema))
	}
	if got := schema[0]; got.Keyspace != "alternator_app" || got.Name != "alternator_app" || got.Type != "legacy_keyspace" || got.CQLStmt != `CREATE KEYSPACE "alternator_app"; CREATE TABLE "alternator_app"."app" (pk text PRIMARY KEY);` {
		t.Fatalf("unexpected legacy schema row: %#v", got)
	}
}

func TestParseLegacyAlternatorSchemaQuotesUnsafeDescribeIdentifiers(t *testing.T) {
	const appCQL = `CREATE KEYSPACE alternator_sophena-main-prod WITH replication = {'class': 'NetworkTopologyStrategy', 'ashburn': '3'};
CREATE TABLE alternator_sophena-main-prod.sophena-main-prod (
    PK text,
    SK text,
    :attrs map<text, blob>,
    GSI1PK text,
    GSI1SK text,
    PRIMARY KEY (PK, SK)
) WITH bloom_filter_fp_chance = 0.01
    AND dclocal_read_repair_chance = 0
    AND read_repair_chance = 0
    AND gc_grace_seconds = 864000;
CREATE TABLE alternator_sophena-main-prod.sophena-main-prod_scylla_cdc_log (
    cdc$stream_id blob,
    cdc$time timeuuid,
    PRIMARY KEY (cdc$stream_id, cdc$time)
);
CREATE MATERIALIZED VIEW alternator_sophena-main-prod.sophena-main-prod:GSI1 AS
    SELECT * FROM alternator_sophena-main-prod.sophena-main-prod
    WHERE "GSI1PK" IS NOT NULL AND "GSI1SK" IS NOT NULL
    PRIMARY KEY (GSI1PK, GSI1SK, PK, SK);`

	schema, err := parseLegacySchemaArchive(context.Background(), legacySchemaTar(t, map[string]string{
		"alternator_sophena-main-prod.cql": appCQL,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(schema) != 1 {
		t.Fatalf("got %d schema rows, expected one", len(schema))
	}
	got := schema[0].CQLStmt
	for _, expected := range []string{
		`CREATE KEYSPACE "alternator_sophena-main-prod"`,
		`CREATE TABLE "alternator_sophena-main-prod"."sophena-main-prod"`,
		`"PK" text`, `"SK" text`, `":attrs" map<text, blob>`,
		`CREATE TABLE "alternator_sophena-main-prod"."sophena-main-prod_scylla_cdc_log"`,
		`"cdc$stream_id" blob`,
		`CREATE MATERIALIZED VIEW "alternator_sophena-main-prod"."sophena-main-prod:GSI1"`,
		`FROM "alternator_sophena-main-prod"."sophena-main-prod"`,
		`PRIMARY KEY ("GSI1PK", "GSI1SK", "PK", "SK")`,
	} {
		if !strings.Contains(got, expected) {
			t.Errorf("normalized schema is missing %q:\n%s", expected, got)
		}
	}
	if strings.Contains(got, `"'NetworkTopologyStrategy'"`) || strings.Contains(got, `"'ashburn'"`) {
		t.Fatalf("normalizer rewrote string literals:\n%s", got)
	}
	if strings.Contains(got, "read_repair_chance") || !strings.Contains(got, "gc_grace_seconds = 864000") {
		t.Fatalf("normalizer did not remove only obsolete read-repair options:\n%s", got)
	}

	alt, err := deriveLegacyAlternatorSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	if len(alt.Tables) != 1 || alt.Tables[0].Describe == nil {
		t.Fatalf("unexpected derived Alternator schema: %#v", alt)
	}
	desc := alt.Tables[0].Describe
	if desc.TableName == nil || *desc.TableName != "sophena-main-prod" || len(desc.KeySchema) != 2 || len(desc.GlobalSecondaryIndexes) != 1 {
		t.Fatalf("unexpected derived table description: %#v", desc)
	}
	if desc.GlobalSecondaryIndexes[0].IndexName == nil || *desc.GlobalSecondaryIndexes[0].IndexName != "GSI1" || len(desc.AttributeDefinitions) != 4 {
		t.Fatalf("unexpected derived GSI description: %#v", desc)
	}
}

func TestParseLegacyAlternatorSchemaRejectsUnexpectedStatement(t *testing.T) {
	_, err := parseLegacySchemaArchive(context.Background(), legacySchemaTar(t, map[string]string{
		"alternator_app.cql": "DROP KEYSPACE system_auth;",
	}))
	if err == nil {
		t.Fatal("expected non-create statement to be rejected")
	}
}

func TestParseLegacySchemaArchiveRejectsUnsafeEntry(t *testing.T) {
	_, err := parseLegacySchemaArchive(context.Background(), legacySchemaTar(t, map[string]string{
		"../alternator_app.cql": "CREATE KEYSPACE alternator_app;",
	}))
	if err == nil {
		t.Fatal("expected path traversal entry to be rejected")
	}
}

func TestParseLegacySchemaArchiveRejectsSystemOnly(t *testing.T) {
	_, err := parseLegacySchemaArchive(context.Background(), legacySchemaTar(t, map[string]string{
		"system_schema.cql": "CREATE KEYSPACE system_schema;",
	}))
	if err == nil {
		t.Fatal("expected system-only archive to be rejected")
	}
}
