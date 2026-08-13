// Copyright (C) 2026 ScyllaDB

package backup

import (
	"archive/tar"
	"bytes"
	"context"
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
	if got := schema[0]; got.Keyspace != "alternator_app" || got.Name != "alternator_app" || got.Type != "legacy_keyspace" || got.CQLStmt != appCQL {
		t.Fatalf("unexpected legacy schema row: %#v", got)
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
