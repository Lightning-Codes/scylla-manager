// Copyright (C) 2026 ScyllaDB

package schema

import (
	"regexp"
	"testing"
)

func TestV391SecureTablesUseIdempotentDDL(t *testing.T) {
	b, err := Files.ReadFile("v3.9.1.cql")
	if err != nil {
		t.Fatal(err)
	}

	re := regexp.MustCompile(`(?im)^\s*CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([a-z_]+)\s*\(`)
	matches := re.FindAllStringSubmatch(string(b), -1)
	if len(matches) != 2 {
		t.Fatalf("expected two idempotent CREATE TABLE statements, got %d", len(matches))
	}
	allCreate := regexp.MustCompile(`(?im)^\s*CREATE\s+TABLE\s+`).FindAll(b, -1)
	if len(allCreate) != len(matches) {
		t.Fatalf("every CREATE TABLE in v3.9.1 must use IF NOT EXISTS: %d total, %d idempotent", len(allCreate), len(matches))
	}

	want := map[string]bool{
		"secure_cluster":           false,
		"secure_connection_bundle": false,
	}
	for _, match := range matches {
		if _, ok := want[match[1]]; !ok {
			t.Fatalf("unexpected table %q in v3.9.1 migration", match[1])
		}
		want[match[1]] = true
	}
	for table, found := range want {
		if !found {
			t.Errorf("missing idempotent CREATE TABLE for %s", table)
		}
	}
}
