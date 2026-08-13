// Copyright (C) 2026 ScyllaDB

package restore

import (
	"reflect"
	"testing"
)

func TestBaseColumnStatements(t *testing.T) {
	views := []View{
		{
			Keyspace: "alternator_app", BaseTable: "events", View: "events:GSI2", Type: AlternatorGlobalSecondaryIndex,
			CreateStmt: `{"TableName":"events","AttributeDefinitions":[{"AttributeName":"N","AttributeType":"N"},{"AttributeName":"S","AttributeType":"S"}],"GlobalSecondaryIndexUpdates":[]}`,
		},
		{
			Keyspace: "alternator_app", BaseTable: "events", View: "events:GSI1", Type: AlternatorGlobalSecondaryIndex,
			CreateStmt: `{"TableName":"events","AttributeDefinitions":[{"AttributeName":"S","AttributeType":"S"},{"AttributeName":"B","AttributeType":"B"}],"GlobalSecondaryIndexUpdates":[]}`,
		},
		{Keyspace: "ks", BaseTable: "base", View: "mv", Type: MaterializedView, CreateStmt: "ignored"},
	}

	got, err := baseColumnStatements(views)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		`ALTER TABLE "alternator_app"."events" ADD IF NOT EXISTS "B" blob`,
		`ALTER TABLE "alternator_app"."events" ADD IF NOT EXISTS "N" decimal`,
		`ALTER TABLE "alternator_app"."events" ADD IF NOT EXISTS "S" text`,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("statements = %#v, want %#v", got, want)
	}
}

func TestBaseColumnStatementsRejectsConflictingType(t *testing.T) {
	views := []View{
		{Keyspace: "ks", BaseTable: "t", View: "t:GSI1", Type: AlternatorGlobalSecondaryIndex, CreateStmt: `{"AttributeDefinitions":[{"AttributeName":"K","AttributeType":"S"}]}`},
		{Keyspace: "ks", BaseTable: "t", View: "t:GSI2", Type: AlternatorGlobalSecondaryIndex, CreateStmt: `{"AttributeDefinitions":[{"AttributeName":"K","AttributeType":"N"}]}`},
	}
	if _, err := baseColumnStatements(views); err == nil {
		t.Fatal("expected conflicting type error")
	}
}
