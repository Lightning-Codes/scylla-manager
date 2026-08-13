// Copyright (C) 2026 ScyllaDB

package restore

import "testing"

func TestIsLegacyGeneratedCDCLogStatement(t *testing.T) {
	tests := []struct {
		stmt string
		want bool
	}{
		{"CREATE TABLE alternator_app.app_scylla_cdc_log (pk text PRIMARY KEY)", true},
		{"  create table alternator_app.app_scylla_cdc_log (pk text PRIMARY KEY)", true},
		{"CREATE TABLE alternator_app.app (pk text PRIMARY KEY)", false},
		{"CREATE MATERIALIZED VIEW alternator_app.app:GSI1 AS SELECT * FROM alternator_app.app", false},
	}
	for _, tc := range tests {
		if got := isLegacyGeneratedCDCLogStatement(tc.stmt); got != tc.want {
			t.Errorf("isLegacyGeneratedCDCLogStatement(%q) = %v, want %v", tc.stmt, got, tc.want)
		}
	}
}
