// Copyright (C) 2026 ScyllaDB

package cluster

import (
	"os"
	"testing"
)

func TestClusterValidateSecureAgentAndAtomicTrust(t *testing.T) {
	ca, err := os.ReadFile("testdata/cluster.crt")
	if err != nil {
		t.Fatal(err)
	}

	for name, c := range map[string]*Cluster{
		"missing Agent token": {},
		"CA without identity": {
			AuthToken: "set",
			CQLCAFile: ca,
		},
		"identity without CA": {
			AuthToken:     "set",
			CQLServerName: "cql.internal",
		},
		"TLS downgrade": {
			AuthToken:        "set",
			CQLCAFile:        ca,
			CQLServerName:    "cql.internal",
			ForceTLSDisabled: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := c.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}

	valid := &Cluster{
		AuthToken:            "set",
		CQLCAFile:            ca,
		CQLServerName:        "cql.internal",
		AlternatorCAFile:     ca,
		AlternatorServerName: "alternator.internal",
		AgentCAFile:          ca,
		AgentServerName:      "agent.internal",
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid secure cluster rejected: %v", err)
	}
}
