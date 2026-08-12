// Copyright (C) 2026 ScyllaDB

package managerclient

import (
	"bytes"
	"crypto/tls"
	"os"
	"strings"
	"testing"

	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla-manager/models"
)

func TestTLSConfigsAreStrict(t *testing.T) {
	if DefaultTLSConfig().InsecureSkipVerify {
		t.Fatal("default Manager API TLS disables certificate verification")
	}
	ca, err := os.ReadFile("../../../pkg/service/cluster/testdata/cluster.crt")
	if err != nil {
		t.Fatal(err)
	}
	clientCertificate := &tls.Certificate{}
	c, err := StrictTLSConfig(ca, "scylla-manager.scylla-manager.svc", clientCertificate)
	if err != nil {
		t.Fatal(err)
	}
	if c.InsecureSkipVerify || c.RootCAs == nil || c.ServerName != "scylla-manager.scylla-manager.svc" || len(c.Certificates) != 1 {
		t.Fatalf("strict TLS configuration is incomplete: %#v", c)
	}
	if _, err := StrictTLSConfig(ca, "", nil); err == nil {
		t.Fatal("missing server name accepted")
	}
	if _, err := StrictTLSConfig([]byte("invalid"), "manager.internal", nil); err == nil {
		t.Fatal("invalid CA accepted")
	}
}

func TestSecureClusterRenderUsesPresenceAndVerifiedStatus(t *testing.T) {
	var clusters bytes.Buffer
	if err := (ClusterSlice{&models.Cluster{
		ID:                       "cluster",
		CqlCredentialsSet:        true,
		AlternatorCredentialsSet: true,
		CqlCaSet:                 true,
		AlternatorCaSet:          true,
		AgentCaSet:               true,
	}}).Render(&clusters); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"CQL, Alternator", "CQL, Alternator, Agent"} {
		if !strings.Contains(clusters.String(), want) {
			t.Fatalf("cluster render missing %q:\n%s", want, clusters.String())
		}
	}

	var status bytes.Buffer
	if err := (ClusterStatus{&models.ClusterStatusItems0{
		Dc:                     "dc1",
		Host:                   "127.0.0.1",
		CqlStatus:              "UP",
		CqlTLSVerified:         true,
		CqlAuthVerified:        true,
		AlternatorStatus:       "UP",
		AlternatorTLSVerified:  true,
		AlternatorAuthVerified: true,
		RestStatus:             "UP",
		AgentTLSVerified:       true,
	}}).Render(&status); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"UP TLS AUTH", "UP TLS"} {
		if !strings.Contains(status.String(), want) {
			t.Fatalf("status render missing %q:\n%s", want, status.String())
		}
	}
}
