// Copyright (C) 2017 ScyllaDB

package testutils

import (
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/secrets"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

var flagAgentAuthToken = flag.String("agent-auth-token", "", "token to authenticate with agent")

// AgentAuthToken returns token to authenticate with agent.
func AgentAuthToken() string {
	if !flag.Parsed() {
		flag.Parse()
	}
	return *flagAgentAuthToken
}

// ManagedClusterAgentConfig returns a test client configuration that verifies
// the stable CA-backed Agent serving certificate used by the integration
// fixture. Tests must not recreate the production insecure TLS shortcut.
func ManagedClusterAgentConfig(tb testing.TB, hosts []string, token string) scyllaclient.Config {
	tb.Helper()
	config, err := ManagedClusterAgentConfigE(hosts, token)
	if err != nil {
		tb.Fatal(err)
	}
	return config
}

// ManagedClusterAgentConfigE is the error-returning variant for TestMain and
// integration setup helpers that do not have a testing.TB.
func ManagedClusterAgentConfigE(hosts []string, token string) (scyllaclient.Config, error) {
	ca, err := os.ReadFile(testconfig.ManagedClusterCAFile())
	if err != nil {
		return scyllaclient.Config{}, fmt.Errorf("read managed cluster CA: %w", err)
	}
	trust := secrets.NewAgentTLSTrust(uuid.Nil)
	trust.CA = ca
	trust.ServerName = testconfig.ManagedClusterTLSServerName()
	tlsConfig, err := secrets.TLSConfig(trust)
	if err != nil {
		return scyllaclient.Config{}, fmt.Errorf("build managed cluster Agent TLS config: %w", err)
	}
	transport := scyllaclient.DefaultTransport()
	transport.TLSClientConfig = tlsConfig
	config := scyllaclient.TestConfig(hosts, token)
	config.Transport = transport
	return config, nil
}
