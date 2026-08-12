// Copyright (C) 2026 ScyllaDB

package cluster

import (
	"context"
	"crypto/tls"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/ping/cqlping"
	"github.com/scylladb/scylla-manager/v3/pkg/ping/dynamoping"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/secrets"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type securityMemoryStore map[string][]byte

func securityStoreKey(v store.Entry) string {
	id, key := v.Key()
	return id.String() + "/" + key
}

func (s securityMemoryStore) Put(v store.Entry) error {
	b, err := v.MarshalBinary()
	if err == nil {
		s[securityStoreKey(v)] = append([]byte(nil), b...)
	}
	return err
}

func (s securityMemoryStore) Get(v store.Entry) error {
	b, ok := s[securityStoreKey(v)]
	if !ok {
		return util.ErrNotFound
	}
	return v.UnmarshalBinary(b)
}

func (s securityMemoryStore) Check(v store.Entry) (bool, error) {
	_, ok := s[securityStoreKey(v)]
	return ok, nil
}

func (s securityMemoryStore) Delete(v store.Entry) error {
	delete(s, securityStoreKey(v))
	return nil
}

func (securityMemoryStore) DeleteAll(uuid.UUID) error { return nil }

func TestShouldValidateHostsConnectivityOnSecureUpdate(t *testing.T) {
	base := Cluster{AuthToken: "token", Host: "host", Port: 10001}
	tests := map[string]func(*Cluster){
		"host":                      func(c *Cluster) { c.Host = "other" },
		"port":                      func(c *Cluster) { c.Port++ },
		"Agent token":               func(c *Cluster) { c.AuthToken = "rotated" },
		"Agent CA":                  func(c *Cluster) { c.AgentCAFile = []byte("ca") },
		"Agent name":                func(c *Cluster) { c.AgentServerName = "agent" },
		"CQL CA":                    func(c *Cluster) { c.CQLCAFile = []byte("ca") },
		"CQL name":                  func(c *Cluster) { c.CQLServerName = "cql" },
		"Alternator CA":             func(c *Cluster) { c.AlternatorCAFile = []byte("ca") },
		"Alternator name":           func(c *Cluster) { c.AlternatorServerName = "alternator" },
		"client certificate":        func(c *Cluster) { c.SSLUserCertFile = []byte("cert") },
		"client key":                func(c *Cluster) { c.SSLUserKeyFile = []byte("key") },
		"CQL credentials":           func(c *Cluster) { c.Username, c.Password = "user", "password" },
		"Alternator credentials":    func(c *Cluster) { c.AlternatorAccessKeyID, c.AlternatorSecretAccessKey = "id", "secret" },
		"force TLS disabled":        func(c *Cluster) { c.ForceTLSDisabled = true },
		"force non-TLS port policy": func(c *Cluster) { c.ForceNonSSLSessionPort = true },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			current := base
			mutate(&current)
			if !shouldValidateHostsConnectivityOnUpdate(&current, &base) {
				t.Fatal("secure connectivity validation was not requested")
			}
		})
	}
	if shouldValidateHostsConnectivityOnUpdate(&base, &base) {
		t.Fatal("unchanged cluster unexpectedly requires connectivity validation")
	}
}

func TestValidateNodeDataPlaneConnectivityUsesPendingTrustAndCredentials(t *testing.T) {
	ca, err := os.ReadFile("testdata/cluster.crt")
	if err != nil {
		t.Fatal(err)
	}
	cert, err := os.ReadFile("testdata/cluster.crt")
	if err != nil {
		t.Fatal(err)
	}
	key, err := os.ReadFile("testdata/cluster.key")
	if err != nil {
		t.Fatal(err)
	}
	c := &Cluster{
		ID:                        uuid.MustRandom(),
		AuthToken:                 "token",
		CQLCAFile:                 ca,
		CQLServerName:             "cql.internal",
		AlternatorCAFile:          ca,
		AlternatorServerName:      "alternator.internal",
		Username:                  "user",
		Password:                  "password",
		AlternatorAccessKeyID:     "access",
		AlternatorSecretAccessKey: "secret",
		SSLUserCertFile:           cert,
		SSLUserKeyFile:            key,
	}
	ni := &scyllaclient.NodeInfo{
		ClientEncryptionEnabled:        true,
		ClientEncryptionRequireAuth:    true,
		CqlPasswordProtected:           true,
		ListenAddress:                  "0.0.0.0",
		NativeTransportPort:            "9042",
		NativeTransportPortSsl:         "9142",
		AlternatorAddress:              "0.0.0.0",
		AlternatorHTTPSPort:            "8043",
		AlternatorEnforceAuthorization: true,
	}

	var cqlCalls, alternatorCalls int
	s := &Service{
		secretsStore:  securityMemoryStore{},
		timeoutConfig: scyllaclient.TimeoutConfig{Timeout: time.Second},
		logger:        log.NewDevelopment(),
		cqlQueryPing: func(_ context.Context, config cqlping.Config, username, password string) (time.Duration, error) {
			cqlCalls++
			assertStrictTLSConfig(t, config.TLSConfig, "cql.internal")
			if config.Addr != "127.0.0.1:9142" {
				t.Fatalf("unexpected CQL address %q", config.Addr)
			}
			if username != "user" || password != "password" {
				t.Fatal("pending CQL credentials were not used")
			}
			if len(config.TLSConfig.Certificates) != 1 {
				t.Fatal("pending CQL client identity was not used")
			}
			return time.Millisecond, nil
		},
		alternatorQueryPing: func(ctx context.Context, config dynamoping.Config) (time.Duration, error) {
			alternatorCalls++
			assertStrictTLSConfig(t, config.TLSConfig, "alternator.internal")
			if config.Addr != "https://127.0.0.1:8043" {
				t.Fatalf("unexpected Alternator address %q", config.Addr)
			}
			credentials, err := config.Credentials.Retrieve(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if credentials.AccessKeyID != "access" || credentials.SecretAccessKey != "secret" {
				t.Fatal("pending Alternator credentials were not used")
			}
			return time.Millisecond, nil
		},
	}

	if err := s.validateNodeDataPlaneConnectivity(context.Background(), c, "127.0.0.1", ni, true); err != nil {
		t.Fatal(err)
	}
	if cqlCalls != 1 || alternatorCalls != 1 {
		t.Fatalf("unexpected query counts: CQL=%d Alternator=%d", cqlCalls, alternatorCalls)
	}
}

func TestValidateNodeDataPlaneConnectivityFailsClosed(t *testing.T) {
	c := &Cluster{ID: uuid.MustRandom(), AuthToken: "token"}
	ni := &scyllaclient.NodeInfo{
		ClientEncryptionEnabled: true,
		NativeTransportPortSsl:  "9142",
		AlternatorHTTPSPort:     "8043",
	}
	s := &Service{
		secretsStore:  securityMemoryStore{},
		timeoutConfig: scyllaclient.TimeoutConfig{Timeout: time.Second},
		logger:        log.NewDevelopment(),
		cqlQueryPing: func(context.Context, cqlping.Config, string, string) (time.Duration, error) {
			t.Fatal("query must not run without strict trust")
			return 0, nil
		},
		alternatorQueryPing: func(context.Context, dynamoping.Config) (time.Duration, error) {
			t.Fatal("query must not run without strict trust")
			return 0, nil
		},
	}
	if err := s.validateNodeDataPlaneConnectivity(context.Background(), c, "127.0.0.1", ni, true); err == nil || !strings.Contains(err.Error(), "were not supplied for cluster creation") {
		t.Fatalf("expected missing inline trust error, got %v", err)
	}

	queryErr := errors.New("verified query rejected")
	ca, err := os.ReadFile("testdata/cluster.crt")
	if err != nil {
		t.Fatal(err)
	}
	c.CQLCAFile, c.CQLServerName = ca, "cql.internal"
	c.Username, c.Password = "user", "password"
	s.cqlQueryPing = func(context.Context, cqlping.Config, string, string) (time.Duration, error) {
		return 0, queryErr
	}
	if err := s.validateNodeDataPlaneConnectivity(context.Background(), c, "127.0.0.1", &scyllaclient.NodeInfo{
		ClientEncryptionEnabled: true,
		CqlPasswordProtected:    true,
		ListenAddress:           "0.0.0.0",
		NativeTransportPortSsl:  "9142",
	}, true); !errors.Is(err, queryErr) {
		t.Fatalf("expected query failure, got %v", err)
	}
}

func TestValidateNodeDataPlaneConnectivitySkipsUnencryptedEndpoints(t *testing.T) {
	s := &Service{
		secretsStore: securityMemoryStore{},
		cqlQueryPing: func(context.Context, cqlping.Config, string, string) (time.Duration, error) {
			t.Fatal("CQL secure query unexpectedly ran")
			return 0, nil
		},
		alternatorQueryPing: func(context.Context, dynamoping.Config) (time.Duration, error) {
			t.Fatal("Alternator secure query unexpectedly ran")
			return 0, nil
		},
	}
	if err := s.validateNodeDataPlaneConnectivity(context.Background(), &Cluster{}, "127.0.0.1", &scyllaclient.NodeInfo{
		NativeTransportPort: "9042",
		AlternatorPort:      "8000",
	}, false); err != nil {
		t.Fatal(err)
	}
}

func TestValidateNodeDataPlaneConnectivityRejectsAuthenticatedPlaintext(t *testing.T) {
	s := &Service{secretsStore: securityMemoryStore{}}
	for name, ni := range map[string]*scyllaclient.NodeInfo{
		"CQL": {
			CqlPasswordProtected: true,
			NativeTransportPort:  "9042",
		},
		"Alternator": {
			AlternatorPort:                 "8000",
			AlternatorEnforceAuthorization: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := s.validateNodeDataPlaneConnectivity(context.Background(), &Cluster{}, "127.0.0.1", ni, false)
			if err == nil || !strings.Contains(err.Error(), "without TLS") {
				t.Fatalf("authenticated plaintext was not rejected: %v", err)
			}
		})
	}
}

func TestOperationalClientsNeverLoadCredentialsForPlaintextEndpoints(t *testing.T) {
	s := &Service{secretsStore: panicGetSecurityStore{securityMemoryStore: securityMemoryStore{}}}

	cqlConfig := gocql.NewCluster("127.0.0.1")
	if err := s.extendClusterConfigWithAuthentication(&Cluster{ID: uuid.MustRandom()}, &scyllaclient.NodeInfo{
		CqlPasswordProtected: true,
	}, cqlConfig); err == nil || !strings.Contains(err.Error(), "refusing to transmit") {
		t.Fatalf("plaintext CQL session did not fail before loading credentials: %v", err)
	}

	if _, err := s.alternatorClientConfig(context.Background(), uuid.MustRandom(), "127.0.0.1", &scyllaclient.NodeInfo{
		AlternatorPort:                 "8000",
		AlternatorEnforceAuthorization: true,
	}); err == nil || !strings.Contains(err.Error(), "refusing to transmit") {
		t.Fatalf("plaintext Alternator client did not fail before loading credentials: %v", err)
	}
}

type panicGetSecurityStore struct{ securityMemoryStore }

func (panicGetSecurityStore) Get(store.Entry) error {
	panic("plaintext operational client attempted to load credentials")
}

func TestDeleteAgentTLSTrustIsRejectedWithoutMutation(t *testing.T) {
	id := uuid.MustRandom()
	trust := secrets.NewAgentTLSTrust(id)
	trust.CA = mustSecurityTestCA(t)
	trust.ServerName = "agent.internal"
	secretsStore := securityMemoryStore{}
	if err := secretsStore.Put(trust); err != nil {
		t.Fatal(err)
	}
	s := &Service{secretsStore: secretsStore}
	if err := s.DeleteTLSTrust(context.Background(), id, secrets.AgentProtocol); !util.IsErrValidate(err) {
		t.Fatalf("expected mandatory Agent trust validation error, got %v", err)
	}
	if configured, err := secretsStore.Check(secrets.NewAgentTLSTrust(id)); err != nil || !configured {
		t.Fatalf("Agent trust was mutated: configured=%v err=%v", configured, err)
	}
}

func TestCreateRejectsOrphanedSecrets(t *testing.T) {
	id := uuid.MustRandom()
	for name, entry := range map[string]store.Entry{
		"CQL credentials":        &secrets.CQLCreds{ClusterID: id, Username: "user", Password: "password"},
		"Alternator credentials": &secrets.AlternatorCreds{ClusterID: id, AccessKeyID: "access", SecretAccessKey: "secret"},
		"TLS identity":           &secrets.TLSIdentity{ClusterID: id, Cert: []byte("cert"), PrivateKey: []byte("key")},
		"CQL trust":              securityTestTrust(t, secrets.NewCQLTLSTrust(id), "cql.internal"),
		"Alternator trust":       securityTestTrust(t, secrets.NewAlternatorTLSTrust(id), "alternator.internal"),
		"Agent trust":            securityTestTrust(t, secrets.NewAgentTLSTrust(id), "agent.internal"),
	} {
		t.Run(name, func(t *testing.T) {
			secretsStore := securityMemoryStore{}
			if err := secretsStore.Put(entry); err != nil {
				t.Fatal(err)
			}
			s := &Service{secretsStore: secretsStore}
			if err := s.ensureNoStoredSecretsOnCreate(id); !util.IsErrValidate(err) || !strings.Contains(err.Error(), "orphaned secrets") {
				t.Fatalf("orphaned secret was not rejected: %v", err)
			}
		})
	}
}

func securityTestTrust(t *testing.T, trust *secrets.TLSTrust, serverName string) *secrets.TLSTrust {
	t.Helper()
	trust.CA = mustSecurityTestCA(t)
	trust.ServerName = serverName
	return trust
}

func mustSecurityTestCA(t *testing.T) []byte {
	t.Helper()
	ca, err := os.ReadFile("testdata/cluster.crt")
	if err != nil {
		t.Fatal(err)
	}
	return ca
}

func assertStrictTLSConfig(t *testing.T, config *tls.Config, serverName string) {
	t.Helper()
	if config == nil || config.InsecureSkipVerify || config.RootCAs == nil || config.ServerName != serverName {
		t.Fatalf("TLS config is not strict for %q: %#v", serverName, config)
	}
}
