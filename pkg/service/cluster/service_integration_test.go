// Copyright (C) 2017 ScyllaDB

//go:build all || integration

package cluster_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/config/server"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util"

	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/secrets"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestValidateHostConnectivityIntegration(t *testing.T) {
	if IsIPV6Network() {
		t.Skip("DB node do not have ip6tables and related modules to make it work properly")
	}

	Print("given: the fresh cluster")
	var (
		ctx     = context.Background()
		session = CreateScyllaManagerDBSession(t)
		c       = &cluster.Cluster{
			AuthToken: "token",
			Host:      ManagedClusterHost(),
		}
	)
	secureManagedCluster(t, c)
	s, err := cluster.NewService(session, metrics.NewClusterMetrics(), scyllaclient.DefaultTimeoutConfig(),
		server.DefaultConfig().ClientCacheTimeout, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	err = s.PutCluster(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}

	allHosts := ManagedClusterHosts()
	for _, tc := range []struct {
		name      string
		hostsDown []string
		result    error
		timeout   time.Duration
	}{
		{
			name:      "coordinator host is DOWN",
			hostsDown: []string{ManagedClusterHost()},
			result:    nil,
			timeout:   6 * time.Second,
		},
		{
			name:      "only one is UP",
			hostsDown: allHosts[:len(allHosts)-1],
			result:    nil,
			timeout:   6 * time.Second,
		},
		{
			name:      "all hosts are DOWN",
			hostsDown: allHosts,
			result:    cluster.ErrNoValidKnownHost,
			timeout:   11 * time.Second, // the 5 seconds calls will timeout twice
		},
		{
			name:      "all hosts are UP",
			hostsDown: nil,
			result:    nil,
			timeout:   6 * time.Second,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				for _, host := range tc.hostsDown {
					if err := StartService(host, "scylla"); err != nil {
						t.Logf("error on starting stopped scylla service on host={%s}, err={%s}", host, err)
					}
					if err := RunIptablesCommand(t, host, CmdUnblockScyllaREST); err != nil {
						t.Logf("error trying to unblock REST API on host = {%s}, err={%s}", host, err)
					}
				}
			}()
			TryUnblockCQL(t, ManagedClusterHosts(), IsSSLEnabled())
			TryUnblockREST(t, ManagedClusterHosts())
			TryUnblockAlternator(t, ManagedClusterHosts(), IsSSLEnabled())
			TryStartAgent(t, ManagedClusterHosts())
			if err := EnsureNodesAreUP(t, ManagedClusterHosts(), time.Minute); err != nil {
				t.Fatalf("not all nodes are UP, err = {%v}", err)
			}

			Printf("then: validate that call to validate host connectivity takes less than %v seconds", tc.timeout.Seconds())
			testCluster, err := s.GetClusterByID(context.Background(), c.ID)
			if err != nil {
				t.Fatal(err)
			}
			if err := callValidateHostConnectivityWithTimeout(ctx, s, tc.timeout, testCluster); err != nil {
				t.Fatal(err)
			}
			Printf("when: the scylla service is stopped and the scylla API is timing out on some hosts")
			// It's needed to block Scylla REST API, so that the clients are just hanging when they call the API.
			// Scylla service must be stopped to make the node to report DOWN status. Blocking REST API is not
			// enough.
			for _, host := range tc.hostsDown {
				if err := StopService(host, "scylla"); err != nil {
					t.Fatal(err)
				}
				if err := RunIptablesCommand(t, host, CmdBlockScyllaREST); err != nil {
					t.Error(err)
				}
			}

			Printf("then: validate that call still takes less than %v seconds", tc.timeout.Seconds())
			if err := callValidateHostConnectivityWithTimeout(ctx, s, tc.timeout, testCluster); !errors.Is(err, tc.result) {
				t.Fatal(err)
			}
		})
	}
}

func callValidateHostConnectivityWithTimeout(ctx context.Context, s *cluster.Service, timeout time.Duration,
	c *cluster.Cluster) error {

	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan error)
	go func() {
		done <- s.ValidateHostsConnectivity(callCtx, c)
	}()

	select {
	case <-time.After(timeout):
		cancel()
		return fmt.Errorf("expected s.ValidateHostsConnectivity to complete in less than %v seconds, time exceeded", timeout.Seconds())
	case err := <-done:
		return err
	}
}

func TestClientIntegration(t *testing.T) {
	expectedHosts := ManagedClusterHosts()

	session := CreateScyllaManagerDBSession(t)
	s, err := cluster.NewService(session, metrics.NewClusterMetrics(), scyllaclient.DefaultTimeoutConfig(),
		server.DefaultConfig().ClientCacheTimeout, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	c := &cluster.Cluster{
		AuthToken: "token",
		Host:      ManagedClusterHost(),
	}
	secureManagedCluster(t, c)
	err = s.PutCluster(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	c, err = s.GetClusterByID(context.Background(), c.ID)
	if err != nil {
		t.Fatal(err)
	}

	// #1
	// update cluster.known_hosts to put non-existing IPs on top of existing one
	// the expectation is that client will finally manage to clean it up
	// and will update known_hosts to its proper values
	fakeHostWithOnlyOneCorrect := []string{"192.168.10.1", "192.168.10.2", c.KnownHosts[0]}
	staleGeneration := c.ConnectionGeneration
	staleSnapshot := *c
	staleSnapshot.KnownHosts = fakeHostWithOnlyOneCorrect
	staleBundle := cluster.ConnectionBundleForTest(&staleSnapshot, uuid.MustRandom())
	staleBundle.PreviousGeneration = staleGeneration
	if err := s.CommitConnectionGenerationForTest(context.Background(), &staleSnapshot, staleBundle, staleGeneration, false); err != nil {
		t.Fatal(err)
	}

	_, err = s.Client(context.Background(), c.ID)
	if err != nil {
		t.Fatal("Cannot create Scylla API client", err)
	}
	c, err = s.GetClusterByID(context.Background(), c.ID)
	if err != nil {
		t.Fatal(err)
	}

	// assert that all nodes are available on cluster.known_hosts
	diff := ipsNotInSlice(c.KnownHosts, expectedHosts)
	if len(diff) > 0 {
		t.Fatalf("Not all expected elements are available on cluster knownHosts, current = {%v}, expected = {%v}",
			c.KnownHosts, expectedHosts)
	}
}

func ipsNotInSlice(a []string, b []string) []string {
	m := make(map[string]struct{})
	for _, elem := range a {
		m[net.ParseIP(elem).String()] = struct{}{}
	}

	var diff []string
	for _, elem := range b {
		_, ok := m[net.ParseIP(elem).String()]
		if !ok {
			diff = append(diff, elem)
		}
	}

	return diff
}

func TestAlternatorClientIntegration(t *testing.T) {
	smSession := CreateScyllaManagerDBSession(t)
	defer smSession.Close()

	s, err := cluster.NewService(smSession, metrics.NewClusterMetrics(), scyllaclient.DefaultTimeoutConfig(),
		server.DefaultConfig().ClientCacheTimeout, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	c := &cluster.Cluster{
		AuthToken: "token",
		Host:      ManagedClusterHost(),
	}
	secureManagedCluster(t, c)
	if err = s.PutCluster(context.Background(), c); err != nil {
		t.Fatal(err)
	}

	scClient, err := s.CreateClientNoCache(context.Background(), c.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer scClient.Close()

	clusterSession := CreateManagedClusterSession(t, false, scClient, "", "")
	defer clusterSession.Close()

	c.AlternatorAccessKeyID, c.AlternatorSecretAccessKey = GetAlternatorCreds(t, clusterSession, "")
	if err = s.PutCluster(context.Background(), c); err != nil {
		t.Fatal(err)
	}

	client, err := s.GetAlternatorClient(context.Background(), c.ID, ManagedClusterHost())
	if err != nil {
		t.Fatal(err)
	}

	const tableName = ".scylla.alternator.system_schema.tables"
	out, err := client.Scan(context.Background(), &dynamodb.ScanInput{
		TableName: aws.String(tableName),
		Limit:     aws.Int32(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	if out == nil {
		t.Fatal("Expected non-nil scan output")
	}
	if out.Count != 1 {
		t.Fatalf("Expected 1 item in scan output, got: %d", out.Count)
	}
}

func TestServiceStorageIntegration(t *testing.T) {
	session := CreateScyllaManagerDBSession(t)

	connectionStore := store.NewTableStore(session, table.SecureConnectionBundle)

	cfg := scyllaclient.DefaultTimeoutConfig()
	cfg.Timeout = 2 * time.Second
	cfg.Backoff.WaitMax = 2 * time.Second
	cfg.Backoff.MaxRetries = 1
	s, err := cluster.NewService(session, metrics.NewClusterMetrics(), cfg,
		server.DefaultConfig().ClientCacheTimeout, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}

	var change cluster.Change
	s.SetOnChangeListener(func(ctx context.Context, c cluster.Change) error {
		change = c
		return nil
	})

	setup := func(t *testing.T) {
		t.Helper()
		ExecStmt(t, session, "TRUNCATE cluster")
		ExecStmt(t, session, "TRUNCATE secure_cluster")
		ExecStmt(t, session, "TRUNCATE secure_connection_bundle")
		ExecStmt(t, session, "TRUNCATE secrets")
	}

	ctx := context.Background()

	diffOpts := cmp.Options{
		UUIDComparer(),
		cmpopts.IgnoreFields(cluster.Cluster{}, "Host", "KnownHosts",
			"CQLCAFile", "CQLServerName", "AlternatorCAFile", "AlternatorServerName", "AgentCAFile", "AgentServerName"),
		cmpopts.SortSlices(func(a, b *cluster.Cluster) bool {
			return a.ID.String() < b.ID.String()
		}),
	}

	t.Run("list empty", func(t *testing.T) {
		setup(t)

		clusters, err := s.ListClusters(ctx, &cluster.Filter{})
		if err != nil {
			t.Fatal(err)
		}
		if len(clusters) != 0 {
			t.Fatal("expected 0 len result")
		}
	})

	t.Run("list not empty", func(t *testing.T) {
		setup(t)

		expected := make([]*cluster.Cluster, 3)
		for i := range expected {
			c := &cluster.Cluster{
				ID:        uuid.NewTime(),
				Name:      "name" + strconv.Itoa(i),
				Host:      ManagedClusterHost(),
				AuthToken: AgentAuthToken(),
			}
			secureManagedCluster(t, c)
			if err := s.PutCluster(ctx, c); err != nil {
				t.Fatal(err)
			}
			expected[i] = c
		}

		clusters, err := s.ListClusters(ctx, &cluster.Filter{})
		if err != nil {
			t.Fatal(err)
		}

		if diff := cmp.Diff(clusters, expected, diffOpts...); diff != "" {
			t.Fatal(diff)
		}
	})

	t.Run("get missing cluster", func(t *testing.T) {
		setup(t)

		c, err := s.GetClusterByID(ctx, uuid.MustRandom())
		if !errors.Is(err, util.ErrNotFound) {
			t.Fatal("expected not found")
		}
		if c != nil {
			t.Fatal("expected nil")
		}
	})

	t.Run("get cluster", func(t *testing.T) {
		setup(t)

		c0 := validCluster(t)
		c0.ID = uuid.Nil

		if err := s.PutCluster(ctx, c0); err != nil {
			t.Fatal(err)
		}
		if c0.ID == uuid.Nil {
			t.Fatal("ID not updated")
		}
		c1, err := s.GetClusterByID(ctx, c0.ID)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(c0, c1, diffOpts...); diff != "" {
			t.Fatal("read write mismatch", diff)
		}

		c2, err := s.GetClusterByName(ctx, c0.Name)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(c0, c2, diffOpts...); diff != "" {
			t.Fatal("read write mismatch", diff)
		}
	})

	t.Run("get cluster name", func(t *testing.T) {
		setup(t)

		c := validCluster(t)
		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		clusterName, err := s.GetClusterName(ctx, c.ID)
		if err != nil {
			t.Fatal(err)
		}
		if clusterName != c.Name {
			t.Fatal("expected", c.Name, "got", clusterName)
		}
	})

	t.Run("put nil cluster", func(t *testing.T) {
		setup(t)

		if err := s.PutCluster(ctx, nil); err == nil {
			t.Fatal("expected validation error")
		} else {
			t.Log(err)
		}
	})

	t.Run("put conflicting cluster name", func(t *testing.T) {
		setup(t)

		c0 := validCluster(t)

		if err := s.PutCluster(ctx, c0); err != nil {
			t.Fatal(err)
		}

		c1 := c0
		c1.ID = uuid.Nil

		if err := s.PutCluster(ctx, c0); err == nil {
			t.Fatal("expected validation error")
		} else {
			t.Log(err)
		}
	})

	t.Run("put cluster with wrong auth token", func(t *testing.T) {
		setup(t)

		c := validCluster(t)
		c.AuthToken = "foobar"

		if err := s.PutCluster(ctx, c); err == nil {
			t.Fatal("expected validation error")
		} else {
			t.Log(err)
		}
	})

	t.Run("put new cluster", func(t *testing.T) {
		setup(t)

		c := validCluster(t)
		c.ID = uuid.Nil

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		if c.ID == uuid.Nil {
			t.Fatal("id not set")
		}
		if change.ID != c.ID {
			t.Fatal("id mismatch")
		}
		if change.Type != cluster.Create {
			t.Fatal("invalid type", change)
		}
	})

	assertSecrets := func(t *testing.T, c *cluster.Cluster) {
		t.Helper()
		active, err := s.GetClusterByID(ctx, c.ID)
		if err != nil {
			t.Fatal(err)
		}
		bundle := secrets.NewConnectionBundle(c.ID)
		bundle.Generation = active.ConnectionGeneration
		if err := connectionStore.Get(bundle); err != nil {
			t.Fatal(err)
		}
		if bundle.CQLUsername != c.Username || bundle.CQLPassword != c.Password ||
			bundle.AlternatorAccessKeyID != c.AlternatorAccessKeyID || bundle.AlternatorSecretAccessKey != c.AlternatorSecretAccessKey ||
			!bytes.Equal(bundle.CQLClientCertificate, c.SSLUserCertFile) || !bytes.Equal(bundle.CQLClientPrivateKey, c.SSLUserKeyFile) {
			t.Fatalf("active connection bundle does not contain one complete secret tuple: %#v", bundle)
		}
	}

	t.Run("put new cluster with secrets", func(t *testing.T) {
		setup(t)

		c := tlsCluster(t)
		c.ID = uuid.Nil

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}

		assertSecrets(t, c)
	})

	t.Run("update cluster with secrets", func(t *testing.T) {
		setup(t)

		c := tlsCluster(t)
		c.ID = uuid.Nil

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}

		c.SSLUserCertFile, err = os.ReadFile("testdata/cluster_update.crt")
		if err != nil {
			t.Fatal(err)
		}
		c.SSLUserKeyFile, err = os.ReadFile("testdata/cluster_update.key")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}

		assertSecrets(t, c)
	})

	t.Run("check existing CQL credentials", func(t *testing.T) {
		setup(t)

		c := tlsCluster(t)
		c.ID = uuid.Nil

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		ok, err := s.CheckCQLCredentials(c.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("expected true")
		}
	})

	t.Run("check non-existing CQL credentials", func(t *testing.T) {
		setup(t)

		c := tlsCluster(t)
		c.ID = uuid.Nil

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteCQLCredentials(ctx, c.ID); err != nil {
			t.Fatal(err)
		}
		ok, err := s.CheckCQLCredentials(c.ID)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatal("expected false")
		}
	})

	t.Run("check existing alternator credentials", func(t *testing.T) {
		setup(t)

		c := tlsCluster(t)
		c.ID = uuid.Nil

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		ok, err := s.CheckAlternatorCredentials(c.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("expected true")
		}
	})

	t.Run("check non-existing alternator credentials", func(t *testing.T) {
		setup(t)

		c := tlsCluster(t)
		c.ID = uuid.Nil

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteAlternatorCredentials(ctx, c.ID); err != nil {
			t.Fatal(err)
		}
		ok, err := s.CheckAlternatorCredentials(c.ID)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Fatal("expected false")
		}
	})

	t.Run("delete cluster commits empty tombstone and retains old immutable bundle", func(t *testing.T) {
		setup(t)

		c := tlsCluster(t)
		c.ID = uuid.Nil

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		active, err := s.GetClusterByID(ctx, c.ID)
		if err != nil {
			t.Fatal(err)
		}
		oldBundle := secrets.NewConnectionBundle(c.ID)
		oldBundle.Generation = active.ConnectionGeneration
		if err := connectionStore.Get(oldBundle); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteCluster(ctx, c.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := s.GetClusterByID(ctx, c.ID); !errors.Is(err, util.ErrNotFound) {
			t.Fatalf("deleted tombstone remained visible: %v", err)
		}
		// A retained old row is still immutable and addressable to a reader that
		// captured A before D committed; it is never selected by new callers.
		retained := secrets.NewConnectionBundle(c.ID)
		retained.Generation = oldBundle.Generation
		if err := connectionStore.Get(retained); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("delete CQL credentials", func(t *testing.T) {
		setup(t)

		c := tlsCluster(t)
		c.ID = uuid.Nil

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteCQLCredentials(ctx, c.ID); err != nil {
			t.Fatal(err)
		}

		configured, err := s.CheckCQLCredentials(c.ID)
		if err != nil || configured {
			t.Fatalf("CQL credentials still active: configured=%v err=%v", configured, err)
		}
	})

	t.Run("delete alternator credentials", func(t *testing.T) {
		setup(t)

		c := tlsCluster(t)
		c.ID = uuid.Nil

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteAlternatorCredentials(ctx, c.ID); err != nil {
			t.Fatal(err)
		}

		configured, err := s.CheckAlternatorCredentials(c.ID)
		if err != nil || configured {
			t.Fatalf("Alternator credentials still active: configured=%v err=%v", configured, err)
		}
	})

	t.Run("delete SSL cert", func(t *testing.T) {
		setup(t)

		c := tlsCluster(t)
		c.ID = uuid.Nil

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteSSLUserCert(ctx, c.ID); err != nil {
			t.Fatal(err)
		}

		configured, err := s.CheckSSLUserCert(c.ID)
		if err != nil || configured {
			t.Fatalf("CQL client identity still active: configured=%v err=%v", configured, err)
		}
	})

	t.Run("put new cluster without automatic repair", func(t *testing.T) {
		setup(t)

		c := validCluster(t)
		c.ID = uuid.Nil
		c.WithoutRepair = true

		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		if !change.WithoutRepair {
			t.Fatal("automatic repair scheduling not skipped")
		}
	})

	t.Run("put existing cluster", func(t *testing.T) {
		setup(t)

		c := validCluster(t)
		// Given cluster
		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		if change.ID != c.ID {
			t.Fatal("id mismatch")
		}
		if change.Type != cluster.Create {
			t.Fatal("invalid type", change)
		}

		// Then PutCluster with same data results in Update
		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		if change.ID != c.ID {
			t.Fatal("id mismatch")
		}
		if change.Type != cluster.Update {
			t.Fatal("invalid type", change)
		}
	})

	t.Run("delete missing cluster", func(t *testing.T) {
		setup(t)

		id := uuid.MustRandom()

		err := s.DeleteCluster(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("delete cluster", func(t *testing.T) {
		setup(t)

		c := validCluster(t)
		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteCluster(ctx, c.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := s.GetClusterByID(ctx, c.ID); !errors.Is(err, util.ErrNotFound) {
			t.Fatal(err)
		}
		if change.ID != c.ID {
			t.Fatal("id mismatch")
		}
		if change.Type != cluster.Delete {
			t.Fatal("invalid type", change)
		}
	})

	t.Run("failed connectivity check", func(t *testing.T) {
		if IsIPV6Network() {
			t.Skip("DB node do not have ip6tables and related modules to make it work properly")
		}

		setup(t)
		hosts := ManagedClusterHosts()
		if len(hosts) < 2 {
			t.Skip("not enough nodes in the cluster")
		}
		h1 := hosts[0]
		h2 := hosts[1]

		c := validCluster(t)
		c.Host = h1
		if err := RunIptablesCommand(t, h2, CmdBlockScyllaREST); err != nil {
			t.Fatal(err)
		}
		defer RunIptablesCommand(t, h2, CmdUnblockScyllaREST)

		if err := s.PutCluster(ctx, c); err == nil {
			t.Fatal("expected put cluster to fail because of connectivity issues")
		} else {
			t.Logf("put cluster ended with expected error: %s", err)
		}

		clusters, err := s.ListClusters(ctx, &cluster.Filter{})
		if err != nil {
			t.Fatalf("list clusters: %s", err)
		}
		if len(clusters) != 0 {
			t.Fatalf("expected no clusters to be listed, got: %v", clusters)
		}

		var cnt int
		if err := session.Query("SELECT COUNT(*) FROM secure_cluster", nil).GetRelease(&cnt); err != nil {
			t.Fatalf("check secure Manager cluster table entries: %s", err)
		}
		if cnt != 0 {
			t.Fatalf("expected no entries in SM DB cluster table, got: %d", cnt)
		}
	})

	t.Run("no connectivity check on update", func(t *testing.T) {
		if IsIPV6Network() {
			t.Skip("DB node do not have ip6tables and related modules to make it work properly")
		}

		setup(t)
		hosts := ManagedClusterHosts()
		clusterHost := hosts[0]
		initialCluster := *validCluster(t)
		initialCluster.Host = clusterHost
		Print("Create initial cluster with host: " + clusterHost)
		if err = s.PutCluster(ctx, &initialCluster); err != nil {
			t.Fatal(err)
		}
		Print("Known hosts are set after cluster creation")
		getCluster, err := s.GetClusterByID(t.Context(), initialCluster.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(getCluster.KnownHosts) != len(hosts) {
			t.Fatalf("Expected %d known hosts, got %d", len(hosts), len(getCluster.KnownHosts))
		}

		Print("Block connectivity to host: " + clusterHost)
		if err := RunIptablesCommand(t, clusterHost, CmdBlockScyllaREST); err != nil {
			t.Fatal(err)
		}
		defer RunIptablesCommand(t, clusterHost, CmdUnblockScyllaREST)

		Print("Expect connectivity failure when adding new cluster")
		putCluster := *validCluster(t)
		putCluster.Host = clusterHost
		// Simulate missing known hosts and expect that
		// they won't overwrite existing known hosts.
		putCluster.KnownHosts = nil
		if err := s.PutCluster(ctx, &putCluster); err == nil {
			t.Fatal("Expected connectivity failure when adding new cluster, got nil")
		}

		Print("Known hosts are set after failed cluster update")
		getCluster, err = s.GetClusterByID(t.Context(), initialCluster.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(getCluster.KnownHosts) != len(hosts) {
			t.Fatalf("Expected %d known hosts, got %d", len(hosts), len(getCluster.KnownHosts))
		}

		newClusterHost := hosts[1]
		Print("Expect connectivity failure when updating existing cluster host param: " + newClusterHost)
		putCluster = initialCluster
		putCluster.Host = newClusterHost
		putCluster.KnownHosts = nil
		if err := s.PutCluster(ctx, &putCluster); err == nil {
			t.Fatal("Expected connectivity failure when updating existing cluster host param, got nil")
		}

		Print("Expect success when updating existing cluster labels param")
		putCluster = initialCluster
		putCluster.Labels = map[string]string{"foo": "bar"}
		putCluster.KnownHosts = nil
		if err := s.PutCluster(ctx, &putCluster); err != nil {
			t.Fatal(err)
		}

		Print("Known hosts are set after successful cluster update")
		getCluster, err = s.GetClusterByID(t.Context(), initialCluster.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(getCluster.KnownHosts) != len(hosts) {
			t.Fatalf("Expected %d known hosts, got %d", len(hosts), len(getCluster.KnownHosts))
		}
	})

	t.Run("list nodes", func(t *testing.T) {
		setup(t)

		c := &cluster.Cluster{
			ID:        uuid.NewTime(),
			Name:      "clust1",
			Host:      ManagedClusterHost(),
			AuthToken: AgentAuthToken(),
		}
		secureManagedCluster(t, c)
		if err := s.PutCluster(ctx, c); err != nil {
			t.Fatal(err)
		}

		got, err := s.ListNodes(ctx, c.ID)
		if err != nil {
			t.Fatal(err)
		}

		for id := range got {
			got[id].Address = ToCanonicalIP(got[id].Address)
		}

		expected := []cluster.Node{
			{
				Datacenter:        "dc1",
				Address:           ToCanonicalIP(IPFromTestNet("11")),
				ShardNum:          2,
				PrometheusAddress: ToCanonicalIP(IPFromSecondTestNet("11")),
				PrometheusPort:    9180,
			},
			{
				Datacenter:        "dc1",
				Address:           ToCanonicalIP(IPFromTestNet("12")),
				ShardNum:          2,
				PrometheusAddress: ToCanonicalIP(IPFromSecondTestNet("12")),
				PrometheusPort:    9180,
			},
			{
				Datacenter:        "dc1",
				Address:           ToCanonicalIP(IPFromTestNet("13")),
				ShardNum:          2,
				PrometheusAddress: ToCanonicalIP(IPFromSecondTestNet("13")),
				PrometheusPort:    9180,
			},
			{
				Datacenter:        "dc2",
				Address:           ToCanonicalIP(IPFromTestNet("21")),
				ShardNum:          2,
				PrometheusAddress: ToCanonicalIP(IPFromSecondTestNet("21")),
				PrometheusPort:    9180,
			},
			{
				Datacenter:        "dc2",
				Address:           ToCanonicalIP(IPFromTestNet("22")),
				ShardNum:          2,
				PrometheusAddress: ToCanonicalIP(IPFromSecondTestNet("22")),
				PrometheusPort:    9180,
			},
			{
				Datacenter:        "dc2",
				Address:           ToCanonicalIP(IPFromTestNet("23")),
				ShardNum:          2,
				PrometheusAddress: ToCanonicalIP(IPFromSecondTestNet("23")),
				PrometheusPort:    9180,
			},
		}

		opts := append(diffOpts, cmpopts.SortSlices(func(x, y cluster.Node) bool {
			if x.Datacenter > y.Datacenter {
				return false
			}
			if x.Address > y.Address {
				return false
			}
			return true
		}))

		if diff := cmp.Diff(expected, got, opts...); diff != "" {
			t.Fatal(diff)
		}
	})
}

func TestConnectionGenerationLWTTwoWritersIntegration(t *testing.T) {
	session := CreateScyllaManagerDBSession(t)
	connectionStore := store.NewTableStore(session, table.SecureConnectionBundle)
	ExecStmt(t, session, "TRUNCATE secure_cluster")
	ExecStmt(t, session, "TRUNCATE secure_connection_bundle")
	newService := func() *cluster.Service {
		svc, err := cluster.NewService(session, metrics.NewClusterMetrics(), scyllaclient.DefaultTimeoutConfig(),
			server.DefaultConfig().ClientCacheTimeout, log.NewDevelopment())
		if err != nil {
			t.Fatal(err)
		}
		return svc
	}
	first, second := newService(), newService()
	id, a := uuid.MustRandom(), uuid.MustRandom()
	base := &secrets.ConnectionBundle{
		ClusterID:       id,
		Generation:      a,
		LifecycleEpoch:  1,
		Host:            "generation-a.internal",
		AuthToken:       "generation-a-token",
		AgentCA:         integrationConnectionCA(t),
		AgentServerName: "agent.internal",
	}
	if err := first.CommitConnectionGenerationForTest(context.Background(), &cluster.Cluster{
		ID: id, Name: "atomic-lwt", Host: base.Host,
	}, base, uuid.Nil, true); err != nil {
		t.Fatal(err)
	}

	b, deleted := uuid.MustRandom(), uuid.MustRandom()
	update := &secrets.ConnectionBundle{
		ClusterID:          id,
		Generation:         b,
		LifecycleEpoch:     1,
		PreviousGeneration: a,
		Host:               "generation-b.internal",
		AuthToken:          "generation-b-token",
		AgentCA:            integrationConnectionCA(t),
		AgentServerName:    "agent.internal",
	}
	tombstone := &secrets.ConnectionBundle{
		ClusterID:          id,
		Generation:         deleted,
		LifecycleEpoch:     2,
		PreviousGeneration: a,
		Deleted:            true,
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		results <- first.CommitConnectionGenerationForTest(context.Background(), &cluster.Cluster{
			ID: id, Name: "atomic-lwt", Host: update.Host,
		}, update, a, false)
	}()
	go func() {
		<-start
		results <- second.CommitConnectionGenerationForTest(context.Background(), &cluster.Cluster{
			ID: id, ConnectionDeleted: true,
		}, tombstone, a, false)
	}()
	close(start)
	var success, conflict int
	for range 2 {
		err := <-results
		switch {
		case err == nil:
			success++
		case errors.Is(err, cluster.ErrConnectionCommitConflict):
			conflict++
		default:
			t.Fatalf("unexpected LWT result: %v", err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("global SERIAL LWT did not choose one winner: success=%d conflict=%d", success, conflict)
	}

	var active uuid.UUID
	if err := table.Cluster.GetQuery(session, "connection_generation").BindMap(map[string]any{"id": id}).GetRelease(&active); err != nil {
		t.Fatal(err)
	}
	selected := secrets.NewConnectionBundle(id)
	selected.Generation = active
	if err := connectionStore.Get(selected); err != nil {
		t.Fatalf("active pointer selected a missing bundle: %v", err)
	}
	if active == b && (selected.Deleted || selected.AuthToken != update.AuthToken || selected.PreviousGeneration != a) {
		t.Fatalf("update winner is not one exact tuple: %#v", selected)
	}
	if active == deleted && (!selected.Deleted || selected.PreviousGeneration != a) {
		t.Fatalf("delete winner is not one exact tombstone: %#v", selected)
	}
}

func TestSecureConnectionReadersObserveOnlyCompleteGenerationsIntegration(t *testing.T) {
	session := CreateScyllaManagerDBSession(t)
	ExecStmt(t, session, "TRUNCATE secure_cluster")
	ExecStmt(t, session, "TRUNCATE secure_connection_bundle")
	newService := func() *cluster.Service {
		svc, err := cluster.NewService(session, metrics.NewClusterMetrics(), scyllaclient.DefaultTimeoutConfig(),
			server.DefaultConfig().ClientCacheTimeout, log.NewDevelopment())
		if err != nil {
			t.Fatal(err)
		}
		return svc
	}
	writer := newService()
	readers := []*cluster.Service{newService(), newService(), newService(), newService()}
	id, a, b := uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom()
	bundleA := &secrets.ConnectionBundle{
		ClusterID: id, Generation: a, LifecycleEpoch: 1,
		Host: "a.internal", KnownHosts: []string{"10.0.0.1"}, Port: 10001,
		AuthToken: "a-token", CQLUsername: "a-user", CQLPassword: "a-password",
		AgentCA: integrationConnectionCA(t), AgentServerName: "agent.internal",
	}
	if err := writer.CommitConnectionGenerationForTest(context.Background(), &cluster.Cluster{ID: id, Name: "atomic"}, bundleA, uuid.Nil, true); err != nil {
		t.Fatal(err)
	}
	bundleB := &secrets.ConnectionBundle{
		ClusterID: id, Generation: b, PreviousGeneration: a, LifecycleEpoch: 1,
		Host: "b.internal", KnownHosts: []string{"10.0.0.2"}, Port: 10002,
		AuthToken: "b-token", CQLUsername: "b-user", CQLPassword: "b-password",
		AgentCA: integrationConnectionCA(t), AgentServerName: "agent.internal",
	}

	start := make(chan struct{})
	stop := make(chan struct{})
	errs := make(chan error, 64)
	var wg sync.WaitGroup
	for _, reader := range readers {
		reader := reader
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				select {
				case <-stop:
					return
				default:
				}
				c, err := reader.GetClusterByID(context.Background(), id)
				if err != nil {
					errs <- err
					return
				}
				switch c.ConnectionGeneration {
				case a:
					if c.Host != bundleA.Host || c.Port != bundleA.Port || c.AuthToken != bundleA.AuthToken ||
						c.Username != bundleA.CQLUsername || c.Password != bundleA.CQLPassword {
						errs <- fmt.Errorf("mixed generation A snapshot: %#v", c)
						return
					}
				case b:
					if c.Host != bundleB.Host || c.Port != bundleB.Port || c.AuthToken != bundleB.AuthToken ||
						c.Username != bundleB.CQLUsername || c.Password != bundleB.CQLPassword {
						errs <- fmt.Errorf("mixed generation B snapshot: %#v", c)
						return
					}
				default:
					errs <- fmt.Errorf("unexpected active generation %s", c.ConnectionGeneration)
					return
				}
			}
		}()
	}
	close(start)
	if err := writer.CommitConnectionGenerationForTest(context.Background(), &cluster.Cluster{ID: id, Name: "atomic"}, bundleB, a, false); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestDeletedSecureClusterIDCannotBeRecreatedIntegration(t *testing.T) {
	session := CreateScyllaManagerDBSession(t)
	secureStore := store.NewTableStore(session, table.SecureConnectionBundle)
	ExecStmt(t, session, "TRUNCATE secure_cluster")
	ExecStmt(t, session, "TRUNCATE secure_connection_bundle")
	svc, err := cluster.NewService(session, metrics.NewClusterMetrics(), scyllaclient.DefaultTimeoutConfig(),
		server.DefaultConfig().ClientCacheTimeout, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}
	id, a, d, c := uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom()
	active := &secrets.ConnectionBundle{
		ClusterID: id, Generation: a, LifecycleEpoch: 1,
		Host: "active.internal", AuthToken: "active-token",
		AgentCA: integrationConnectionCA(t), AgentServerName: "agent.internal",
	}
	if err := svc.CommitConnectionGenerationForTest(context.Background(), &cluster.Cluster{ID: id}, active, uuid.Nil, true); err != nil {
		t.Fatal(err)
	}
	tombstone := &secrets.ConnectionBundle{ClusterID: id, Generation: d, PreviousGeneration: a, LifecycleEpoch: 2, Deleted: true}
	if err := svc.CommitConnectionGenerationForTest(context.Background(), &cluster.Cluster{ID: id}, tombstone, a, false); err != nil {
		t.Fatal(err)
	}
	recreate := &secrets.ConnectionBundle{
		ClusterID: id, Generation: c, PreviousGeneration: d, LifecycleEpoch: 2,
		Host: "recreate.internal", AuthToken: "recreate-token",
		AgentCA: integrationConnectionCA(t), AgentServerName: "agent.internal",
	}
	if err := svc.CommitConnectionGenerationForTest(context.Background(), &cluster.Cluster{ID: id}, recreate, uuid.Nil, true); !errors.Is(err, cluster.ErrConnectionCommitConflict) {
		t.Fatalf("tombstoned UUID was recreated: %v", err)
	}
	selected := secrets.NewConnectionBundle(id)
	selected.Generation = d
	if err := secureStore.Get(selected); err != nil || !selected.Deleted {
		t.Fatalf("authoritative tombstone was replaced: bundle=%#v err=%v", selected, err)
	}
}

func integrationConnectionCA(t *testing.T) []byte {
	t.Helper()
	ca, err := os.ReadFile("testdata/cluster.crt")
	if err != nil {
		t.Fatal(err)
	}
	return ca
}

func validCluster(t *testing.T) *cluster.Cluster {
	c := &cluster.Cluster{
		ID:        uuid.MustRandom(),
		Name:      "name_" + uuid.MustRandom().String(),
		Host:      ManagedClusterHost(),
		Port:      10001,
		AuthToken: AgentAuthToken(),
	}
	secureManagedCluster(t, c)
	return c
}

func secureManagedCluster(t *testing.T, c *cluster.Cluster) {
	t.Helper()
	ca, err := os.ReadFile(ManagedClusterCAFile())
	if err != nil {
		t.Fatalf("read managed cluster CA: %v", err)
	}
	serverName := ManagedClusterTLSServerName()
	c.AgentCAFile, c.AgentServerName = ca, serverName
	c.CQLCAFile, c.CQLServerName = ca, serverName
	c.AlternatorCAFile, c.AlternatorServerName = ca, serverName
	if IsSSLEnabled() {
		c.Username, c.Password = ManagedClusterCredentials()
		c.AlternatorAccessKeyID, c.AlternatorSecretAccessKey = managedClusterAlternatorCredentials(t, ca, serverName)
	}
}

var (
	managedClusterAlternatorCredentialsOnce sync.Once
	managedClusterAlternatorAccessKeyID     string
	managedClusterAlternatorSecretAccessKey string
)

func managedClusterAlternatorCredentials(t *testing.T, ca []byte, serverName string) (string, string) {
	t.Helper()
	managedClusterAlternatorCredentialsOnce.Do(func() {
		trust := secrets.NewAgentTLSTrust(uuid.Nil)
		trust.CA, trust.ServerName = ca, serverName
		tlsConfig, err := secrets.TLSConfig(trust)
		if err != nil {
			t.Fatal(err)
		}
		transport := scyllaclient.DefaultTransport()
		transport.TLSClientConfig = tlsConfig
		config := ManagedClusterAgentConfig(t, ManagedClusterHosts(), AgentAuthToken())
		config.Transport = transport
		client, err := scyllaclient.NewClient(config, log.NewDevelopment())
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		session := CreateManagedClusterSession(t, false, client, "", "")
		defer session.Close()
		managedClusterAlternatorAccessKeyID, managedClusterAlternatorSecretAccessKey = GetAlternatorCreds(t, session, "")
	})
	return managedClusterAlternatorAccessKeyID, managedClusterAlternatorSecretAccessKey
}

var (
	tlsCert []byte
	tlsKey  []byte
)

func init() {
	var err error
	tlsCert, err = os.ReadFile("testdata/cluster.crt")
	if err != nil {
		panic(err)
	}
	tlsKey, err = os.ReadFile("testdata/cluster.key")
	if err != nil {
		panic(err)
	}
}

func tlsCluster(t *testing.T) *cluster.Cluster {
	c := validCluster(t)
	if c.Username == "" {
		c.Username, c.Password = "user", "password"
	}
	if c.AlternatorAccessKeyID == "" {
		c.AlternatorAccessKeyID, c.AlternatorSecretAccessKey = "id", "key"
	}
	c.SSLUserCertFile = tlsCert
	c.SSLUserKeyFile = tlsKey
	return c
}
