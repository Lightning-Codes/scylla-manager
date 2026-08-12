// Copyright (C) 2017 ScyllaDB

package cluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/metrics"
	"github.com/scylladb/scylla-manager/v3/pkg/ping/cqlping"
	"github.com/scylladb/scylla-manager/v3/pkg/ping/dynamoping"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/secrets"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
	"github.com/scylladb/scylla-manager/v3/pkg/util/logutil"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/multierr"
)

// ProviderFunc defines the function that will be used by other services to get current cluster data.
type ProviderFunc func(ctx context.Context, id uuid.UUID) (*Cluster, error)

// ChangeType specifies type on Change.
type ChangeType int8

// ErrNoValidKnownHost is thrown when it was not possible to connect to any of the currently known hosts of the cluster.
var (
	ErrNoValidKnownHost      = errors.New("unable to connect to any of cluster's known hosts")
	ErrNoLiveHostAvailable   = errors.New("no single live host available")
	ErrSecureConnectivity    = errors.New("secure cluster connectivity validation failed")
	ErrDataPlaneConnectivity = errors.New("secure data-plane connectivity validation failed")
)

// ChangeType enumeration.
const (
	Create ChangeType = iota
	Update
	Delete
)

// Change specifies cluster modification.
type Change struct {
	ID            uuid.UUID
	Type          ChangeType
	WithoutRepair bool
}

// Servicer interface defines the responsibilities of the cluster service.
// It's a duplicate of the restapi.ClusterService, but I want to avoid doing bigger refactor
// and removing the interface from restapi package (although nothing prevents us from doing so).
type Servicer interface {
	ListClusters(ctx context.Context, f *Filter) ([]*Cluster, error)
	GetCluster(ctx context.Context, idOrName string) (*Cluster, error)
	PutCluster(ctx context.Context, c *Cluster) error
	DeleteCluster(ctx context.Context, id uuid.UUID) error
	CheckCQLCredentials(id uuid.UUID) (bool, error)
	DeleteCQLCredentials(ctx context.Context, id uuid.UUID) error
	CheckAlternatorCredentials(id uuid.UUID) (bool, error)
	DeleteAlternatorCredentials(ctx context.Context, id uuid.UUID) error
	CheckSSLUserCert(id uuid.UUID) (bool, error)
	DeleteSSLUserCert(ctx context.Context, id uuid.UUID) error
	CheckTLSTrust(id uuid.UUID, protocol string) (bool, error)
	DeleteTLSTrust(ctx context.Context, id uuid.UUID, protocol string) error
	ListNodes(ctx context.Context, id uuid.UUID) ([]Node, error)
}

// Service manages cluster configurations.
type Service struct {
	// mutationMu reduces same-process contention. Correctness across Manager
	// replicas comes from immutable staging and the global-serial pointer LWT.
	mutationMu               sync.Mutex
	session                  gocqlx.Session
	metrics                  metrics.ClusterMetrics
	connectionBundleStore    store.Store
	clientCache              *scyllaclient.CachedProvider
	timeoutConfig            scyllaclient.TimeoutConfig
	logger                   log.Logger
	onChangeListener         func(ctx context.Context, c Change) error
	onConnectionInvalidation func(clusterID uuid.UUID)
	stageConnectionBundle    func(context.Context, *secrets.ConnectionBundle) error
	swapConnectionGeneration func(context.Context, *Cluster, uuid.UUID, bool) error
	postCommitSession        func(context.Context, uuid.UUID) (gocqlx.Session, error)
	cqlQueryPing             func(context.Context, cqlping.Config, string, string) (time.Duration, error)
	cqlNativePing            func(context.Context, cqlping.Config) (time.Duration, error)
	alternatorQueryPing      func(context.Context, dynamoping.Config) (time.Duration, error)
}

func NewService(session gocqlx.Session, metrics metrics.ClusterMetrics, timeoutConfig scyllaclient.TimeoutConfig,
	cacheInvalidationTimeout time.Duration, l log.Logger,
) (*Service, error) {
	if session.Session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}

	s := &Service{
		session:             session,
		metrics:             metrics,
		logger:              l,
		timeoutConfig:       timeoutConfig,
		cqlQueryPing:        cqlping.QueryPing,
		alternatorQueryPing: dynamoping.QueryPing,
	}
	s.clientCache = scyllaclient.NewCachedProvider(s.CreateClientNoCache, cacheInvalidationTimeout, l)
	s.stageConnectionBundle = s.stageImmutableConnectionBundle
	s.swapConnectionGeneration = s.compareAndSwapConnectionGeneration
	s.cqlNativePing = func(ctx context.Context, cfg cqlping.Config) (time.Duration, error) {
		return cqlping.NativeCQLPing(ctx, cfg, s.logger)
	}

	return s, nil
}

// Init initializes metrics from database.
func (s *Service) Init(ctx context.Context) error {
	s.logger.Debug(ctx, "Init")
	if err := s.requireGreenfieldLegacyTablesEmpty(ctx); err != nil {
		return err
	}

	var secureClusters []*Cluster
	if err := s.session.Query(table.Cluster.SelectAll()).SelectRelease(&secureClusters); err != nil {
		return err
	}
	byID := make(map[uuid.UUID]*Cluster, len(secureClusters))
	for _, scanned := range secureClusters {
		// SERIAL reads are single-partition only. The range scan discovers IDs;
		// this point read authoritatively resolves the current pointer/epoch.
		var c Cluster
		if err := table.Cluster.GetQueryContext(ctx, s.session).Consistency(gocql.Serial).
			BindMap(qb.M{"id": scanned.ID}).GetRelease(&c); err != nil {
			if errors.Is(err, util.ErrNotFound) {
				continue
			}
			return errors.Wrapf(err, "resolve secure cluster %s", scanned.ID)
		}
		if err := s.hydrateClusterConnection(&c); err != nil {
			return errors.Wrapf(err, "hydrate secure connection generation for cluster %s", c.ID)
		}
		byID[c.ID] = &c
	}

	for _, c := range byID {
		if c.ConnectionDeleted {
			if s.clientCache != nil {
				s.clientCache.RevokeClusterEpoch(c.ID, c.ConnectionGeneration, c.LifecycleEpoch)
			}
			continue
		}
		if s.clientCache != nil {
			s.clientCache.ResetClusterEpoch(c.ID, c.ConnectionGeneration, c.LifecycleEpoch)
		}
		s.metrics.SetName(c.ID, c.Name)
	}
	return nil
}

func (s *Service) requireGreenfieldLegacyTablesEmpty(ctx context.Context) error {
	checks := []struct {
		query string
		name  string
	}{
		{query: "SELECT id FROM cluster LIMIT 1", name: "legacy cluster"},
		{query: "SELECT cluster_id FROM secrets LIMIT 1", name: "legacy generic-secrets"},
	}
	for _, check := range checks {
		var id uuid.UUID
		err := s.session.ContextQuery(ctx, check.query, nil).Consistency(gocql.All).GetRelease(&id)
		switch {
		case err == nil:
			return errors.Errorf("%s data exists; this secure Manager build supports only a fresh metadata store", check.name)
		case errors.Is(err, util.ErrNotFound):
			continue
		default:
			return errors.Wrapf(err, "verify %s table is empty", check.name)
		}
	}
	return nil
}

// SetOnChangeListener sets a function that would be invoked when a cluster
// changes.
func (s *Service) SetOnChangeListener(f func(ctx context.Context, c Change) error) {
	s.onChangeListener = f
}

// SetOnConnectionInvalidationListener installs the synchronous fail-closed
// cache hook run immediately before a connection generation is switched.
func (s *Service) SetOnConnectionInvalidationListener(f func(clusterID uuid.UUID)) {
	s.onConnectionInvalidation = f
}

// Client is cluster client provider.
func (s *Service) Client(ctx context.Context, clusterID uuid.UUID) (*scyllaclient.Client, error) {
	s.logger.Debug(ctx, "Client", "cluster_id", clusterID)
	c, err := s.GetClusterByID(ctx, clusterID)
	if err != nil {
		return nil, err
	}
	client, err := s.clientCache.ClientForGenerationValidatedEpoch(ctx, clusterID, c.ConnectionGeneration, c.LifecycleEpoch,
		func() (*scyllaclient.Client, error) {
			return s.createClientFromCluster(c)
		},
		func() error {
			return s.validateActiveConnectionGeneration(ctx, clusterID, c.ConnectionGeneration, c.LifecycleEpoch)
		},
	)
	if !errors.Is(err, scyllaclient.ErrCachedHostsChanged) {
		return client, err
	}
	// Host topology is part of the immutable connection snapshot. Refresh it
	// outside the provider cell lock, commit a new generation, then retry the
	// normal cache acquisition. The temporary discovery client is never handed
	// to a cache-owned caller.
	discoveryClient, refreshErr := s.CreateClientNoCache(ctx, clusterID)
	if discoveryClient != nil {
		_ = discoveryClient.Close()
	}
	if refreshErr != nil {
		return nil, refreshErr
	}
	// Discovery can legitimately confirm that the stored sorted live-host set
	// is unchanged even though CheckHostsChanged observed a transient topology
	// difference. Reset the reusable A cell before retrying to avoid recursion.
	s.clientCache.RefreshGeneration(clusterID, c.ConnectionGeneration)
	return s.Client(ctx, clusterID)
}

func (s *Service) validateActiveConnectionGeneration(ctx context.Context, clusterID, generation uuid.UUID, lifecycleEpoch int64) error {
	var active connectionPointer
	err := table.Cluster.GetQueryContext(ctx, s.session, "connection_generation", "connection_deleted", "lifecycle_epoch").BindMap(qb.M{
		"id": clusterID,
	}).Consistency(gocql.Serial).GetRelease(&active)
	if err != nil {
		return errors.Wrap(err, "validate active connection generation")
	}
	if active.Generation != generation || active.Epoch != lifecycleEpoch || active.Deleted {
		return errors.Wrapf(ErrConnectionCommitConflict, "expected active generation %s at lifecycle epoch %d, got generation %s epoch %d deleted=%v", generation, lifecycleEpoch, active.Generation, active.Epoch, active.Deleted)
	}
	return nil
}

// CreateClientNoCache creates Scylla API that load balances calls to every node from given cluster.
// There may be a situation that cluster keeps outdated information about list of available hosts.
// To work it around:
//   - function iterates over all currently known hosts
//   - calls consecutive client to get list of available hosts known by Scylla server
//   - updates list of known hosts to Scylla Manager DB
//   - returns client created on top of list of hosts returned by the Scylla server
func (s *Service) CreateClientNoCache(ctx context.Context, clusterID uuid.UUID) (*scyllaclient.Client, error) {
	s.logger.Info(ctx, "Creating new Scylla HTTP client", "cluster_id", clusterID)

	c, err := s.GetClusterByID(ctx, clusterID)
	if err != nil {
		return nil, err
	}

	client, err := s.createClientFromCluster(c)
	if err != nil {
		return nil, err
	}
	liveHosts, err := client.GossiperEndpointLiveGet(ctx)
	if err != nil {
		client.Close()
		return nil, errors.Wrap(err, "discover live Agent hosts")
	}
	knownHosts, err := s.discoverHosts(ctx, client, liveHosts)
	if err != nil {
		client.Close()
		return nil, errors.Wrap(err, "discover Agent hosts")
	}
	if slices.Equal(knownHosts, c.KnownHosts) {
		return client, nil
	}
	client.Close()
	if err := s.rotateKnownHosts(ctx, c, knownHosts); err != nil && !errors.Is(err, ErrConnectionCommitConflict) {
		return nil, errors.Wrap(err, "commit discovered hosts connection generation")
	}
	current, err := s.GetClusterByID(ctx, clusterID)
	if err != nil {
		return nil, err
	}
	return s.createClientFromCluster(current)
}

func (s *Service) createClientFromCluster(c *Cluster) (*scyllaclient.Client, error) {
	config, err := s.clientConfig(c)
	if err != nil {
		return nil, err
	}
	return scyllaclient.NewClient(config, s.logger.Named("client"))
}

func (s *Service) rotateKnownHosts(ctx context.Context, current *Cluster, hosts []string) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	// Refetch after taking the process-local contention lock. The LWT below is
	// still the correctness boundary across Manager replicas.
	active, err := s.GetClusterByID(ctx, current.ID)
	if err != nil {
		return err
	}
	if active.ConnectionGeneration != current.ConnectionGeneration {
		return ErrConnectionCommitConflict
	}
	candidate := *active
	candidate.expectedLifecycleEpoch = &active.LifecycleEpoch
	candidate.KnownHosts = slices.Clone(hosts)
	generation, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	bundle := connectionBundleFromCluster(&candidate, generation)
	bundle.PreviousGeneration = active.ConnectionGeneration
	if err := s.commitConnectionGeneration(ctx, &candidate, bundle, active.ConnectionGeneration, false); err != nil {
		return err
	}
	if s.clientCache != nil {
		s.clientCache.ResetClusterEpoch(candidate.ID, generation, bundle.LifecycleEpoch)
		s.clientCache.InvalidateGeneration(candidate.ID, active.ConnectionGeneration)
	}
	s.invalidateConnectionConfigCache(candidate.ID)
	if err := s.notifyCommittedChange(ctx, generation, Change{ID: candidate.ID, Type: Update}); err != nil {
		s.logger.Error(ctx, "KnownHosts generation committed but post-commit refresh failed",
			"cluster_id", candidate.ID,
			"generation", generation,
			"error", err,
		)
	}
	return nil
}

// CreateClientForClusterSnapshot constructs an Agent client from exactly the
// supplied immutable connection snapshot without refetching the active row.
func (s *Service) CreateClientForClusterSnapshot(c *Cluster) (*scyllaclient.Client, error) {
	return s.createClientFromCluster(c)
}

func (s *Service) clientConfig(c *Cluster) (scyllaclient.Config, error) {
	if c.ConnectionDeleted {
		return scyllaclient.Config{}, util.ErrNotFound
	}
	config := scyllaclient.DefaultConfigWithTimeout(s.timeoutConfig)
	config.ConnectionGeneration = c.ConnectionGeneration
	config.ConnectionLifecycleEpoch = c.LifecycleEpoch
	if c.Port != 0 {
		config.Port = strconv.Itoa(c.Port)
	}
	config.AuthToken = c.AuthToken
	config.Hosts = c.KnownHosts
	transport := scyllaclient.DefaultTransport()
	tlsConfig, err := s.clusterTLSConfig(c, secrets.AgentProtocol)
	if err != nil {
		return scyllaclient.Config{}, errors.Wrap(err, "strict Agent TLS trust is unavailable")
	}
	transport.TLSClientConfig = tlsConfig
	config.Transport = transport
	return config, nil
}

func (s *Service) clusterTLSConfig(c *Cluster, protocol string) (*tls.Config, error) {
	var trust *secrets.TLSTrust
	switch protocol {
	case secrets.CQLProtocol:
		trust = secrets.NewCQLTLSTrust(c.ID)
		trust.CA = c.CQLCAFile
		trust.ServerName = c.CQLServerName
	case secrets.AlternatorProtocol:
		trust = secrets.NewAlternatorTLSTrust(c.ID)
		trust.CA = c.AlternatorCAFile
		trust.ServerName = c.AlternatorServerName
	case secrets.AgentProtocol:
		trust = secrets.NewAgentTLSTrust(c.ID)
		trust.CA = c.AgentCAFile
		trust.ServerName = c.AgentServerName
	default:
		return nil, errors.Errorf("unknown TLS protocol %q", protocol)
	}
	if len(trust.CA) != 0 || trust.ServerName != "" {
		return secrets.TLSConfig(trust)
	}
	return nil, util.ErrNotFound
}

const (
	discoverClusterHostsTimeout = 5 * time.Second
)

func (s *Service) discoverClusterHosts(ctx context.Context, c *Cluster) (knownHosts, liveHosts []string, err error) {
	if c.Host != "" {
		knownHosts, liveHosts, err := s.discoverClusterHostUsingCoordinator(ctx, c, discoverClusterHostsTimeout, c.Host)
		if err != nil {
			s.logger.Error(ctx, "Couldn't discover hosts using stored coordinator host, proceeding with other known ones",
				"coordinator-host", c.Host, "error", err)
		} else {
			return knownHosts, liveHosts, nil
		}
	} else {
		s.logger.Error(ctx, "Missing --host flag. Using only previously discovered hosts instead", "cluster ID", c.ID)
	}
	if len(c.KnownHosts) < 1 {
		return nil, nil, ErrNoValidKnownHost
	}

	wg := sync.WaitGroup{}
	type hostsTuple struct {
		live, known []string
	}
	result := make(chan hostsTuple, len(c.KnownHosts))
	discoverContext, discoverCancel := context.WithCancel(ctx)
	defer discoverCancel()

	for _, cp := range c.KnownHosts {
		wg.Add(1)

		go func(host string) {
			defer wg.Done()

			knownHosts, liveHosts, err := s.discoverClusterHostUsingCoordinator(discoverContext, c, discoverClusterHostsTimeout, host)
			if err != nil {
				// Only log if the context hasn't been canceled
				if !errors.Is(discoverContext.Err(), context.Canceled) {
					s.logger.Error(ctx, "Couldn't discover hosts", "host", host, "error", err)
				}
				return
			}
			result <- hostsTuple{
				live:  liveHosts,
				known: knownHosts,
			}
		}(cp)
	}

	go func() {
		wg.Wait()
		close(result)
	}()

	// Read results until the channel is closed
	hosts, ok := <-result
	if ok {
		return hosts.known, hosts.live, nil
	}

	// If no valid results, return error
	return nil, nil, ErrNoValidKnownHost
}

func (s *Service) discoverClusterHostUsingCoordinator(ctx context.Context, c *Cluster, apiCallTimeout time.Duration,
	host string,
) (knownHosts, liveHosts []string, err error) {
	config, err := s.clientConfig(c)
	if err != nil {
		return nil, nil, err
	}
	config.Timeout = apiCallTimeout
	config.Hosts = []string{host}

	client, err := scyllaclient.NewClient(config, s.logger.Named("client"))
	if err != nil {
		return nil, nil, err
	}
	defer logutil.LogOnError(ctx, s.logger, client.Close, "Couldn't close scylla client")

	liveHosts, err = client.GossiperEndpointLiveGet(ctx)
	if err != nil {
		return nil, nil, err
	}
	knownHosts, err = s.discoverHosts(ctx, client, liveHosts)
	if err != nil {
		return nil, nil, err
	}
	return knownHosts, liveHosts, nil
}

// discoverHosts returns a list of all hosts sorted by DC speed. This is
// an optimisation for Epsilon-Greedy host pool used internally by
// scyllaclient.Client that makes it use supposedly faster hosts first.
func (s *Service) discoverHosts(ctx context.Context, client *scyllaclient.Client, liveHosts []string) (hosts []string, err error) {
	if len(liveHosts) == 0 {
		return nil, ErrNoLiveHostAvailable
	}

	dcs, err := client.Datacenters(ctx)
	if err != nil {
		return nil, err
	}
	// remove dead nodes from the map
	liveSet := make(map[string]struct{})
	for _, host := range liveHosts {
		liveSet[host] = struct{}{}
	}
	filteredDCs := make(map[string][]string)
	for dc, hosts := range dcs {
		for _, host := range hosts {
			if _, isLive := liveSet[host]; isLive {
				filteredDCs[dc] = append(filteredDCs[dc], host)
			}
		}
	}

	closest, err := client.ClosestDC(ctx, filteredDCs)
	if err != nil {
		return nil, err
	}
	for _, dc := range closest {
		hosts = append(hosts, dcs[dc]...)
	}
	return hosts, nil
}

// ListClusters returns all the clusters for a given filtering criteria.
func (s *Service) ListClusters(ctx context.Context, f *Filter) ([]*Cluster, error) {
	s.logger.Debug(ctx, "ListClusters", "filter", f)

	// Validate the filter
	if err := f.Validate(); err != nil {
		return nil, err
	}

	q := qb.Select(table.Cluster.Name()).Query(s.session)
	defer q.Release()

	var clusters []*Cluster
	if err := q.Select(&clusters); err != nil {
		return nil, err
	}
	authoritative := clusters[:0]
	for _, scanned := range clusters {
		// A range scan is only an ID discovery hint. Resolve every pointer with a
		// point SERIAL read before returning metadata or letting configcache create
		// a credential-bearing client from it.
		var c Cluster
		err := table.Cluster.GetQueryContext(ctx, s.session).Consistency(gocql.Serial).
			BindMap(qb.M{"id": scanned.ID}).GetRelease(&c)
		if errors.Is(err, util.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, errors.Wrapf(err, "resolve secure cluster %s", scanned.ID)
		}
		if err := s.hydrateClusterConnection(&c); err != nil {
			return nil, errors.Wrapf(err, "load connection generation for cluster %s", c.ID)
		}
		authoritative = append(authoritative, &c)
	}
	clusters = authoritative
	clusters = slices.DeleteFunc(clusters, func(c *Cluster) bool { return c.ConnectionDeleted })

	sort.Slice(clusters, func(i, j int) bool {
		return bytes.Compare(clusters[i].ID.Bytes(), clusters[j].ID.Bytes()) < 0
	})

	// Nothing to filter
	if f.Name == "" {
		return clusters, nil
	}

	filtered := clusters[:0]
	for _, u := range clusters {
		if u.Name == f.Name {
			filtered = append(filtered, u)
		}
	}

	return filtered, nil
}

// GetCluster returns cluster based on ID or name. If nothing was found
// scylla-manager.ErrNotFound is returned.
func (s *Service) GetCluster(ctx context.Context, idOrName string) (*Cluster, error) {
	if id, err := uuid.Parse(idOrName); err == nil {
		return s.GetClusterByID(ctx, id)
	}

	return s.GetClusterByName(ctx, idOrName)
}

// GetClusterByID returns cluster based on ID. If nothing was found
// scylla-manager.ErrNotFound is returned.
func (s *Service) GetClusterByID(ctx context.Context, id uuid.UUID) (*Cluster, error) {
	return s.getClusterByID(ctx, id, false)
}

func (s *Service) getClusterByID(ctx context.Context, id uuid.UUID, includeDeleted bool) (*Cluster, error) {
	s.logger.Debug(ctx, "GetClusterByID", "id", id)

	q := table.Cluster.GetQuery(s.session).BindMap(qb.M{
		"id": id,
	}).Consistency(gocql.Serial)
	defer q.Release()

	if q.Err() != nil {
		return nil, q.Err()
	}

	var c Cluster
	if err := q.Get(&c); err != nil {
		return nil, err
	}
	if err := s.hydrateClusterConnection(&c); err != nil {
		return nil, errors.Wrap(err, "load atomic connection bundle")
	}
	if c.ConnectionDeleted && !includeDeleted {
		return nil, util.ErrNotFound
	}
	if err := requireExpectedConnectionGeneration(ctx, &c); err != nil {
		return nil, err
	}

	return &c, nil
}

// GetClusterByName returns cluster based on name. If nothing was found
// scylla-manager.ErrNotFound is returned.
func (s *Service) GetClusterByName(ctx context.Context, name string) (*Cluster, error) {
	s.logger.Debug(ctx, "GetClusterByName", "name", name)

	clusters, err := s.ListClusters(ctx, &Filter{Name: name})
	if err != nil {
		return nil, err
	}

	switch len(clusters) {
	case 0:
		return nil, util.ErrNotFound
	case 1:
		return clusters[0], nil
	default:
		return nil, errors.Errorf("multiple clusters share the same name %q", name)
	}
}

// NameFunc returns name for a given ID.
type NameFunc func(ctx context.Context, clusterID uuid.UUID) (string, error)

// GetClusterName returns cluster name for a given ID. If nothing was found
// scylla-manager.ErrNotFound is returned.
func (s *Service) GetClusterName(ctx context.Context, id uuid.UUID) (string, error) {
	s.logger.Debug(ctx, "GetClusterName", "id", id)

	c, err := s.GetClusterByID(ctx, id)
	if err != nil {
		return "", err
	}

	return c.String(), nil
}

// PutCluster upserts a cluster, cluster instance must pass Validate() checks.
// If u.ID == uuid.Nil a new one is generated.
func (s *Service) PutCluster(ctx context.Context, c *Cluster) (err error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	return s.putClusterLocked(ctx, c)
}

func (s *Service) putClusterLocked(ctx context.Context, c *Cluster) (err error) {

	if c == nil {
		return util.ErrNilPtr
	}
	s.logger.Debug(ctx, "PutCluster", "cluster_id", c.ID, "cluster_name", c.Name)

	t, err := s.choosePutClusterChangeType(ctx, c)
	if err != nil {
		return errors.Wrap(err, "check for cluster creation or update")
	}

	var old *Cluster
	var activeBundle *secrets.ConnectionBundle
	if t == Create {
		s.logger.Info(ctx, "Adding new cluster", "cluster_id", c.ID)
		c.LifecycleEpoch = 1
	} else {
		s.logger.Info(ctx, "Updating cluster", "cluster_id", c.ID)
		old, err = s.getClusterByID(ctx, c.ID, true)
		if err != nil {
			return err
		}
		if err := c.requireExpectedLifecycleEpoch(ctx, old.LifecycleEpoch); err != nil {
			return err
		}
		if old.ConnectionDeleted {
			return util.ErrValidate(errors.New("deleted secure cluster IDs are permanently retired; enroll the cluster with a new ID"))
		}
		// Every update LWT includes this exact lifecycle precondition. It is
		// assigned only after the request-admission epoch has been checked.
		c.expectedLifecycleEpoch = &old.LifecycleEpoch
		c.LifecycleEpoch = old.LifecycleEpoch
		activeBundle = connectionBundleFromCluster(old, old.ConnectionGeneration)
		mergeClusterConnection(c, activeBundle)
		c.ConnectionDeleted = false
		// Putting cluster should theoretically just set cluster state to the provided one.
		// The problem is that Cluster.KnownHosts are not part of REST API definitions,
		// so it's possible that cluster passed to PutCluster doesn't have it set by accident.
		// This shouldn't be the case, but since it never makes sense for Cluster.KnownHosts
		// to be empty (they should at least contain resolved Cluster.Host), we can add additional
		// safety net here and load them if they are missing.
		if len(c.KnownHosts) == 0 {
			c.KnownHosts = append([]string(nil), old.KnownHosts...)
		}
	}

	// Validate cluster model
	if err := c.Validate(); err != nil {
		return err
	}
	if t == Create && (len(c.AgentCAFile) == 0 || c.AgentServerName == "") {
		return util.ErrValidate(errors.New("Agent CA file and server name are required when creating a cluster"))
	}
	if c.ForceTLSDisabled {
		if len(c.CQLCAFile) != 0 || c.CQLServerName != "" {
			return util.ErrValidate(errors.New("force TLS disabled conflicts with stored CQL TLS trust"))
		}
	}

	// Check for conflicting cluster names
	if err := s.checkClusterNameConflict(ctx, c); err != nil {
		return errors.Wrap(err, "check for cluster name conflict")
	}

	// Check if host connectivity should be checked
	shouldValidateHostsConnectivity := true
	if t == Update {
		shouldValidateHostsConnectivity = shouldValidateHostsConnectivityOnUpdate(c, old)
	}

	if shouldValidateHostsConnectivity {
		if err := s.validateHostsConnectivity(ctx, c, t == Create); err != nil {
			var tip string
			if !errors.Is(err, ErrDataPlaneConnectivity) {
				switch scyllaclient.StatusCodeOf(err) {
				case 0:
					tip = "make sure the IP is correct and access to port 10001 is unblocked"
				case 401:
					tip = "make sure auth_token config option on nodes is set correctly"
				}
			}
			if tip != "" {
				err = fmt.Errorf("%w - %s", err, tip)
			}
			return err
		}
	}
	generation, err := uuid.NewRandom()
	if err != nil {
		return errors.Wrap(err, "generate connection generation")
	}
	expected := uuid.Nil
	if old != nil {
		expected = old.ConnectionGeneration
	}
	bundle := connectionBundleFromCluster(c, generation)
	bundle.PreviousGeneration = expected
	if err := bundle.Validate(); err != nil {
		return util.ErrValidate(err)
	}
	if err := s.commitConnectionGeneration(ctx, c, bundle, expected, t == Create); err != nil {
		return err
	}
	if s.clientCache != nil {
		s.clientCache.ResetClusterEpoch(c.ID, generation, bundle.LifecycleEpoch)
		if t != Create {
			s.clientCache.InvalidateGeneration(c.ID, expected)
		}
	}
	// The LWT is the activation point. New Agent acquisitions are keyed by the
	// new generation, while a reader that captured the immutable old generation
	// may finish. Remove only the ID-scoped NodeConfig cache now; it will be
	// rebuilt from the newly active snapshot by the listener below.
	s.invalidateConnectionConfigCache(c.ID)

	changeEvent := Change{
		ID:            c.ID,
		Type:          t,
		WithoutRepair: c.WithoutRepair,
	}
	listenerErr, sessionErr := s.reconcileCommittedConnection(ctx, generation, changeEvent)
	if listenerErr != nil {
		// The LWT above is the commit point and cannot be rolled back. Cache
		// refresh failure is fail-closed and operationally visible, but reporting
		// the PUT as uncommitted would invite an unsafe blind retry.
		s.logger.Error(ctx, "Connection generation committed but post-commit refresh failed",
			"cluster_id", c.ID,
			"generation", generation,
			"error", listenerErr,
		)
	}

	if c.AuthToken == "" {
		s.logger.Info(ctx, "WARNING! Scylla data is exposed on hosts, "+
			"protect it by specifying auth_token in Scylla Manager Agent config file on Scylla nodes",
			"cluster_id", c.ID,
			"hosts", c.KnownHosts,
		)
	}

	if sessionErr != nil {
		s.logger.Info(ctx, "WARNING! Cannot create CQL session to the cluster. It will affect backup/restore/healthcheck services.",
			"cluster_id", c.ID)
	}

	switch t {
	case Create:
		s.logger.Info(ctx, "Cluster added", "cluster_id", c.ID)
	case Update:
		s.logger.Info(ctx, "Cluster updated", "cluster_id", c.ID)
	}

	s.metrics.SetName(c.ID, c.Name)
	return nil
}

// choosePutClusterChangeType distinguishes between cluster creation and update.
// Additionally, if cluster is created without predesignated ID, it generates a new random UUID for it.
func (s *Service) choosePutClusterChangeType(ctx context.Context, c *Cluster) (ChangeType, error) {
	var zero ChangeType

	// Handle cluster without predesignated ID
	if c.ID == uuid.Nil {
		var err error
		if c.ID, err = uuid.NewRandom(); err != nil {
			return zero, errors.Wrap(err, "couldn't generate random UUID for Cluster")
		}
		return Create, nil
	}

	// Handle cluster with predesignated ID
	var existingID uuid.UUID
	err := table.Cluster.GetQueryContext(ctx, s.session, "id").Consistency(gocql.Serial).
		BindMap(qb.M{"id": c.ID}).GetRelease(&existingID)
	switch {
	case err == nil:
		return Update, nil
	case errors.Is(err, util.ErrNotFound):
		return Create, nil
	default:
		return zero, err
	}
}

// checkClusterNameConflict checks if the cluster that is supposed to be put
// has a name conflict with already existing cluster.
func (s *Service) checkClusterNameConflict(ctx context.Context, c *Cluster) error {
	if c.Name == "" {
		return nil
	}
	conflict, err := s.GetClusterByName(ctx, c.Name)
	switch {
	case err != nil && !errors.Is(err, util.ErrNotFound):
		return err
	case err == nil && conflict.ID != c.ID:
		return util.ErrValidate(errors.Errorf("name %q is already taken", c.Name))
	default:
		return nil
	}
}

// shouldValidateHostsConnectivityOnUpdate based on changed cluster params.
// It needs to be done when cluster params influencing connectivity over http to sm-agents have been updated.
// It doesn't need to be done when cluster params influencing connectivity over cql to scylla nodes have been updated,
// as cql connectivity is not required for all tasks.
func shouldValidateHostsConnectivityOnUpdate(c, old *Cluster) bool {
	return old.Host != c.Host || old.Port != c.Port || old.AuthToken != c.AuthToken ||
		old.ForceTLSDisabled != c.ForceTLSDisabled ||
		old.ForceNonSSLSessionPort != c.ForceNonSSLSessionPort ||
		len(c.AgentCAFile) != 0 || c.AgentServerName != "" ||
		len(c.CQLCAFile) != 0 || c.CQLServerName != "" ||
		len(c.AlternatorCAFile) != 0 || c.AlternatorServerName != "" ||
		len(c.SSLUserCertFile) != 0 || len(c.SSLUserKeyFile) != 0 ||
		c.Username != "" || c.Password != "" ||
		c.AlternatorAccessKeyID != "" || c.AlternatorSecretAccessKey != ""
}

// ValidateHostsConnectivity validates that scylla manager agent API is available and responding on all live hosts.
// Hosts are discovered using cluster.host + cluster.knownHosts saved to the manager's database.
func (s *Service) ValidateHostsConnectivity(ctx context.Context, c *Cluster) error {
	return s.validateHostsConnectivity(ctx, c, false)
}

func (s *Service) validateHostsConnectivity(ctx context.Context, c *Cluster, requireInlineSecrets bool) error {
	knownHosts, liveHosts, err := s.discoverClusterHosts(ctx, c)
	if err != nil {
		return util.ErrValidate(errors.Wrap(multierr.Append(ErrSecureConnectivity, err), "discover cluster hosts"))
	}
	c.KnownHosts = knownHosts

	if len(liveHosts) == 0 {
		return util.ErrValidate(errors.New("no live nodes"))
	}

	config, err := s.clientConfig(c)
	if err != nil {
		return err
	}
	config.Hosts = liveHosts
	client, err := scyllaclient.NewClient(config, s.logger.Named("client"))
	if err != nil {
		return err
	}
	defer logutil.LogOnError(ctx, s.logger, client.Close, "Couldn't close scylla client")

	var errs error
	for i, err := range client.CheckHostsConnectivity(ctx, liveHosts) {
		errs = multierr.Append(errs, errors.Wrap(err, liveHosts[i]))
	}
	if errs != nil {
		return util.ErrValidate(errors.Wrap(multierr.Append(ErrSecureConnectivity, errs), "secure Agent connectivity check"))
	}

	// NodeInfo is a protected Agent endpoint. Use it to discover the security
	// requirements of every live data-plane endpoint, then prove the pending
	// (or already stored) trust and credentials before committing a cluster
	// create or rotation. This prevents a bad CQL/Alternator CA from being
	// persisted merely because the Agent itself was reachable.
	dataPlaneErrs := make([]error, len(liveHosts))
	var wg sync.WaitGroup
	wg.Add(len(liveHosts))
	for i, host := range liveHosts {
		go func(i int, host string) {
			defer wg.Done()
			ni, err := client.NodeInfo(ctx, host)
			if err == nil {
				err = s.validateNodeDataPlaneConnectivity(ctx, c, host, ni, requireInlineSecrets)
			}
			dataPlaneErrs[i] = errors.Wrap(err, host)
		}(i, host)
	}
	wg.Wait()
	if errs := multierr.Combine(dataPlaneErrs...); errs != nil {
		return util.ErrValidate(errors.Wrap(multierr.Combine(ErrSecureConnectivity, ErrDataPlaneConnectivity, errs), "secure data-plane connectivity check"))
	}
	return nil
}

func (s *Service) validateNodeDataPlaneConnectivity(ctx context.Context, c *Cluster, host string, ni *scyllaclient.NodeInfo, requireInlineSecrets bool) error {
	if ni == nil {
		return errors.New("node info is unavailable")
	}
	if err := s.validateCQLConnectivity(ctx, c, host, ni, requireInlineSecrets); err != nil {
		return errors.Wrap(err, "CQL")
	}
	if err := s.validateAlternatorConnectivity(ctx, c, host, ni, requireInlineSecrets); err != nil {
		return errors.Wrap(err, "Alternator")
	}
	return nil
}

func (s *Service) validateCQLConnectivity(ctx context.Context, c *Cluster, host string, ni *scyllaclient.NodeInfo, requireInlineSecrets bool) error {
	if !ni.ClientEncryptionEnabled {
		if ni.CqlPasswordProtected || ni.ClientEncryptionRequireAuth {
			return errors.New("authentication is required without TLS; refusing an unauthenticated or plaintext CQL connection")
		}
		_, err := s.runCQLNativePing(ctx, cqlping.Config{
			Addr:    ni.CQLAddr(host, c.ForceNonSSLSessionPort),
			Timeout: s.timeoutConfig.Timeout,
		})
		return errors.Wrap(err, "verified plaintext query")
	}
	if c.ForceTLSDisabled {
		return errors.New("TLS is advertised but force TLS disabled is set")
	}
	if requireInlineSecrets && (len(c.CQLCAFile) == 0 || c.CQLServerName == "") {
		return errors.New("TLS is enabled, but CQL CA file and server name were not supplied for cluster creation")
	}

	tlsConfig, err := s.clusterTLSConfig(c, secrets.CQLProtocol)
	if err != nil {
		return errors.Wrap(err, "TLS is enabled, but strict trust is unavailable")
	}
	if ni.ClientEncryptionRequireAuth {
		if requireInlineSecrets && (len(c.SSLUserCertFile) == 0 || len(c.SSLUserKeyFile) == 0) {
			return errors.New("client certificate authentication is enabled, but a client certificate and key were not supplied for cluster creation")
		}
		identity, err := s.tlsIdentityForCluster(c)
		if err != nil {
			return errors.Wrap(err, "load required client identity")
		}
		tlsConfig.Certificates = []tls.Certificate{identity}
	}

	config := cqlping.Config{
		Addr:      ni.CQLAddr(host, c.ForceNonSSLSessionPort),
		Timeout:   s.timeoutConfig.Timeout,
		TLSConfig: tlsConfig,
	}
	var username, password string
	credentialsSet := c.Username != "" || c.Password != ""
	if !requireInlineSecrets || credentialsSet {
		username, password, credentialsSet, err = s.cqlCredentialsForCluster(c)
		if err != nil {
			return errors.Wrap(err, "load credentials")
		}
	}
	if ni.CqlPasswordProtected && !credentialsSet {
		if requireInlineSecrets {
			return errors.Wrap(ErrNoCQLCredentials, "credentials were not supplied for cluster creation")
		}
		return ErrNoCQLCredentials
	}
	if credentialsSet {
		_, err = s.runCQLQueryPing(ctx, config, username, password)
	} else {
		_, err = cqlping.NativeCQLPing(ctx, config, s.logger.With("cluster_id", c.ID, "host", host))
	}
	return errors.Wrap(err, "verified query")
}

func (s *Service) validateAlternatorConnectivity(ctx context.Context, c *Cluster, host string, ni *scyllaclient.NodeInfo, requireInlineSecrets bool) error {
	if !ni.AlternatorEnabled() {
		return nil
	}
	if !ni.AlternatorEncryptionEnabled() {
		if ni.AlternatorEnforceAuthorization {
			return errors.New("authentication is enabled without TLS; refusing to transmit Alternator credentials over plaintext")
		}
		_, err := s.runAlternatorQueryPing(ctx, dynamoping.Config{
			Addr:    ni.AlternatorAddr(host),
			Timeout: s.timeoutConfig.Timeout,
		})
		return errors.Wrap(err, "verified plaintext query")
	}
	if requireInlineSecrets && (len(c.AlternatorCAFile) == 0 || c.AlternatorServerName == "") {
		return errors.New("TLS is enabled, but Alternator CA file and server name were not supplied for cluster creation")
	}

	tlsConfig, err := s.clusterTLSConfig(c, secrets.AlternatorProtocol)
	if err != nil {
		return errors.Wrap(err, "TLS is enabled, but strict trust is unavailable")
	}
	config := dynamoping.Config{
		Addr:                   ni.AlternatorAddr(host),
		RequiresAuthentication: ni.AlternatorEnforceAuthorization,
		Timeout:                s.timeoutConfig.Timeout,
		TLSConfig:              tlsConfig,
	}
	if ni.AlternatorEnforceAuthorization {
		if requireInlineSecrets && (c.AlternatorAccessKeyID == "" || c.AlternatorSecretAccessKey == "") {
			return errors.Wrap(ErrNoAlternatorCredentials, "credentials were not supplied for cluster creation")
		}
		config.Credentials, err = s.alternatorCredentialsForCluster(c)
		if err != nil {
			return errors.Wrap(err, "load required credentials")
		}
	}
	_, err = s.runAlternatorQueryPing(ctx, config)
	return errors.Wrap(err, "verified query")
}

func (s *Service) runCQLQueryPing(ctx context.Context, config cqlping.Config, username, password string) (time.Duration, error) {
	if s.cqlQueryPing != nil {
		return s.cqlQueryPing(ctx, config, username, password)
	}
	return cqlping.QueryPing(ctx, config, username, password)
}

func (s *Service) runCQLNativePing(ctx context.Context, config cqlping.Config) (time.Duration, error) {
	if s.cqlNativePing != nil {
		return s.cqlNativePing(ctx, config)
	}
	return cqlping.NativeCQLPing(ctx, config, s.logger)
}

func (s *Service) runAlternatorQueryPing(ctx context.Context, config dynamoping.Config) (time.Duration, error) {
	if s.alternatorQueryPing != nil {
		return s.alternatorQueryPing(ctx, config)
	}
	return dynamoping.QueryPing(ctx, config)
}

// DeleteCluster removes cluster and it's secrets.
func (s *Service) DeleteCluster(ctx context.Context, clusterID uuid.UUID) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	s.logger.Debug(ctx, "DeleteCluster", "cluster_id", clusterID)

	active, err := s.getClusterByID(ctx, clusterID, true)
	if err != nil {
		return err
	}
	if active.ConnectionDeleted {
		return util.ErrNotFound
	}
	if expected, ok := ExpectedLifecycleEpoch(ctx); ok && expected != active.LifecycleEpoch {
		return errors.Wrapf(ErrConnectionCommitConflict, "expected lifecycle epoch %d, active lifecycle epoch %d", expected, active.LifecycleEpoch)
	}
	generation, err := uuid.NewRandom()
	if err != nil {
		return errors.Wrap(err, "generate deleted lifecycle generation")
	}
	tombstone := &Cluster{
		ID:                   clusterID,
		ConnectionGeneration: generation,
		ConnectionDeleted:    true,
		LifecycleEpoch:       active.LifecycleEpoch + 1,
	}
	tombstone.expectedLifecycleEpoch = &active.LifecycleEpoch
	bundle := deletedConnectionBundle(clusterID, generation, active.ConnectionGeneration, tombstone.LifecycleEpoch)
	if err := s.commitConnectionGeneration(ctx, tombstone, bundle, active.ConnectionGeneration, false); err != nil {
		if errors.Is(err, ErrConnectionCommitIndeterminate) && s.clientCache != nil {
			// The outcome cannot be claimed either way. Revoke locally until an
			// authoritative later PUT/Init reset observes a complete generation.
			s.clientCache.ProvisionallyRevokeCluster(clusterID, generation)
			s.invalidateConnectionConfigCache(clusterID)
		}
		return err
	}
	// The durable tombstone is the delete commit point. Old immutable bundles
	// are retained for audit/recovery. Secure cluster IDs are never reusable.
	if s.clientCache != nil {
		s.clientCache.RevokeClusterEpoch(clusterID, generation, tombstone.LifecycleEpoch)
	}
	s.invalidateConnectionConfigCache(clusterID)
	if err := s.notifyCommittedChange(ctx, generation, Change{ID: clusterID, Type: Delete}); err != nil {
		s.logger.Error(ctx, "Cluster deletion committed but post-commit cleanup failed",
			"cluster_id", clusterID,
			"generation", generation,
			"error", err,
		)
	}
	return nil
}

// CheckCQLCredentials checks if associated CQLCreds exist in secrets store.
func (s *Service) CheckCQLCredentials(id uuid.UUID) (bool, error) {
	c, err := s.GetClusterByID(context.Background(), id)
	return c != nil && c.Username != "" && c.Password != "", err
}

// DeleteCQLCredentials removes the associated CQLCreds from secrets store.
func (s *Service) DeleteCQLCredentials(ctx context.Context, clusterID uuid.UUID) error {
	return s.DeleteConnectionSecrets(ctx, clusterID, SecretDeletion{CQLCredentials: true})
}

// CheckAlternatorCredentials checks if associated AlternatorCreds exist in secrets store.
func (s *Service) CheckAlternatorCredentials(id uuid.UUID) (bool, error) {
	c, err := s.GetClusterByID(context.Background(), id)
	return c != nil && c.AlternatorAccessKeyID != "" && c.AlternatorSecretAccessKey != "", err
}

// DeleteAlternatorCredentials removes the associated AlternatorCreds from secrets store.
func (s *Service) DeleteAlternatorCredentials(ctx context.Context, clusterID uuid.UUID) error {
	return s.DeleteConnectionSecrets(ctx, clusterID, SecretDeletion{AlternatorCredentials: true})
}

// DeleteSSLUserCert removes the associated TLSIdentity from secrets store.
func (s *Service) DeleteSSLUserCert(ctx context.Context, clusterID uuid.UUID) error {
	return s.DeleteConnectionSecrets(ctx, clusterID, SecretDeletion{SSLUserCert: true})
}

// CheckSSLUserCert checks if an associated TLS client identity exists.
func (s *Service) CheckSSLUserCert(id uuid.UUID) (bool, error) {
	c, err := s.GetClusterByID(context.Background(), id)
	return c != nil && len(c.SSLUserCertFile) != 0 && len(c.SSLUserKeyFile) != 0, err
}

// CheckTLSTrust checks if protocol-specific strict TLS trust exists.
func (s *Service) CheckTLSTrust(id uuid.UUID, protocol string) (bool, error) {
	c, err := s.GetClusterByID(context.Background(), id)
	if err != nil {
		return false, err
	}
	switch protocol {
	case secrets.CQLProtocol:
		return len(c.CQLCAFile) != 0 && c.CQLServerName != "", nil
	case secrets.AlternatorProtocol:
		return len(c.AlternatorCAFile) != 0 && c.AlternatorServerName != "", nil
	case secrets.AgentProtocol:
		return len(c.AgentCAFile) != 0 && c.AgentServerName != "", nil
	default:
		return false, util.ErrValidate(errors.Errorf("unsupported TLS protocol %q", protocol))
	}
}

// DeleteTLSTrust removes protocol-specific strict TLS trust.
func (s *Service) DeleteTLSTrust(ctx context.Context, id uuid.UUID, protocol string) error {
	if protocol == secrets.AgentProtocol {
		return util.ErrValidate(errors.New("Agent TLS trust is mandatory; rotate it through PUT or delete the cluster"))
	}
	switch protocol {
	case secrets.CQLProtocol:
		return s.DeleteConnectionSecrets(ctx, id, SecretDeletion{CQLTrust: true})
	case secrets.AlternatorProtocol:
		return s.DeleteConnectionSecrets(ctx, id, SecretDeletion{AlternatorTrust: true})
	default:
		return util.ErrValidate(errors.Errorf("unsupported TLS protocol %q", protocol))
	}
}

// DeleteConnectionSecrets clears all selected values in one preflighted,
// immutable connection generation and one pointer CAS.
func (s *Service) DeleteConnectionSecrets(ctx context.Context, id uuid.UUID, deletion SecretDeletion) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	c, err := s.GetClusterByID(ctx, id)
	if err != nil {
		return err
	}
	if deletion.ExpectedLifecycleEpoch != nil && *deletion.ExpectedLifecycleEpoch != c.LifecycleEpoch {
		return errors.Wrapf(ErrConnectionCommitConflict, "expected lifecycle epoch %d, active lifecycle epoch %d", *deletion.ExpectedLifecycleEpoch, c.LifecycleEpoch)
	}
	if deletion.ExpectedLifecycleEpoch != nil {
		c.expectedLifecycleEpoch = deletion.ExpectedLifecycleEpoch
	} else if expected, ok := ExpectedLifecycleEpoch(ctx); ok {
		c.expectedLifecycleEpoch = &expected
	}
	c.deleteCQLCredentials = deletion.CQLCredentials
	c.deleteAlternatorCredentials = deletion.AlternatorCredentials
	c.deleteSSLUserCert = deletion.SSLUserCert
	c.deleteCQLTrust = deletion.CQLTrust
	c.deleteAlternatorTrust = deletion.AlternatorTrust
	if deletion.CQLCredentials {
		c.Username, c.Password = "", ""
	}
	if deletion.AlternatorCredentials {
		c.AlternatorAccessKeyID, c.AlternatorSecretAccessKey = "", ""
	}
	if deletion.SSLUserCert {
		c.SSLUserCertFile, c.SSLUserKeyFile = nil, nil
	}
	if deletion.CQLTrust {
		c.CQLCAFile, c.CQLServerName = nil, ""
	}
	if deletion.AlternatorTrust {
		c.AlternatorCAFile, c.AlternatorServerName = nil, ""
	}
	// A remote writer can commit B after the first read. Pin the second
	// authoritative read in putClusterLocked to A; otherwise public endpoint and
	// force-policy fields captured in A could be carried across B into C while
	// only the selected secret is cleared.
	ctx = WithExpectedConnectionGeneration(ctx, c.ConnectionGeneration)
	return s.putClusterLocked(ctx, c)
}

func tlsTrustEntry(id uuid.UUID, protocol string) (*secrets.TLSTrust, error) {
	switch protocol {
	case secrets.CQLProtocol:
		return secrets.NewCQLTLSTrust(id), nil
	case secrets.AlternatorProtocol:
		return secrets.NewAlternatorTLSTrust(id), nil
	case secrets.AgentProtocol:
		return secrets.NewAgentTLSTrust(id), nil
	default:
		return nil, util.ErrValidate(errors.Errorf("unsupported TLS protocol %q", protocol))
	}
}

// ListNodes returns information about all the nodes in the cluster.
// Address will be set as node name if it's not resolvable.
func (s *Service) ListNodes(ctx context.Context, clusterID uuid.UUID) ([]Node, error) {
	s.logger.Debug(ctx, "ListNodes", "cluster_id", clusterID)

	var nodes []Node

	client, err := s.CreateClientNoCache(ctx, clusterID)
	if err != nil {
		return nil, err
	}
	defer logutil.LogOnError(ctx, s.logger, client.Close, "Couldn't close scylla client")

	dcs, err := client.Datacenters(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "get hosts for cluster with id %s", clusterID)
	}

	for dc, hosts := range dcs {
		for _, h := range hosts {
			sh, err := client.ShardCount(ctx, h)
			if err != nil {
				s.logger.Error(ctx, "Failed to get number of shards", "error", err)
			}
			ni, err := client.NodeInfo(ctx, h)
			if err != nil {
				return nil, errors.Wrapf(err, "node info call of %s", h)
			}

			promAddr := h
			// If PrometheusAddress is valid and specified, use its normalized string form.
			// Otherwise fall back to node address.
			if addr := ni.PrometheusAddress; addr != "" {
				if parsed, err := netip.ParseAddr(addr); err == nil && !parsed.IsUnspecified() {
					promAddr = parsed.String()
				}
				// Else: leave promAddr as `h`
			}

			promPort, err := strconv.Atoi(ni.PrometheusPort)
			if err != nil {
				promPort = 9180
			}
			nodes = append(nodes, Node{
				Datacenter:        dc,
				Address:           h,
				ShardNum:          sh,
				PrometheusAddress: promAddr,
				PrometheusPort:    promPort,
			})
		}
	}

	return nodes, nil
}

// SessionConfigOption defines function modifying cluster config that can be used when creating session.
type SessionConfigOption func(ctx context.Context, cluster *Cluster, client *scyllaclient.Client, cfg *gocql.ClusterConfig) error

// SingleHostSessionConfigOption ensures that session will be connected only to the single, provided host.
func SingleHostSessionConfigOption(host string) SessionConfigOption {
	return func(ctx context.Context, cluster *Cluster, client *scyllaclient.Client, cfg *gocql.ClusterConfig) error {
		ni, err := client.NodeInfo(ctx, host)
		if err != nil {
			return errors.Wrapf(err, "fetch node (%s) info", host)
		}
		cqlAddr := ni.CQLAddr(host, cluster.ForceTLSDisabled || cluster.ForceNonSSLSessionPort)
		cfg.Hosts = []string{cqlAddr}
		cfg.DisableInitialHostLookup = true
		cfg.HostFilter = gocql.WhiteListHostFilter(cqlAddr)
		return nil
	}
}

// SessionFunc returns CQL session for given cluster ID.
type SessionFunc func(ctx context.Context, clusterID uuid.UUID, opts ...SessionConfigOption) (gocqlx.Session, error)

// GetSession returns CQL session to provided cluster.
func (s *Service) GetSession(ctx context.Context, clusterID uuid.UUID, opts ...SessionConfigOption) (session gocqlx.Session, err error) {
	s.logger.Info(ctx, "Get session", "cluster_id", clusterID)

	clusterInfo, err := s.GetClusterByID(ctx, clusterID)
	if err != nil {
		return session, errors.Wrap(err, "cluster by id")
	}
	client, err := s.createClientFromCluster(clusterInfo)
	if err != nil {
		return session, errors.Wrap(err, "get client")
	}
	defer logutil.LogOnError(ctx, s.logger, client.Close, "Couldn't close scylla client")

	cfg := gocql.NewCluster()
	for _, opt := range opts {
		if err := opt(ctx, clusterInfo, client, cfg); err != nil {
			return session, err
		}
	}

	// Fill hosts if they weren't specified by the options or make sure that they use correct rpc address.
	if len(cfg.Hosts) == 0 {
		sessionHosts, err := GetRPCAddresses(ctx, client, client.Config().Hosts, clusterInfo.ForceTLSDisabled || clusterInfo.ForceNonSSLSessionPort)
		if err != nil {
			s.logger.Info(ctx, "Gets session", "err", err)
			if errors.Is(err, ErrNoRPCAddressesFound) {
				return session, err
			}
		}
		cfg.Hosts = sessionHosts
	}

	ni, err := client.AnyNodeInfo(ctx)
	if err != nil {
		return session, errors.Wrap(err, "fetch node info")
	}
	if err := s.extendClusterConfigWithTLS(clusterInfo, ni, cfg); err != nil {
		return session, err
	}
	if err := s.extendClusterConfigWithAuthentication(clusterInfo, ni, cfg); err != nil {
		return session, err
	}

	return gocqlx.WrapSession(cfg.CreateSession())
}

// ErrNoCQLCredentials is returned when cluster CQL credentials are required to create session,
// but they weren't added to the SM.
var ErrNoCQLCredentials = errors.New("cluster requires CQL authentication but username/password was not set. " +
	"Use 'sctool cluster update --username --password' for adding them")

func (s *Service) extendClusterConfigWithAuthentication(clusterInfo *Cluster, ni *scyllaclient.NodeInfo, cfg *gocql.ClusterConfig) error {
	if ni.ClientEncryptionRequireAuth && cfg.SslOpts == nil {
		return errors.New("CQL client-certificate authentication requires verified TLS")
	}
	if ni.CqlPasswordProtected {
		if !ni.ClientEncryptionEnabled || clusterInfo.ForceTLSDisabled || cfg.SslOpts == nil {
			return errors.New("CQL password authentication requires verified TLS; refusing to transmit credentials over plaintext")
		}
		username, password, configured, err := s.cqlCredentialsForCluster(clusterInfo)
		if err != nil {
			return errors.Wrap(err, "get credentials")
		}
		if !configured {
			return ErrNoCQLCredentials
		}

		cfg.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}
	return nil
}

func (s *Service) cqlCredentialsForCluster(c *Cluster) (username, password string, configured bool, err error) {
	if c.Username != "" || c.Password != "" {
		if c.Username == "" || c.Password == "" {
			return "", "", false, util.ErrValidate(errors.New("incomplete CQL credentials"))
		}
		return c.Username, c.Password, true, nil
	}
	return "", "", false, nil
}

func (s *Service) extendClusterConfigWithTLS(cluster *Cluster, ni *scyllaclient.NodeInfo, cfg *gocql.ClusterConfig) error {
	if ni.ClientEncryptionEnabled && !cluster.ForceTLSDisabled {
		tlsConfig, err := s.clusterTLSConfig(cluster, secrets.CQLProtocol)
		if err != nil {
			return errors.Wrap(err, "CQL TLS is enabled, but strict CQL trust is unavailable")
		}
		cfg.SslOpts = &gocql.SslOptions{
			Config:                 tlsConfig,
			EnableHostVerification: true,
		}
		if ni.ClientEncryptionRequireAuth {
			keyPair, err := s.tlsIdentityForCluster(cluster)
			if err != nil {
				return err
			}
			cfg.SslOpts.Certificates = []tls.Certificate{keyPair}
		}
	}

	return nil
}

// AlternatorClientFunc returns alternator client for given cluster ID.
type AlternatorClientFunc func(ctx context.Context, clusterID uuid.UUID, host string) (*dynamodb.Client, error)

// GetAlternatorClient returns alternator client for given cluster ID.
func (s *Service) GetAlternatorClient(ctx context.Context, clusterID uuid.UUID, host string) (*dynamodb.Client, error) {
	s.logger.Info(ctx, "Get Alternator client", "cluster_id", clusterID)

	clusterInfo, err := s.GetClusterByID(ctx, clusterID)
	if err != nil {
		return nil, errors.Wrap(err, "get cluster")
	}
	client, err := s.createClientFromCluster(clusterInfo)
	if err != nil {
		return nil, errors.Wrap(err, "get client")
	}
	defer logutil.LogOnError(ctx, s.logger, client.Close, "Couldn't close scylla client")
	ni, err := client.NodeInfo(ctx, host)
	if err != nil {
		return nil, errors.Wrapf(err, "get node (%s) info", host)
	}

	cfg, err := s.alternatorClientConfigForCluster(clusterInfo, host, ni)
	if err != nil {
		return nil, errors.Wrap(err, "create alternator client config")
	}
	return dynamodb.NewFromConfig(cfg), nil
}

// alternatorClientConfig return aws.Config used for creating *dynamodb.DynamoDB for communicating with alternator API.
// It uses cluster scyllaclient.Config for configuring timeout and retry mechanisms.
func (s *Service) alternatorClientConfig(ctx context.Context, clusterID uuid.UUID, host string, ni *scyllaclient.NodeInfo) (aws.Config, error) {
	if ni.AlternatorEnforceAuthorization && !ni.AlternatorEncryptionEnabled() {
		return aws.Config{}, errors.New("Alternator authentication requires verified TLS; refusing to transmit credentials over plaintext")
	}
	cluster, err := s.GetClusterByID(ctx, clusterID)
	if err != nil {
		return aws.Config{}, errors.Wrap(err, "get cluster")
	}
	return s.alternatorClientConfigForCluster(cluster, host, ni)
}

func (s *Service) alternatorClientConfigForCluster(cluster *Cluster, host string, ni *scyllaclient.NodeInfo) (aws.Config, error) {
	if ni.AlternatorEnforceAuthorization && !ni.AlternatorEncryptionEnabled() {
		return aws.Config{}, errors.New("Alternator authentication requires verified TLS; refusing to transmit credentials over plaintext")
	}
	scCfg, err := s.clientConfig(cluster)
	if err != nil {
		return aws.Config{}, err
	}

	transport := alternatorTransport()
	if ni.AlternatorEncryptionEnabled() {
		transport.TLSClientConfig, err = s.clusterTLSConfig(cluster, secrets.AlternatorProtocol)
		if err != nil {
			return aws.Config{}, errors.Wrap(err, "Alternator TLS is enabled, but strict Alternator trust is unavailable")
		}
	}

	cfg := aws.Config{
		BaseEndpoint: aws.String(ni.AlternatorAddr(host)),
		Region:       "scylla",
		HTTPClient: &http.Client{
			Transport: transport,
			Timeout:   scCfg.Timeout,
		},
		Retryer: func() aws.Retryer {
			return retry.NewStandard(func(options *retry.StandardOptions) {
				options.MaxAttempts = int(scCfg.Backoff.MaxRetries) + 1
				options.MaxBackoff = scCfg.Backoff.WaitMax
			})
		},
	}

	if ni.AlternatorEnforceAuthorization {
		cfg.Credentials, err = s.alternatorCredentialsForCluster(cluster)
		if err != nil {
			return aws.Config{}, errors.Wrap(err, "get alternator credentials")
		}
	}

	return cfg, nil
}

func alternatorTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

// ErrNoAlternatorCredentials is returned when cluster alternator credentials are required, but credentials weren't added.
var ErrNoAlternatorCredentials = errors.New("cluster requires alternator authentication but they aren't set. " +
	"Use 'sctool cluster update --alternator-access-key-id --alternator-secret-access-key' for adding them")

func (s *Service) alternatorCredentialsForCluster(cluster *Cluster) (aws.CredentialsProvider, error) {
	c := &secrets.AlternatorCreds{
		ClusterID:       cluster.ID,
		AccessKeyID:     cluster.AlternatorAccessKeyID,
		SecretAccessKey: cluster.AlternatorSecretAccessKey,
	}
	if c.AccessKeyID == "" && c.SecretAccessKey == "" {
		return nil, ErrNoAlternatorCredentials
	} else if c.AccessKeyID == "" || c.SecretAccessKey == "" {
		return nil, util.ErrValidate(errors.New("incomplete Alternator credentials"))
	}
	return aws.CredentialsProviderFunc(func(_ context.Context) (aws.Credentials, error) {
		return aws.Credentials{
			AccessKeyID:     c.AccessKeyID,
			SecretAccessKey: c.SecretAccessKey,
		}, nil
	}), nil
}

// ErrNoTLSIdentity is returned when cluster TSL/SSL key/cert is required to create session,
// but they weren't added to the SM.
var ErrNoTLSIdentity = errors.New("cluster requires encryption authentication but TSL/SSL key/cert were not set. " +
	"Use 'sctool cluster update --ssl-user-key-file --ssl-user-cert-file' for adding them")

func (s *Service) tlsIdentityForCluster(cluster *Cluster) (tls.Certificate, error) {
	tlsIdentity := &secrets.TLSIdentity{
		ClusterID:  cluster.ID,
		Cert:       cluster.SSLUserCertFile,
		PrivateKey: cluster.SSLUserKeyFile,
	}
	if len(tlsIdentity.Cert) == 0 && len(tlsIdentity.PrivateKey) == 0 {
		return tls.Certificate{}, ErrNoTLSIdentity
	} else if len(tlsIdentity.Cert) == 0 || len(tlsIdentity.PrivateKey) == 0 {
		return tls.Certificate{}, util.ErrValidate(errors.New("incomplete TLS/SSL identity"))
	}

	keyPair, err := tls.X509KeyPair(tlsIdentity.Cert, tlsIdentity.PrivateKey)
	if err != nil {
		return tls.Certificate{}, errors.Wrap(err, "invalid TLS/SSL user key pair")
	}
	return keyPair, nil
}

func (s *Service) notifyChangeListener(ctx context.Context, c Change) error {
	if s.onChangeListener == nil {
		return nil
	}
	return s.onChangeListener(ctx, c)
}

func (s *Service) notifyCommittedChange(parent context.Context, generation uuid.UUID, change Change) error {
	ctx, cancel := s.postCommitConnectionContext(parent, generation)
	defer cancel()
	return s.notifyChangeListener(ctx, change)
}

func (s *Service) reconcileCommittedConnection(parent context.Context, generation uuid.UUID, change Change) (listenerErr, sessionErr error) {
	ctx, cancel := s.postCommitConnectionContext(parent, generation)
	defer cancel()

	listenerErr = s.notifyChangeListener(ctx, change)
	openSession := s.postCommitSession
	if openSession == nil {
		openSession = func(ctx context.Context, id uuid.UUID) (gocqlx.Session, error) {
			return s.GetSession(ctx, id)
		}
	}
	session, sessionErr := openSession(ctx, change.ID)
	if sessionErr == nil {
		session.Close()
	}
	return listenerErr, sessionErr
}

func (s *Service) invalidateConnectionConfigCache(clusterID uuid.UUID) {
	if s.onConnectionInvalidation != nil {
		s.onConnectionInvalidation(clusterID)
	}
}

// Close closes all connections to cluster.
func (s *Service) Close() {
	s.clientCache.Close()
}

// ErrNoRPCAddressesFound is the error representation of "no RPC addresses found".
var ErrNoRPCAddressesFound = errors.New("no RPC addresses found")

// GetRPCAddresses accepts client and hosts parameters that are used later on to query client.NodeInfo endpoint
// returning RPC addresses for given hosts.
// RPC addresses are the ones that scylla uses to accept CQL connections.
func GetRPCAddresses(ctx context.Context, client *scyllaclient.Client, hosts []string, clusterTLSAddrDisabled bool) ([]string, error) {
	var sessionHosts []string
	var combinedError error
	for _, h := range hosts {
		ni, err := client.NodeInfo(ctx, h)
		if err != nil {
			combinedError = multierr.Append(combinedError, err)
			continue
		}
		addr := ni.CQLAddr(h, clusterTLSAddrDisabled)
		sessionHosts = append(sessionHosts, addr)
	}

	if len(sessionHosts) == 0 {
		combinedError = multierr.Append(ErrNoRPCAddressesFound, combinedError)
	}

	return sessionHosts, combinedError
}
