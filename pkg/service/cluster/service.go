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
	// mutationMu serializes row and secret mutations so concurrent rotations
	// cannot interleave into a bundle that neither caller preflighted. This is
	// process-local; the secure deployment contract is one Manager writer.
	mutationMu          sync.Mutex
	session             gocqlx.Session
	metrics             metrics.ClusterMetrics
	secretsStore        store.Store
	clientCache         *scyllaclient.CachedProvider
	timeoutConfig       scyllaclient.TimeoutConfig
	logger              log.Logger
	onChangeListener    func(ctx context.Context, c Change) error
	cqlQueryPing        func(context.Context, cqlping.Config, string, string) (time.Duration, error)
	alternatorQueryPing func(context.Context, dynamoping.Config) (time.Duration, error)
}

func NewService(session gocqlx.Session, metrics metrics.ClusterMetrics, secretsStore store.Store, timeoutConfig scyllaclient.TimeoutConfig,
	cacheInvalidationTimeout time.Duration, l log.Logger,
) (*Service, error) {
	if session.Session == nil || session.Closed() {
		return nil, errors.New("invalid session")
	}

	s := &Service{
		session:             session,
		metrics:             metrics,
		secretsStore:        secretsStore,
		logger:              l,
		timeoutConfig:       timeoutConfig,
		cqlQueryPing:        cqlping.QueryPing,
		alternatorQueryPing: dynamoping.QueryPing,
	}
	s.clientCache = scyllaclient.NewCachedProvider(s.CreateClientNoCache, cacheInvalidationTimeout, l)

	return s, nil
}

// Init initializes metrics from database.
func (s *Service) Init(ctx context.Context) error {
	s.logger.Debug(ctx, "Init")

	var clusters []*Cluster
	if err := s.session.Query(table.Cluster.SelectAll()).SelectRelease(&clusters); err != nil {
		return err
	}

	for _, c := range clusters {
		s.metrics.SetName(c.ID, c.Name)
	}

	return nil
}

// SetOnChangeListener sets a function that would be invoked when a cluster
// changes.
func (s *Service) SetOnChangeListener(f func(ctx context.Context, c Change) error) {
	s.onChangeListener = f
}

// Client is cluster client provider.
func (s *Service) Client(ctx context.Context, clusterID uuid.UUID) (*scyllaclient.Client, error) {
	s.logger.Debug(ctx, "Client", "cluster_id", clusterID)
	return s.clientCache.Client(ctx, clusterID)
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

	if err := s.discoverAndSetClusterHosts(ctx, c); err != nil {
		return nil, errors.Wrap(err, "discover and set cluster hosts")
	}

	config, err := s.clientConfig(c)
	if err != nil {
		return nil, err
	}
	return scyllaclient.NewClient(config, s.logger.Named("client"))
}

func (s *Service) clientConfig(c *Cluster) (scyllaclient.Config, error) {
	config := scyllaclient.DefaultConfigWithTimeout(s.timeoutConfig)
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
	return secrets.LoadTLSConfig(s.secretsStore, trust)
}

func (s *Service) discoverAndSetClusterHosts(ctx context.Context, c *Cluster) error {
	knownHosts, _, err := s.discoverClusterHosts(ctx, c)
	if err != nil {
		if errors.Is(err, ErrNoValidKnownHost) {
			s.logger.Error(ctx, "There is no single valid known host for the cluster. "+
				"Please update it with 'sctool cluster update -h <host>'",
				"cluster", c.ID,
				"contact point", c.Host,
				"discovered hosts", c.KnownHosts,
			)
		}
		return err
	}
	return errors.Wrap(s.setKnownHosts(c, knownHosts), "update known_hosts in SM DB")
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

func (s *Service) loadKnownHosts(c *Cluster) error {
	q := table.Cluster.GetQuery(s.session, "known_hosts").BindStruct(c)
	return q.GetRelease(c)
}

func (s *Service) setKnownHosts(c *Cluster, hosts []string) error {
	c.KnownHosts = hosts

	q := table.Cluster.UpdateQuery(s.session, "known_hosts").BindStruct(c)
	return q.ExecRelease()
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
	s.logger.Debug(ctx, "GetClusterByID", "id", id)

	q := table.Cluster.GetQuery(s.session).BindMap(qb.M{
		"id": id,
	})
	defer q.Release()

	if q.Err() != nil {
		return nil, q.Err()
	}

	var c Cluster
	if err := q.Get(&c); err != nil {
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

	if c == nil {
		return util.ErrNilPtr
	}
	s.logger.Debug(ctx, "PutCluster", "cluster_id", c.ID, "cluster_name", c.Name)

	t, err := s.choosePutClusterChangeType(ctx, c)
	if err != nil {
		return errors.Wrap(err, "check for cluster creation or update")
	}

	if t == Create {
		s.logger.Info(ctx, "Adding new cluster", "cluster_id", c.ID)
	} else {
		s.logger.Info(ctx, "Updating cluster", "cluster_id", c.ID)
		// Putting cluster should theoretically just set cluster state to the provided one.
		// The problem is that Cluster.KnownHosts are not part of REST API definitions,
		// so it's possible that cluster passed to PutCluster doesn't have it set by accident.
		// This shouldn't be the case, but since it never makes sense for Cluster.KnownHosts
		// to be empty (they should at least contain resolved Cluster.Host), we can add additional
		// safety net here and load them if they are missing.
		if len(c.KnownHosts) == 0 {
			if err := s.loadKnownHosts(c); err != nil && !errors.Is(err, gocql.ErrNotFound) {
				return errors.Wrap(err, "load known hosts")
			}
		}
	}

	// Validate cluster model
	if err := c.Validate(); err != nil {
		return err
	}
	if t == Create && (len(c.AgentCAFile) == 0 || c.AgentServerName == "") {
		return util.ErrValidate(errors.New("Agent CA file and server name are required when creating a cluster"))
	}
	if t == Create {
		if err := s.ensureNoStoredSecretsOnCreate(c.ID); err != nil {
			return err
		}
	}
	if c.ForceTLSDisabled {
		configured, err := s.CheckTLSTrust(c.ID, secrets.CQLProtocol)
		if err != nil {
			return errors.Wrap(err, "check CQL TLS trust")
		}
		if configured {
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
		old, err := s.GetClusterByID(ctx, c.ID)
		if err != nil {
			return err
		}
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

	// Rollback on error.
	var rollback []func() error
	defer func() {
		if err != nil {
			for i := len(rollback) - 1; i >= 0; i-- {
				r := rollback[i]
				if r != nil {
					if rollbackErr := r(); rollbackErr != nil {
						s.logger.Error(ctx, "Failed to roll back cluster secret mutation", "cluster_id", c.ID, "error", rollbackErr)
						err = multierr.Append(err, errors.Wrap(rollbackErr, "roll back cluster secret mutation"))
					}
				}
			}
		}
	}()

	if len(c.SSLUserCertFile) != 0 && len(c.SSLUserKeyFile) != 0 {
		r, err := store.PutWithRollback(s.secretsStore, &secrets.TLSIdentity{
			ClusterID:  c.ID,
			Cert:       c.SSLUserCertFile,
			PrivateKey: c.SSLUserKeyFile,
		})
		if err != nil {
			return errors.Wrap(err, "save SSL cert file")
		}
		rollback = append(rollback, r)
	}

	if c.Username != "" {
		r, err := store.PutWithRollback(s.secretsStore, &secrets.CQLCreds{
			ClusterID: c.ID,
			Username:  c.Username,
			Password:  c.Password,
		})
		if err != nil {
			return errors.Wrap(err, "save CQL credentials")
		}
		rollback = append(rollback, r)
	}

	if c.AlternatorAccessKeyID != "" {
		r, err := store.PutWithRollback(s.secretsStore, &secrets.AlternatorCreds{
			ClusterID:       c.ID,
			AccessKeyID:     c.AlternatorAccessKeyID,
			SecretAccessKey: c.AlternatorSecretAccessKey,
		})
		if err != nil {
			return errors.Wrap(err, "save alternator credentials")
		}
		rollback = append(rollback, r)
	}

	for _, trust := range clusterTLSTrustEntries(c) {
		r, err := store.PutWithRollback(s.secretsStore, trust)
		if err != nil {
			return errors.Wrapf(err, "save %s TLS trust", trust.Protocol)
		}
		rollback = append(rollback, r)
	}

	q := table.Cluster.InsertQuery(s.session).BindStruct(c)

	if err := q.ExecRelease(); err != nil {
		return err
	}
	// The cluster row and its secrets now represent one committed state. A
	// downstream cache or scheduler notification failure must not roll back only
	// the secrets while leaving the database row behind.
	rollback = nil

	// Secrets and the cluster row are now committed. Retire cached Agent
	// transports immediately, and let the listener invalidate/rebuild the node
	// configuration cache before any potentially slow post-commit network work.
	// This closes the window in which a rotated CA, token, or client identity
	// could coexist with a usable old cached client/configuration.
	s.clientCache.Invalidate(c.ID)
	changeEvent := Change{
		ID:            c.ID,
		Type:          t,
		WithoutRepair: c.WithoutRepair,
	}
	listenerErr := s.notifyChangeListener(ctx, changeEvent)

	if c.AuthToken == "" {
		s.logger.Info(ctx, "WARNING! Scylla data is exposed on hosts, "+
			"protect it by specifying auth_token in Scylla Manager Agent config file on Scylla nodes",
			"cluster_id", c.ID,
			"hosts", c.KnownHosts,
		)
	}

	// Create the session and log error
	session, err := s.GetSession(ctx, c.ID)
	if err != nil {
		s.logger.Info(ctx, "WARNING! Cannot create CQL session to the cluster. It will affect backup/restore/healthcheck services.",
			"cluster_id", c.ID)
	} else {
		session.Close()
	}

	switch t {
	case Create:
		s.logger.Info(ctx, "Cluster added", "cluster_id", c.ID)
	case Update:
		s.logger.Info(ctx, "Cluster updated", "cluster_id", c.ID)
	}

	s.metrics.SetName(c.ID, c.Name)
	return listenerErr
}

func clusterTLSTrustEntries(c *Cluster) []*secrets.TLSTrust {
	entries := make([]*secrets.TLSTrust, 0, 3)
	appendTrust := func(trust *secrets.TLSTrust, ca []byte, serverName string) {
		if len(ca) == 0 && serverName == "" {
			return
		}
		trust.CA = ca
		trust.ServerName = serverName
		entries = append(entries, trust)
	}
	appendTrust(secrets.NewCQLTLSTrust(c.ID), c.CQLCAFile, c.CQLServerName)
	appendTrust(secrets.NewAlternatorTLSTrust(c.ID), c.AlternatorCAFile, c.AlternatorServerName)
	appendTrust(secrets.NewAgentTLSTrust(c.ID), c.AgentCAFile, c.AgentServerName)
	return entries
}

func (s *Service) ensureNoStoredSecretsOnCreate(clusterID uuid.UUID) error {
	entries := []store.Entry{
		&secrets.CQLCreds{ClusterID: clusterID},
		&secrets.AlternatorCreds{ClusterID: clusterID},
		&secrets.TLSIdentity{ClusterID: clusterID},
		secrets.NewCQLTLSTrust(clusterID),
		secrets.NewAlternatorTLSTrust(clusterID),
		secrets.NewAgentTLSTrust(clusterID),
	}
	for _, entry := range entries {
		configured, err := s.secretsStore.Check(entry)
		if err != nil {
			return errors.Wrap(err, "check for orphaned cluster secrets")
		}
		if configured {
			return util.ErrValidate(errors.New("the requested cluster ID has orphaned secrets; clean them up or use a fresh ID"))
		}
	}
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
	_, err := s.GetClusterByID(ctx, c.ID)
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
	if err := s.loadKnownHosts(c); err != nil && !errors.Is(err, gocql.ErrNotFound) {
		return errors.Wrap(err, "load known hosts")
	}

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
		return nil
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
		return nil
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

	q := table.Cluster.DeleteQuery(s.session).BindMap(qb.M{
		"id": clusterID,
	})

	if err := q.ExecRelease(); err != nil {
		return err
	}

	// The row is gone, so retire all cached authenticated transports and node
	// configuration before attempting best-effort secret cleanup. A secrets
	// table failure must not leave a deleted cluster's old bearer token and
	// pinned trust usable by scheduled work.
	s.clientCache.Invalidate(clusterID)
	listenerErr := s.notifyChangeListener(ctx, Change{ID: clusterID, Type: Delete})
	secretsErr := s.secretsStore.DeleteAll(clusterID)
	if secretsErr != nil {
		s.logger.Error(ctx, "Failed to delete cluster secrets",
			"cluster_id", clusterID,
			"error", secretsErr,
		)
	}
	return errors.Wrap(multierr.Combine(listenerErr, secretsErr), "delete cluster cleanup")
}

// CheckCQLCredentials checks if associated CQLCreds exist in secrets store.
func (s *Service) CheckCQLCredentials(id uuid.UUID) (bool, error) {
	credentials := secrets.CQLCreds{
		ClusterID: id,
	}
	return s.secretsStore.Check(&credentials)
}

// DeleteCQLCredentials removes the associated CQLCreds from secrets store.
func (s *Service) DeleteCQLCredentials(_ context.Context, clusterID uuid.UUID) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	return s.secretsStore.Delete(&secrets.CQLCreds{
		ClusterID: clusterID,
	})
}

// CheckAlternatorCredentials checks if associated AlternatorCreds exist in secrets store.
func (s *Service) CheckAlternatorCredentials(id uuid.UUID) (bool, error) {
	credentials := secrets.AlternatorCreds{
		ClusterID: id,
	}
	return s.secretsStore.Check(&credentials)
}

// DeleteAlternatorCredentials removes the associated AlternatorCreds from secrets store.
func (s *Service) DeleteAlternatorCredentials(_ context.Context, clusterID uuid.UUID) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	return s.secretsStore.Delete(&secrets.AlternatorCreds{
		ClusterID: clusterID,
	})
}

// DeleteSSLUserCert removes the associated TLSIdentity from secrets store.
func (s *Service) DeleteSSLUserCert(ctx context.Context, clusterID uuid.UUID) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if err := s.secretsStore.Delete(&secrets.TLSIdentity{
		ClusterID: clusterID,
	}); err != nil {
		return err
	}
	s.clientCache.Invalidate(clusterID)
	return s.notifyChangeListener(ctx, Change{ID: clusterID, Type: Update})
}

// CheckSSLUserCert checks if an associated TLS client identity exists.
func (s *Service) CheckSSLUserCert(id uuid.UUID) (bool, error) {
	return s.secretsStore.Check(&secrets.TLSIdentity{ClusterID: id})
}

// CheckTLSTrust checks if protocol-specific strict TLS trust exists.
func (s *Service) CheckTLSTrust(id uuid.UUID, protocol string) (bool, error) {
	trust, err := tlsTrustEntry(id, protocol)
	if err != nil {
		return false, err
	}
	return s.secretsStore.Check(trust)
}

// DeleteTLSTrust removes protocol-specific strict TLS trust.
func (s *Service) DeleteTLSTrust(ctx context.Context, id uuid.UUID, protocol string) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if protocol == secrets.AgentProtocol {
		return util.ErrValidate(errors.New("Agent TLS trust is mandatory; rotate it through PUT or delete the cluster"))
	}
	trust, err := tlsTrustEntry(id, protocol)
	if err != nil {
		return err
	}
	if err := s.secretsStore.Delete(trust); err != nil {
		return err
	}
	s.clientCache.Invalidate(id)
	return s.notifyChangeListener(ctx, Change{ID: id, Type: Update})
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

	client, err := s.CreateClientNoCache(ctx, clusterID)
	if err != nil {
		return session, errors.Wrap(err, "get client")
	}
	defer logutil.LogOnError(ctx, s.logger, client.Close, "Couldn't close scylla client")

	clusterInfo, err := s.GetClusterByID(ctx, clusterID)
	if err != nil {
		return session, errors.Wrap(err, "cluster by id")
	}

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
	credentials := &secrets.CQLCreds{ClusterID: c.ID}
	if err := s.secretsStore.Get(credentials); err != nil {
		if errors.Is(err, util.ErrNotFound) {
			return "", "", false, nil
		}
		return "", "", false, err
	}
	return credentials.Username, credentials.Password, true, nil
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
			keyPair, err := s.loadTLSIdentity(cluster.ID)
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

	client, err := s.clientCache.Client(ctx, clusterID)
	if err != nil {
		return nil, errors.Wrap(err, "get client")
	}
	ni, err := client.NodeInfo(ctx, host)
	if err != nil {
		return nil, errors.Wrapf(err, "get node (%s) info", host)
	}

	cfg, err := s.alternatorClientConfig(ctx, clusterID, host, ni)
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
		cfg.Credentials, err = s.alternatorCredentials(clusterID)
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

func (s *Service) alternatorCredentials(clusterID uuid.UUID) (aws.CredentialsProvider, error) {
	return s.alternatorCredentialsForCluster(&Cluster{ID: clusterID})
}

func (s *Service) alternatorCredentialsForCluster(cluster *Cluster) (aws.CredentialsProvider, error) {
	c := &secrets.AlternatorCreds{
		ClusterID:       cluster.ID,
		AccessKeyID:     cluster.AlternatorAccessKeyID,
		SecretAccessKey: cluster.AlternatorSecretAccessKey,
	}
	if c.AccessKeyID == "" && c.SecretAccessKey == "" {
		if err := s.secretsStore.Get(c); err != nil {
			if errors.Is(err, util.ErrNotFound) {
				return nil, ErrNoAlternatorCredentials
			}
			return nil, errors.Wrap(err, "get credentials from secrets store")
		}
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

func (s *Service) loadTLSIdentity(clusterID uuid.UUID) (tls.Certificate, error) {
	return s.tlsIdentityForCluster(&Cluster{ID: clusterID})
}

func (s *Service) tlsIdentityForCluster(cluster *Cluster) (tls.Certificate, error) {
	tlsIdentity := &secrets.TLSIdentity{
		ClusterID:  cluster.ID,
		Cert:       cluster.SSLUserCertFile,
		PrivateKey: cluster.SSLUserKeyFile,
	}
	if len(tlsIdentity.Cert) == 0 && len(tlsIdentity.PrivateKey) == 0 {
		if err := s.secretsStore.Get(tlsIdentity); err != nil {
			if errors.Is(err, util.ErrNotFound) {
				return tls.Certificate{}, ErrNoTLSIdentity
			}
			return tls.Certificate{}, errors.Wrap(err, "get TLS/SSL identity")
		}
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
