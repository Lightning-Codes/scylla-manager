// Copyright (C) 2024 ScyllaDB

package configcache

import (
	"context"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// ConfigCacher is the interface defining the cache behavior.
type ConfigCacher interface {
	// Read returns either the host configuration that is currently stored in the cache,
	// ErrNoClusterConfig if config for the whole cluster doesn't exist,
	// or ErrNoHostConfig if config of the particular host doesn't exist.
	Read(clusterID uuid.UUID, host string) (NodeConfig, error)

	// ReadAll calls Read on all AvailableHosts.
	ReadAll(clusterID uuid.UUID) (map[string]NodeConfig, error)

	// AvailableHosts returns list of hosts of given cluster that keep their configuration in cache.
	AvailableHosts(ctx context.Context, clusterID uuid.UUID) ([]string, error)

	// ForceUpdateCluster updates single cluster config in cache and does it outside the background process.
	// Hosts argument allows for restricting the update to specific hosts. Empty hosts results in full update.
	ForceUpdateCluster(ctx context.Context, clusterID uuid.UUID, hosts ...string) bool

	// RemoveCluster removes cluster data of a given uuid from cache.
	RemoveCluster(clusterID uuid.UUID)

	// Init updates cache with config of all currently managed clusters.
	Init(ctx context.Context)

	// Run starts the infinity loop responsible for updating the clusters configuration periodically.
	Run(ctx context.Context)
}

// Service is responsible for handling all cluster configuration cache related operations.
// Use svc.Read(clusterID, host) to read the configuration of particular host in given cluster.
// Use svc.Run() to let the cache update itself periodically with the current configuration.
type Service struct {
	svcConfig Config

	clusterSvc   cluster.Servicer
	scyllaClient scyllaclient.ProviderFunc
	configs      *sync.Map
	// revisions contains a monotonically increasing generation per cluster. It
	// prevents a slower refresh that started with old trust from publishing
	// after a newer rotation or deletion refresh.
	revisions *sync.Map
	logger    log.Logger

	// nodeConfigLoader is a test seam for exercising refresh publication races
	// without issuing Agent requests. Production always uses retrieveNodeConfig.
	nodeConfigLoader func(context.Context, string, *scyllaclient.Client, *cluster.Cluster) (NodeConfig, error)
}

type clusterConfigEntry struct {
	revision             uint64
	connectionGeneration uuid.UUID
	lifecycleEpoch       int64
	configs              *sync.Map
}

// NewService is the constructor for the cluster config cache service.
func NewService(config Config, clusterSvc cluster.Servicer, client scyllaclient.ProviderFunc, logger log.Logger) ConfigCacher {
	return &Service{
		svcConfig:    config,
		clusterSvc:   clusterSvc,
		scyllaClient: client,
		configs:      &sync.Map{},
		revisions:    &sync.Map{},
		logger:       logger,
	}
}

func (svc *Service) Init(ctx context.Context) {
	// Invalidate every in-flight refresh without resetting generation numbers.
	// Resetting generations could let a pre-Init refresh collide with and
	// overwrite the first post-Init generation.
	svc.revisions.Range(func(_, value any) bool {
		value.(*atomic.Uint64).Add(1)
		return true
	})
	svc.configs.Range(func(key, _ any) bool {
		svc.configs.Delete(key)
		return true
	})
	svc.updateAll(ctx)
}

func (svc *Service) Read(clusterID uuid.UUID, host string) (NodeConfig, error) {
	emptyConfig := NodeConfig{}

	clusterConfig, err := svc.readClusterConfig(clusterID)
	if err != nil {
		return emptyConfig, err
	}

	hostKey := host
	// Remove leading '[' and trailing ']' if present.
	// netip.ParseAddr(bracketedIP) leaves brackets.
	// Need to remove it.
	if host != "" && host[0] == '[' && host[len(host)-1] == ']' {
		host = host[1 : len(host)-1]
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		hostKey = addr.String()
	}

	rawHostConfig, ok := clusterConfig.Load(hostKey)
	if !ok {
		return emptyConfig, ErrNoHostConfig
	}
	hostConfig, ok := rawHostConfig.(NodeConfig)
	if !ok {
		panic("cluster host emptyConfig cache stores unexpected type")
	}
	return hostConfig, nil
}

// ReadAll calls Read on AvailableHosts.
func (svc *Service) ReadAll(clusterID uuid.UUID) (map[string]NodeConfig, error) {
	clusterConfig, err := svc.readClusterConfig(clusterID)
	if err != nil {
		return nil, err
	}

	out := make(map[string]NodeConfig)
	clusterConfig.Range(func(key, value any) bool {
		host := key.(string)
		cfg := value.(NodeConfig)
		out[host] = cfg
		return true
	})
	return out, nil
}

// Run starts the infinity loop responsible for updating the clusters configuration periodically.
func (svc *Service) Run(ctx context.Context) {
	freq := time.NewTicker(svc.svcConfig.UpdateFrequency)

	for {
		// make sure to shut down when the context is cancelled
		select {
		case <-ctx.Done():
			return
		default:
		}

		select {
		case <-freq.C:
			svc.updateAll(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// RemoveCluster removes cluster data of a given uuid from cache.
func (svc *Service) RemoveCluster(clusterID uuid.UUID) {
	revision := svc.nextRevision(clusterID)
	svc.invalidateOlderConfig(clusterID, revision)
}

// ForceUpdateCluster updates single cluster config in cache and does it outside the background process.
// Hosts argument allows for restricting the update to specific hosts. Empty hosts results in full update.
func (svc *Service) ForceUpdateCluster(ctx context.Context, clusterID uuid.UUID, hosts ...string) bool {
	logger := svc.logger.Named("Force update cluster").With("cluster", clusterID)
	// Reserve ordering before the authoritative read. A refresh that captures A
	// and pauses cannot later take a revision newer than a B refresh and evict B.
	revision := svc.nextRevision(clusterID)
	c, err := svc.clusterSvc.GetCluster(ctx, clusterID.String())
	if err != nil {
		if errors.Is(err, cluster.ErrConnectionCommitConflict) {
			// A workflow pinned to stale A must not evict an already-published B.
			logger.Error(ctx, "Update rejected by generation precondition", "error", err)
			return false
		}
		// Not-found may be a remote lifecycle tombstone, and any other failure
		// cannot authorize continued use of cached credentials.
		svc.invalidateOlderConfig(clusterID, revision)
		logger.Error(ctx, "Update failed", "error", err)
		return false
	}
	if expected, ok := cluster.ExpectedConnectionGeneration(ctx); ok && c.ConnectionGeneration != expected {
		// A workflow pinned to stale A must not evict an already-published B
		// cache. It fails before touching the current generation instead.
		logger.Error(ctx, "Update failed", "error", errors.Wrapf(cluster.ErrConnectionCommitConflict,
			"expected generation %s, active generation %s", expected, c.ConnectionGeneration))
		return false
	}
	svc.invalidateOlderConfig(clusterID, revision)

	return svc.updateSingle(ctx, c, revision, hosts...)
}

// AvailableHosts returns list of hosts of given cluster that keep their configuration in cache.
func (svc *Service) AvailableHosts(ctx context.Context, clusterID uuid.UUID) ([]string, error) {
	logger := svc.logger.Named("Listing available hosts").With("cluster", clusterID)
	current, err := svc.clusterSvc.GetCluster(ctx, clusterID.String())
	if err != nil {
		// Do not mutate a possibly newer cache based on a stale not-found window
		// across D->C. This acquisition itself fails closed before returning hosts;
		// updateAll performs authoritative absence reconciliation.
		return nil, err
	}

	rawEntry, ok := svc.configs.Load(clusterID.String())
	if !ok {
		return nil, ErrNoClusterConfig
	}
	entry, ok := rawEntry.(*clusterConfigEntry)
	if !ok {
		panic("cluster config cache stores unexpected type")
	}
	clusterConfig := entry.configs

	var availableHosts []string
	var generationErr error
	clusterConfig.Range(func(key, value any) bool {
		host, ok := key.(string)
		if !ok {
			logger.Error(ctx, "Cannot cast to string", "host", key, "error", err)
			return false
		}
		cfg, ok := value.(NodeConfig)
		if !ok {
			panic("cluster host config cache stores unexpected type")
		}
		if cfg.ConnectionGeneration != current.ConnectionGeneration || cfg.ConnectionLifecycleEpoch != current.LifecycleEpoch {
			generationErr = errors.Wrapf(cluster.ErrConnectionCommitConflict,
				"cached generation %s at epoch %d, active generation %s at epoch %d",
				cfg.ConnectionGeneration, cfg.ConnectionLifecycleEpoch, current.ConnectionGeneration, current.LifecycleEpoch)
			return false
		}
		availableHosts = append(availableHosts, host)
		return true
	})
	if generationErr != nil {
		// current may be A while a concurrent refresh has already published B.
		// Unconditionally removing here would evict the newer verified snapshot.
		// Fail this caller closed; the authoritative refresh path reconciles the
		// remaining active generation.
		// Do not delete here: current may be A while rawEntry is a concurrently
		// published B. The authoritative refresh/reconciliation path owns cache
		// removal and revalidates the pointer around publication.
		return nil, generationErr
	}

	return availableHosts, nil
}

func (svc *Service) updateSingle(ctx context.Context, c *cluster.Cluster, revision uint64, hosts ...string) bool {
	logger := svc.logger.Named("Cluster config update").With("cluster", c.ID)
	clusterConfig := &sync.Map{}
	// List/updateAll snapshots are discovery hints only. Resolve the exact
	// current pointer before creating a credential-bearing client or making any
	// Agent/data-plane request. A remote tombstone or rotation fails before the
	// network boundary.
	current, err := svc.clusterSvc.GetCluster(ctx, c.ID.String())
	if err != nil || current.ConnectionGeneration != c.ConnectionGeneration || current.LifecycleEpoch != c.LifecycleEpoch {
		logger.Info(ctx, "Discarding stale cluster snapshot before client construction",
			"snapshot_generation", c.ConnectionGeneration,
			"snapshot_epoch", c.LifecycleEpoch,
			"error", err,
		)
		return false
	}
	c = current

	var client *scyllaclient.Client
	err = nil
	if provider, ok := svc.clusterSvc.(interface {
		CreateClientForClusterSnapshot(*cluster.Cluster) (*scyllaclient.Client, error)
	}); ok {
		client, err = provider.CreateClientForClusterSnapshot(c)
	} else {
		client, err = svc.scyllaClient(ctx, c.ID)
	}
	if err != nil {
		logger.Error(ctx, "Couldn't create scylla client", "cluster", c.ID, "error", err)
		return false
	}
	defer func() {
		if err := client.Close(); err != nil {
			logger.Error(ctx, "Couldn't close HTTP client", "error", err)
		}
	}()
	if client.Config().ConnectionGeneration != c.ConnectionGeneration {
		logger.Info(ctx, "Discarding mixed-generation Agent client",
			"cluster_generation", c.ConnectionGeneration,
			"client_generation", client.Config().ConnectionGeneration,
		)
		return false
	}
	if client.Config().ConnectionLifecycleEpoch != c.LifecycleEpoch {
		logger.Info(ctx, "Discarding mixed-lifecycle Agent client",
			"cluster_epoch", c.LifecycleEpoch,
			"client_epoch", client.Config().ConnectionLifecycleEpoch,
		)
		return false
	}
	// In case of empty hosts, use the ones discovered by scylla client
	if len(hosts) == 0 {
		hosts = client.Config().Hosts
	}
	hostsWg := sync.WaitGroup{}
	failed := atomic.Bool{}
	for _, host := range hosts {
		hostsWg.Add(1)
		hostKey := host

		parsedIP, err := netip.ParseAddr(host)
		if err == nil {
			hostKey = parsedIP.String()
		}

		perHostLogger := logger.Named("Cluster host config update").With("host", host, "hostKey", hostKey)
		go func() {
			defer hostsWg.Done()

			loader := svc.nodeConfigLoader
			if loader == nil {
				loader = svc.retrieveNodeConfig
			}
			config, err := loader(ctx, hostKey, client, c)
			if err != nil {
				failed.Store(true)
				perHostLogger.Error(ctx, "Couldn't read cluster host config", "error", err)
				return
			}
			clusterConfig.Store(hostKey, config)
		}()
	}
	hostsWg.Wait()
	if failed.Load() {
		// A partially refreshed cluster is not safe to consume: callers could
		// otherwise continue running tasks against only the nodes whose old or
		// rotated TLS material happened to load successfully.
		return false
	}
	current, err = svc.clusterSvc.GetCluster(ctx, c.ID.String())
	if err != nil || current.ConnectionGeneration != c.ConnectionGeneration || current.LifecycleEpoch != c.LifecycleEpoch {
		logger.Info(ctx, "Discarding configuration from a no-longer-active connection generation",
			"loaded_generation", c.ConnectionGeneration,
			"error", err,
		)
		return false
	}
	if !svc.publishConfig(c.ID, c.ConnectionGeneration, c.LifecycleEpoch, revision, clusterConfig) {
		logger.Info(ctx, "Discarding stale cluster config refresh", "revision", revision)
		return false
	}

	return true
}

func (svc *Service) invalidateOlderConfig(clusterID uuid.UUID, revision uint64) {
	key := clusterID.String()
	for {
		raw, ok := svc.configs.Load(key)
		if !ok {
			return
		}
		entry, ok := raw.(*clusterConfigEntry)
		if !ok {
			panic("cluster config cache stores unexpected type")
		}
		if entry.revision >= revision {
			return
		}
		if svc.configs.CompareAndDelete(key, raw) {
			return
		}
	}
}

func (svc *Service) publishConfig(clusterID, generation uuid.UUID, lifecycleEpoch int64, revision uint64, configs *sync.Map) bool {
	key := clusterID.String()
	candidate := &clusterConfigEntry{
		revision:             revision,
		connectionGeneration: generation,
		lifecycleEpoch:       lifecycleEpoch,
		configs:              configs,
	}
	for svc.currentRevision(clusterID) == revision {
		raw, loaded := svc.configs.LoadOrStore(key, candidate)
		if !loaded {
			if svc.currentRevision(clusterID) == revision {
				return true
			}
			svc.configs.CompareAndDelete(key, candidate)
			return false
		}
		entry, ok := raw.(*clusterConfigEntry)
		if !ok {
			panic("cluster config cache stores unexpected type")
		}
		if entry.revision > revision {
			return false
		}
		if svc.configs.CompareAndSwap(key, raw, candidate) {
			if svc.currentRevision(clusterID) == revision {
				return true
			}
			svc.configs.CompareAndDelete(key, candidate)
			return false
		}
	}
	return false
}

func (svc *Service) nextRevision(clusterID uuid.UUID) uint64 {
	key := clusterID.String()
	v, _ := svc.revisions.LoadOrStore(key, &atomic.Uint64{})
	return v.(*atomic.Uint64).Add(1)
}

func (svc *Service) currentRevision(clusterID uuid.UUID) uint64 {
	v, ok := svc.revisions.Load(clusterID.String())
	if !ok {
		return 0
	}
	return v.(*atomic.Uint64).Load()
}

func (svc *Service) updateAll(ctx context.Context) {
	clusters, err := svc.clusterSvc.ListClusters(ctx, &cluster.Filter{})
	if err != nil {
		svc.logger.Error(ctx, "Couldn't list clusters", "error", err)
		// An unavailable authoritative list cannot justify retaining any cached
		// credential-bearing snapshot. Background refresh is fail closed.
		svc.removeClustersAbsentFrom(nil)
		return
	}
	active := make(map[string]struct{}, len(clusters))
	for _, c := range clusters {
		active[c.ID.String()] = struct{}{}
	}
	svc.removeClustersAbsentFrom(active)

	clustersWg := sync.WaitGroup{}
	for _, c := range clusters {
		clustersWg.Go(func() {
			// Refetch by ID so a delayed background pass cannot publish a
			// pre-rotation Cluster snapshot after a newer forced refresh.
			svc.ForceUpdateCluster(ctx, c.ID)
		})
	}
	clustersWg.Wait()
}

func (svc *Service) removeClustersAbsentFrom(active map[string]struct{}) {
	svc.configs.Range(func(key, _ any) bool {
		clusterKey, ok := key.(string)
		if !ok {
			svc.configs.Delete(key)
			return true
		}
		if _, keep := active[clusterKey]; keep {
			return true
		}
		id, err := uuid.Parse(clusterKey)
		if err != nil {
			svc.configs.Delete(key)
			return true
		}
		svc.RemoveCluster(id)
		return true
	})
}

func (svc *Service) readClusterConfig(clusterID uuid.UUID) (*sync.Map, error) {
	emptyConfig := &sync.Map{}

	rawClusterConfig, ok := svc.configs.Load(clusterID.String())
	if !ok {
		return emptyConfig, ErrNoClusterConfig
	}
	entry, ok := rawClusterConfig.(*clusterConfigEntry)
	if !ok {
		panic("cluster cache emptyConfig stores unexpected type")
	}
	return entry.configs, nil
}

func (svc *Service) retrieveNodeConfig(ctx context.Context, host string, client *scyllaclient.Client,
	c *cluster.Cluster,
) (NodeConfig, error) {
	config := NodeConfig{}

	nodeInfoResp, err := client.NodeInfo(ctx, host)
	if err != nil {
		return config, errors.Wrap(err, "retrieve cluster host configuration")
	}
	dc, err := client.HostDatacenter(ctx, host)
	if err != nil {
		return config, errors.Wrap(err, "retrieve host Datacenter info")
	}
	rack, err := client.HostRack(ctx, host)
	if err != nil {
		return config, errors.Wrap(err, "retrieve host Rack info")
	}

	config, err = NewNodeConfig(c, nodeInfoResp, host, dc, rack)
	if err != nil {
		return config, errors.Wrap(err, "retrieve cluster host configuration")
	}

	return config, nil
}
