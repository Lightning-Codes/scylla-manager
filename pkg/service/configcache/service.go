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
	"github.com/scylladb/scylla-manager/v3/pkg/store"
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
	secretsStore store.Store

	configs *sync.Map
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
	revision uint64
	configs  *sync.Map
}

// NewService is the constructor for the cluster config cache service.
func NewService(config Config, clusterSvc cluster.Servicer, client scyllaclient.ProviderFunc, secretsStore store.Store, logger log.Logger) ConfigCacher {
	return &Service{
		svcConfig:    config,
		clusterSvc:   clusterSvc,
		scyllaClient: client,
		secretsStore: secretsStore,
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
	revision := svc.beginRefresh(clusterID)

	c, err := svc.clusterSvc.GetCluster(ctx, clusterID.String())
	if err != nil {
		logger.Error(ctx, "Update failed", "error", err)
		return false
	}

	return svc.updateSingle(ctx, c, revision, hosts...)
}

// AvailableHosts returns list of hosts of given cluster that keep their configuration in cache.
func (svc *Service) AvailableHosts(ctx context.Context, clusterID uuid.UUID) ([]string, error) {
	logger := svc.logger.Named("Listing available hosts").With("cluster", clusterID)

	clusterConfig, err := svc.readClusterConfig(clusterID)
	if err != nil {
		return nil, err
	}

	var availableHosts []string
	clusterConfig.Range(func(key, _ any) bool {
		host, ok := key.(string)
		if !ok {
			logger.Error(ctx, "Cannot cast to string", "host", key, "error", err)
			return false
		}
		availableHosts = append(availableHosts, host)
		return true
	})

	return availableHosts, nil
}

func (svc *Service) updateSingle(ctx context.Context, c *cluster.Cluster, revision uint64, hosts ...string) bool {
	logger := svc.logger.Named("Cluster config update").With("cluster", c.ID)
	clusterConfig := &sync.Map{}

	client, err := svc.scyllaClient(ctx, c.ID)
	if err != nil {
		logger.Error(ctx, "Couldn't create scylla client", "cluster", c.ID, "error", err)
		return false
	}
	defer func() {
		if err := client.Close(); err != nil {
			logger.Error(ctx, "Couldn't close HTTP client", "error", err)
		}
	}()

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
	if !svc.publishConfig(c.ID, revision, clusterConfig) {
		logger.Info(ctx, "Discarding stale cluster config refresh", "revision", revision)
		return false
	}

	return true
}

func (svc *Service) beginRefresh(clusterID uuid.UUID) uint64 {
	revision := svc.nextRevision(clusterID)
	// Remove the previous material before reading either the cluster row or its
	// rotated secrets. A failed refresh must never retain stale trust/identity.
	svc.invalidateOlderConfig(clusterID, revision)
	return revision
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

func (svc *Service) publishConfig(clusterID uuid.UUID, revision uint64, configs *sync.Map) bool {
	key := clusterID.String()
	candidate := &clusterConfigEntry{revision: revision, configs: configs}
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
		return
	}

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
	if entry.revision != svc.currentRevision(clusterID) {
		return emptyConfig, ErrNoClusterConfig
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

	config, err = NewNodeConfig(c, nodeInfoResp, svc.secretsStore, host, dc, rack)
	if err != nil {
		return config, errors.Wrap(err, "retrieve cluster host configuration")
	}

	return config, nil
}
