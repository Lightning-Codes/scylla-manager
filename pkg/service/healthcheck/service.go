// Copyright (C) 2017 ScyllaDB

package healthcheck

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
	"golang.org/x/sync/errgroup"

	"github.com/scylladb/scylla-manager/v3/pkg/ping"
	"github.com/scylladb/scylla-manager/v3/pkg/ping/cqlping"
	"github.com/scylladb/scylla-manager/v3/pkg/ping/dynamoping"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/secrets"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	"github.com/scylladb/scylla-manager/v3/pkg/util/parallel"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// Service manages health checks.
type Service struct {
	config          Config
	scyllaClient    scyllaclient.ProviderFunc
	secretsStore    store.Store
	clusterProvider cluster.ProviderFunc
	configCache     configcache.ConfigCacher

	logger log.Logger
}

func NewService(config Config, scyllaClient scyllaclient.ProviderFunc, secretsStore store.Store,
	clusterProvider cluster.ProviderFunc, configCache configcache.ConfigCacher, logger log.Logger,
) (*Service, error) {
	if scyllaClient == nil {
		return nil, errors.New("invalid scylla provider")
	}

	return &Service{
		config:          config,
		scyllaClient:    scyllaClient,
		secretsStore:    secretsStore,
		clusterProvider: clusterProvider,
		configCache:     configCache,
		logger:          logger,
	}, nil
}

func (s *Service) Runner() Runner {
	return Runner{
		cql: runner{
			logger:       s.logger.Named("CQL healthcheck"),
			configCache:  s.configCache,
			scyllaClient: s.scyllaClient,
			timeout:      s.config.MaxTimeout,
			metrics: &runnerMetrics{
				status: cqlStatus,
				rtt:    cqlRTT,
			},
			ping:      s.pingCQL,
			pingAgent: s.pingAgent,
		},
		rest: runner{
			logger:       s.logger.Named("REST healthcheck"),
			configCache:  s.configCache,
			scyllaClient: s.scyllaClient,
			timeout:      s.config.MaxTimeout,
			metrics: &runnerMetrics{
				status: restStatus,
				rtt:    restRTT,
			},
			ping:      s.pingREST,
			pingAgent: s.pingAgent,
		},
		alternator: runner{
			logger:       s.logger.Named("Alternator healthcheck"),
			configCache:  s.configCache,
			scyllaClient: s.scyllaClient,
			timeout:      s.config.MaxTimeout,
			metrics: &runnerMetrics{
				status: alternatorStatus,
				rtt:    alternatorRTT,
			},
			ping:      s.pingAlternator,
			pingAgent: s.pingAgent,
		},
	}
}

// Status returns the current status of the supplied cluster.
func (s *Service) Status(ctx context.Context, clusterID uuid.UUID) ([]NodeStatus, error) {
	s.logger.Debug(ctx, "Status", "cluster_id", clusterID)

	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return nil, errors.Wrap(err, "get client")
	}

	status, err := client.Status(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "status")
	}

	out := makeNodeStatus(status)

	g := new(errgroup.Group)
	g.Go(s.parallelAlternatorPingFunc(ctx, clusterID, status, out))
	g.Go(s.parallelCQLPingFunc(ctx, clusterID, status, out))
	g.Go(s.parallelRESTPingFunc(ctx, clusterID, status, out))
	g.Go(s.parallelNodeInfoFunc(ctx, clusterID, status, out))

	return out, g.Wait()
}

func (s *Service) parallelNodeInfoFunc(ctx context.Context, clusterID uuid.UUID, status scyllaclient.NodeStatusInfoSlice, out []NodeStatus) func() error {
	return func() error {
		return parallel.Run(len(status), parallel.NoLimit, func(i int) error {
			// Ignore check if node is not Un and Normal
			if !status[i].IsUN() {
				return nil
			}

			ni, err := s.configCache.Read(clusterID, status[i].Addr)
			if err != nil {
				s.logger.Error(ctx, "Node info fetch failed",
					"cluster_id", clusterID,
					"host", status[i].Addr,
					"error", err,
				)
			}
			if ni.NodeInfo != nil {
				s.decorateNodeStatus(&out[i], ni)
			}
			return nil
		}, parallel.NopNotify)
	}
}

func (s *Service) parallelRESTPingFunc(ctx context.Context, clusterID uuid.UUID, status scyllaclient.NodeStatusInfoSlice, out []NodeStatus) func() error {
	return func() error {
		return parallel.Run(len(status), parallel.NoLimit, func(i int) error {
			o := &out[i]

			// Ignore check if node is not Un and Normal
			if !status[i].IsUN() {
				return nil
			}

			rtt := time.Duration(0)
			ni, err := s.configCache.Read(clusterID, status[i].Addr)
			if err == nil {
				rtt, err = s.pingREST(ctx, clusterID, status[i].Addr, s.config.MaxTimeout, ni)
				// The proxied REST endpoint is protected by the Agent bearer
				// token. Only its success proves both the strict TLS transport
				// and an authenticated request over that transport.
				o.AgentTLSVerified = err == nil
			}

			o.RESTRtt = float64(rtt.Milliseconds())
			if err != nil {
				s.logger.Error(ctx, "REST ping failed",
					"cluster_id", clusterID,
					"host", status[i].Addr,
					"error", err,
				)
				switch {
				case rtt == 0:
					o.RESTStatus = statusError
					o.RESTCause = "Agent REST probe failed; see Manager logs with the request trace ID"
				case errors.Is(err, context.DeadlineExceeded):
					o.RESTStatus = statusTimeout
				case scyllaclient.StatusCodeOf(err) == http.StatusUnauthorized:
					o.RESTStatus = statusUnauthorized
				case scyllaclient.StatusCodeOf(err) != 0:
					o.RESTStatus = fmt.Sprintf("%s %d", statusHTTP, scyllaclient.StatusCodeOf(err))
				default:
					o.RESTStatus = statusDown
					o.RESTCause = "Agent REST endpoint is unavailable; see Manager logs with the request trace ID"
				}
			} else {
				o.RESTStatus = statusUp
			}

			return nil
		}, parallel.NopNotify)
	}
}

func (s *Service) parallelCQLPingFunc(ctx context.Context, clusterID uuid.UUID, status scyllaclient.NodeStatusInfoSlice, out []NodeStatus) func() error {
	return func() error {
		return parallel.Run(len(status), parallel.NoLimit, func(i int) error {
			o := &out[i]

			// Ignore check if node is not Un and Normal.
			if !status[i].IsUN() {
				return nil
			}

			rtt := time.Duration(0)
			authVerified := false
			ni, err := s.configCache.Read(clusterID, status[i].Addr)
			if err == nil {
				rtt, authVerified, err = s.pingCQLVerified(ctx, clusterID, status[i].Addr, s.config.MaxTimeout, ni)
			}

			o.CQLRtt = float64(rtt.Milliseconds())
			if err != nil {
				s.logger.Error(ctx, "CQL ping failed",
					"cluster_id", clusterID,
					"host", status[i].Addr,
					"error", err,
				)

				o := &out[i]
				switch {
				case rtt == 0:
					o.CQLStatus = statusError
					o.CQLCause = "CQL probe failed; see Manager logs with the request trace ID"
				case errors.Is(err, ping.ErrTimeout):
					o.CQLStatus = statusTimeout
				case errors.Is(err, ping.ErrUnauthorised):
					o.CQLStatus = statusUnauthorized
				default:
					o.CQLStatus = statusDown
					o.CQLCause = "CQL endpoint is unavailable; see Manager logs with the request trace ID"
				}
			} else {
				o.CQLStatus = statusUp
				o.CQLTLSVerified = ni.CQLTLSConfig() != nil
				o.CQLAuthVerified = authVerified
			}

			o.SSL = o.CQLTLSVerified

			return nil
		}, parallel.NopNotify)
	}
}

func (s *Service) parallelAlternatorPingFunc(ctx context.Context, clusterID uuid.UUID,
	status scyllaclient.NodeStatusInfoSlice, out []NodeStatus,
) func() error {
	return func() error {
		return parallel.Run(len(status), parallel.NoLimit, func(i int) error {
			o := &out[i]

			// Ignore check if node is not Un and Normal.
			if !status[i].IsUN() {
				return nil
			}

			rtt := time.Duration(0)
			ni, err := s.configCache.Read(clusterID, status[i].Addr)
			if err == nil {
				rtt, err = s.pingAlternator(ctx, clusterID, status[i].Addr, s.config.MaxTimeout, ni)
			}

			if err != nil {
				s.logger.Error(ctx, "Alternator ping failed",
					"cluster_id", clusterID,
					"host", status[i].Addr,
					"error", err,
				)

				switch {
				case rtt == 0:
					o.AlternatorStatus = statusError
					o.AlternatorCause = "Alternator probe failed; see Manager logs with the request trace ID"
				case errors.Is(err, ping.ErrTimeout):
					o.AlternatorStatus = statusTimeout
				case errors.Is(err, ping.ErrUnauthorised):
					o.AlternatorStatus = statusUnauthorized
				default:
					o.AlternatorStatus = statusDown
					o.AlternatorCause = "Alternator endpoint is unavailable; see Manager logs with the request trace ID"
				}
			} else if rtt != 0 {
				o.AlternatorStatus = statusUp
				o.AlternatorTLSVerified = ni.AlternatorTLSConfig() != nil
				o.AlternatorAuthVerified = ni.NodeInfo != nil && ni.AlternatorEnforceAuthorization
			}
			if rtt != 0 {
				o.AlternatorRtt = float64(rtt.Milliseconds())
			}

			return nil
		}, parallel.NopNotify)
	}
}

// pingAlternator sends ping probe and returns RTT.
// When Alternator frontend is disabled, it returns 0 and nil error.
func (s *Service) pingAlternator(ctx context.Context, clusterID uuid.UUID, host string, timeout time.Duration, ni configcache.NodeConfig) (rtt time.Duration, err error) {
	if !ni.AlternatorEnabled() {
		return 0, nil
	}

	addr := ni.AlternatorAddr(host)
	config := dynamoping.Config{
		Addr:                   addr,
		Timeout:                timeout,
		RequiresAuthentication: ni.AlternatorEnforceAuthorization,
	}
	tlsConfig := ni.AlternatorTLSConfig()
	if config.RequiresAuthentication && tlsConfig == nil {
		return 0, errors.New("Alternator authentication requires verified TLS; refusing to transmit credentials over plaintext")
	}
	if config.RequiresAuthentication {
		creds := &secrets.AlternatorCreds{ClusterID: clusterID}
		if err := s.secretsStore.Get(creds); err != nil {
			return 0, errors.Wrap(err, "load Alternator credentials")
		}
		config.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: creds.AccessKeyID, SecretAccessKey: creds.SecretAccessKey}, nil
		})
	}

	if tlsConfig != nil {
		config.TLSConfig = tlsConfig.Clone()
	}

	return dynamoping.QueryPing(ctx, config)
}

func (s *Service) decorateNodeStatus(status *NodeStatus, ni configcache.NodeConfig) {
	status.TotalRAM = ni.MemoryTotal
	status.Uptime = ni.Uptime
	status.CPUCount = ni.CPUCount
	status.ScyllaVersion = ni.ScyllaVersion
	status.AgentVersion = ni.AgentVersion
}

func (s *Service) pingCQL(ctx context.Context, clusterID uuid.UUID, host string, timeout time.Duration, ni configcache.NodeConfig) (rtt time.Duration, err error) {
	rtt, _, err = s.pingCQLVerified(ctx, clusterID, host, timeout, ni)
	return rtt, err
}

func (s *Service) pingCQLVerified(ctx context.Context, clusterID uuid.UUID, host string, timeout time.Duration, ni configcache.NodeConfig) (rtt time.Duration, authVerified bool, err error) {
	if ni.NodeInfo == nil {
		return 0, false, errors.New("CQL node configuration is unavailable")
	}
	cluster, err := s.clusterProvider(ctx, clusterID)
	if err != nil {
		return 0, false, errors.Wrap(err, "cluster provider")
	}
	// Try to connect directly to host address.
	config := cqlping.Config{
		Addr:    ni.CQLAddr(host, cluster.ForceTLSDisabled || cluster.ForceNonSSLSessionPort),
		Timeout: timeout,
	}

	tlsConfig := ni.CQLTLSConfig()
	if (ni.CqlPasswordProtected || ni.ClientEncryptionRequireAuth) && tlsConfig == nil {
		return 0, false, errors.New("CQL authentication requires verified TLS; refusing to transmit credentials over plaintext")
	}
	if tlsConfig != nil {
		config.Addr = tlsConfig.Address
		config.TLSConfig = tlsConfig.Clone()
	}
	if tlsConfig == nil {
		logger := s.logger.With("cluster_id", clusterID, "host", host)
		rtt, err = cqlping.NativeCQLPing(ctx, config, logger)
		return rtt, false, err
	}

	credentials := &secrets.CQLCreds{ClusterID: clusterID}
	credentialsErr := s.secretsStore.Get(credentials)
	if ni.CqlPasswordProtected && credentialsErr != nil {
		return 0, false, errors.Wrap(credentialsErr, "load required CQL credentials")
	}
	if credentialsErr == nil {
		rtt, err = cqlping.QueryPing(ctx, config, credentials.Username, credentials.Password)
		return rtt, cqlAuthenticationVerified(ni, tlsConfig != nil, err), err
	}
	if !errors.Is(credentialsErr, util.ErrNotFound) {
		return 0, false, errors.Wrap(credentialsErr, "load CQL credentials")
	}
	logger := s.logger.With("cluster_id", clusterID, "host", host)
	rtt, err = cqlping.NativeCQLPing(ctx, config, logger)

	return rtt, cqlAuthenticationVerified(ni, tlsConfig != nil, err), err
}

func cqlAuthenticationVerified(ni configcache.NodeConfig, tlsVerified bool, err error) bool {
	return err == nil && tlsVerified && (ni.CqlPasswordProtected || ni.ClientEncryptionRequireAuth)
}

func (s *Service) pingREST(ctx context.Context, clusterID uuid.UUID, host string, timeout time.Duration, _ configcache.NodeConfig) (time.Duration, error) {
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return 0, errors.Wrapf(err, "get client for cluster with id %s", clusterID)
	}

	return client.Ping(ctx, host, timeout)
}

func (s *Service) pingAgent(ctx context.Context, clusterID uuid.UUID, host string, timeout time.Duration) (time.Duration, error) {
	c, err := s.clusterProvider(ctx, clusterID)
	if err != nil {
		return 0, errors.Wrap(err, "cluster provider")
	}
	if c.AuthToken == "" {
		return 0, errors.New("Agent authentication is not configured")
	}
	client, err := s.scyllaClient(ctx, clusterID)
	if err != nil {
		return 0, errors.Wrapf(err, "get client for cluster with id %s", clusterID)
	}

	return client.PingAgent(ctx, host, timeout)
}
