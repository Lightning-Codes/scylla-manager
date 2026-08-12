// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	api "github.com/go-openapi/runtime/client"
	apiMiddleware "github.com/go-openapi/runtime/middleware"
	"github.com/go-openapi/strfmt"
	"github.com/hailocab/go-hostpool"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"

	"github.com/scylladb/scylla-manager/v3/pkg/auth"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	agentClient "github.com/scylladb/scylla-manager/v3/swagger/gen/agent/client"
	agentOperations "github.com/scylladb/scylla-manager/v3/swagger/gen/agent/client/operations"
	scyllaClient "github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/client"
	scyllaOperations "github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/client/operations"
)

var setOpenAPIGlobalsOnce sync.Once

func setOpenAPIGlobals() {
	setOpenAPIGlobalsOnce.Do(func() {
		// Timeout is defined in http client that we provide in api.NewWithClient.
		// If Context is provided to operation, which is always the case here,
		// this value has no meaning since OpenAPI runtime ignores it.
		api.DefaultTimeout = 0
		// Disable debug output to stderr, it could have been enabled by setting
		// SWAGGER_DEBUG or DEBUG env variables.
		apiMiddleware.Debug = false
	})
}

//go:generate ../rclone/rcserver/internalgen.sh

// DefaultTransport returns a new http.Transport with similar default values to
// http.DefaultTransport. Do not use this for transient transports as it can
// leak file descriptors over time. Only use this for transports that will be
// re-used for the same host(s).
func DefaultTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConnsPerHost:   100,
	}
}

// Client provides means to interact with Scylla nodes.
type Client struct {
	config Config
	logger log.Logger

	scyllaOps scyllaOperations.ClientService
	agentOps  agentOperations.ClientService
	client    retryableClient
	hostPool  hostpool.HostPool
	transport *revocableTransport
	closeOnce sync.Once

	mu      sync.RWMutex
	dcCache map[string]string
}

// NewClient creates new scylla HTTP client.
func NewClient(config Config, logger log.Logger) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid config")
	}
	setOpenAPIGlobals()

	// Copy hosts
	hosts := make([]string, len(config.Hosts))
	copy(hosts, config.Hosts)

	pool := hostpool.NewEpsilonGreedy(hosts, config.PoolDecayDuration, &hostpool.LinearEpsilonValueCalculator{})

	if config.Transport == nil {
		config.Transport = DefaultTransport()
	}
	transport := config.Transport
	transport = timeout(transport, config.Timeout)
	transport = requestLogger(transport, logger)
	transport = hostPool(transport, pool, config.Port)
	transport = auth.AddToken(transport, config.AuthToken)
	transport = fixContentType(transport)

	revocable := newRevocableTransport(transport)
	client := &http.Client{Transport: revocable}

	scyllaRuntime := api.NewWithClient(
		scyllaClient.DefaultHost, scyllaClient.DefaultBasePath, []string{config.Scheme}, client,
	)
	agentRuntime := api.NewWithClient(
		agentClient.DefaultHost, agentClient.DefaultBasePath, []string{config.Scheme}, client,
	)

	// Debug can be turned on by SWAGGER_DEBUG or DEBUG env variable
	scyllaRuntime.Debug = false
	agentRuntime.Debug = false

	rc := newRetryConfig(config)
	scyllaOps := scyllaOperations.New(retryableWrapTransport(scyllaRuntime, rc, logger), strfmt.Default)
	agentOps := agentOperations.New(retryableWrapTransport(agentRuntime, rc, logger), strfmt.Default)

	return &Client{
		config:    config,
		logger:    logger,
		scyllaOps: scyllaOps,
		agentOps:  agentOps,
		hostPool:  pool,
		client:    retryableWrapClient(client, rc, logger),
		transport: revocable,
		dcCache:   make(map[string]string),
	}, nil
}

// Config returns a copy of client config.
func (c *Client) Config() Config {
	return c.config
}

// ErrClientClosed is returned when an invalidated Client is used again.
var ErrClientClosed = errors.New("scylla client is closed")

type revocableTransport struct {
	closed atomic.Bool
	next   http.RoundTripper
}

func newRevocableTransport(next http.RoundTripper) *revocableTransport {
	return &revocableTransport{next: next}
}

func (t *revocableTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.closed.Load() {
		return nil, ErrClientClosed
	}
	return t.next.RoundTrip(req)
}

func (t *revocableTransport) Close() {
	t.closed.Store(true)
}

// Close revokes the client for all current holders and closes idle
// connections. Requests already in flight are allowed to complete, while any
// request starting after Close fails before reaching the network.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		if c.transport != nil {
			c.transport.Close()
		}
		if t, ok := c.config.Transport.(*http.Transport); ok {
			t.CloseIdleConnections()
		}
		if c.hostPool != nil {
			c.hostPool.Close()
		}
	})
	return nil
}

// fixContentType adjusts Scylla REST API response so that it can be consumed
// by Open API.
func fixContentType(next http.RoundTripper) http.RoundTripper {
	return httpx.RoundTripperFunc(func(req *http.Request) (resp *http.Response, err error) {
		defer func() {
			if resp != nil {
				// Force JSON, Scylla returns "text/plain" that misleads the
				// unmarshaller and breaks processing.
				resp.Header.Set("Content-Type", "application/json")
			}
		}()
		return next.RoundTrip(req)
	})
}
