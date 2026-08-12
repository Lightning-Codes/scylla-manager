// Copyright (C) 2026 ScyllaDB

package healthcheck

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

type rejectedGenerationCache struct {
	readCalls int
}

func (c *rejectedGenerationCache) Read(uuid.UUID, string) (configcache.NodeConfig, error) {
	c.readCalls++
	return configcache.NodeConfig{}, errors.New("unexpected read")
}

func (*rejectedGenerationCache) ReadAll(uuid.UUID) (map[string]configcache.NodeConfig, error) {
	return nil, errors.New("unexpected read all")
}

func (*rejectedGenerationCache) AvailableHosts(context.Context, uuid.UUID) ([]string, error) {
	return nil, errors.New("authoritative connection generation was deleted")
}

func (*rejectedGenerationCache) ForceUpdateCluster(context.Context, uuid.UUID, ...string) bool {
	return false
}
func (*rejectedGenerationCache) RemoveCluster(uuid.UUID) {}
func (*rejectedGenerationCache) Init(context.Context)    {}
func (*rejectedGenerationCache) Run(context.Context)     {}

func TestRunnerRemoteDeletionPerformsNoDataPlaneProbe(t *testing.T) {
	generation := uuid.MustRandom()
	client, err := scyllaclient.NewClient(scyllaclient.Config{
		ConnectionGeneration: generation,
		Hosts:                []string{"127.0.0.1"},
		Port:                 "10001",
	}, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	cache := new(rejectedGenerationCache)
	pingCalls := 0
	r := runner{
		logger:       log.NewDevelopment(),
		configCache:  cache,
		scyllaClient: func(context.Context, uuid.UUID) (*scyllaclient.Client, error) { return client, nil },
		timeout:      time.Second,
		metrics:      &runnerMetrics{status: cqlStatus, rtt: cqlRTT},
		ping: func(context.Context, uuid.UUID, string, time.Duration, configcache.NodeConfig) (time.Duration, error) {
			pingCalls++
			return 0, nil
		},
	}
	if err := r.Run(context.Background(), uuid.MustRandom(), uuid.Nil, uuid.Nil, json.RawMessage(`{}`)); err == nil {
		t.Fatal("remote tombstone was not surfaced")
	}
	if cache.readCalls != 0 || pingCalls != 0 {
		t.Fatalf("remote tombstone reached cached credentials or network probe: reads=%d pings=%d", cache.readCalls, pingCalls)
	}
}

type mixedGenerationCache struct {
	host       string
	generation uuid.UUID
}

func (c *mixedGenerationCache) Read(uuid.UUID, string) (configcache.NodeConfig, error) {
	return configcache.NodeConfig{ConnectionGeneration: c.generation}, nil
}

func (*mixedGenerationCache) ReadAll(uuid.UUID) (map[string]configcache.NodeConfig, error) {
	return nil, errors.New("unexpected read all")
}

func (c *mixedGenerationCache) AvailableHosts(context.Context, uuid.UUID) ([]string, error) {
	return []string{c.host}, nil
}

func (*mixedGenerationCache) ForceUpdateCluster(context.Context, uuid.UUID, ...string) bool {
	return false
}
func (*mixedGenerationCache) RemoveCluster(uuid.UUID) {}
func (*mixedGenerationCache) Init(context.Context)    {}
func (*mixedGenerationCache) Run(context.Context)     {}

func TestRunnerRejectsNodeConfigFromAnotherGenerationBeforeProbe(t *testing.T) {
	a := uuid.MustRandom()
	b := uuid.MustRandom()
	client, err := scyllaclient.NewClient(scyllaclient.Config{
		ConnectionGeneration: a,
		Hosts:                []string{"127.0.0.1"},
		Port:                 "10001",
	}, log.NewDevelopment())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	pingCalls := 0
	r := runner{
		logger:       log.NewDevelopment(),
		configCache:  &mixedGenerationCache{host: "127.0.0.1", generation: b},
		scyllaClient: func(context.Context, uuid.UUID) (*scyllaclient.Client, error) { return client, nil },
		timeout:      time.Second,
		metrics:      &runnerMetrics{status: cqlStatus, rtt: cqlRTT},
		ping: func(context.Context, uuid.UUID, string, time.Duration, configcache.NodeConfig) (time.Duration, error) {
			pingCalls++
			return 0, nil
		},
	}

	if err := r.Run(context.Background(), uuid.MustRandom(), uuid.Nil, uuid.Nil, json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	if pingCalls != 0 {
		t.Fatalf("mixed generation reached network probe: calls=%d", pingCalls)
	}
}
