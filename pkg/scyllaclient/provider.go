// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/util/logutil"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// ProviderFunc is a function that returns a Client for a given cluster.
type ProviderFunc func(ctx context.Context, clusterID uuid.UUID) (*Client, error)

type clientTTL struct {
	mu       sync.Mutex
	client   *Client
	ttl      time.Time // time after which client is invalid
	hostsTTL time.Time // time after which client hosts needs to be validated
	revoked  bool
}

const hostsValidity = 15 * time.Second

// ErrCachedHostsChanged asks the generation-aware cluster provider to rotate
// KnownHosts through the immutable bundle before constructing another client.
var ErrCachedHostsChanged = errors.New("cached Agent hosts changed")

// isValid checks if client can be safely returned from cache.
// Client is invalid when it reaches the end of TTL, or when its hosts changed.
// In order to reduce API calls under mutex when creating many clients
// (e.g. when healthcheck svc runs pingREST for every node),
// checking for changed hosts is done only every hostsValidity.
func (c *clientTTL) isValid(ctx context.Context) (bool, error) {
	// Check client TTL (if set)
	if c.ttl.IsZero() || c.ttl.Before(timeutc.Now()) {
		return false, nil
	}
	// Check hosts TTL and refresh if they didn't change
	if c.hostsTTL.Before(timeutc.Now()) {
		changed, err := c.client.CheckHostsChanged(ctx)
		switch {
		case err != nil:
			return false, errors.Wrap(err, "check if client's hosts changed")
		case changed:
			return false, ErrCachedHostsChanged
		default:
			c.hostsTTL = timeutc.Now().Add(hostsValidity)
		}
	}
	return true, nil
}

// CachedProvider is a provider implementation that reuses clients.
type CachedProvider struct {
	inner             ProviderFunc
	validity          time.Duration
	clients           map[uuid.UUID]*clientTTL
	generationClients map[clientGenerationKey]*clientTTL
	clusterStates     map[uuid.UUID]*clusterGenerationState
	mu                sync.Mutex
	logger            log.Logger
}

type clusterGenerationState struct {
	revoked               bool
	provisionalRevocation bool
	deletedGeneration     uuid.UUID
	highestLifecycleEpoch int64
	lifecycleVersion      uint64
	allowed               map[uuid.UUID]struct{}
	retiredGenerations    map[uuid.UUID]struct{}
}

type clientGenerationKey struct {
	clusterID  uuid.UUID
	generation uuid.UUID
}

func NewCachedProvider(f ProviderFunc, cacheInvalidationTimeout time.Duration, logger log.Logger) *CachedProvider {
	return &CachedProvider{
		inner:             f,
		validity:          cacheInvalidationTimeout,
		clients:           make(map[uuid.UUID]*clientTTL),
		generationClients: make(map[clientGenerationKey]*clientTTL),
		clusterStates:     make(map[uuid.UUID]*clusterGenerationState),
		logger:            logger.Named("cache-provider"),
	}
}

// Client is the cached ProviderFunc.
func (p *CachedProvider) Client(ctx context.Context, clusterID uuid.UUID) (*Client, error) {
	c := p.getClientTTL(clusterID)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.revoked {
		return nil, ErrClientClosed
	}

	if valid, err := c.isValid(ctx); err != nil && !errors.Is(err, ErrCachedHostsChanged) {
		p.logger.Error(ctx, "Cannot check client validity", "error", err)
	} else if valid {
		return c.client, nil
	}

	// Revoke the previous generation before attempting a replacement. Old
	// callers must not retain a transport that later invalidations can no
	// longer see, and an inner-provider failure must leave the cell fail-closed.
	previous := c.client
	c.client = nil
	c.ttl = time.Time{}
	c.hostsTTL = time.Time{}
	if previous != nil {
		logutil.LogOnError(ctx, p.logger, previous.Close, "Couldn't close replaced scylla client")
	}

	// If not found or invalid, create a new one
	client, err := p.inner(ctx, clusterID)
	if err != nil {
		return nil, err
	}

	c.client = client
	c.ttl = timeutc.Now().Add(p.validity)
	c.hostsTTL = timeutc.Now().Add(hostsValidity)
	return c.client, nil
}

// ClientForGeneration caches clients by immutable generation. A reader that
// captured generation A before a concurrent B commit can finish on A without
// replacing or revoking B; callers acquiring after the commit ask only for B.
func (p *CachedProvider) ClientForGeneration(ctx context.Context, clusterID, generation uuid.UUID, create func() (*Client, error)) (*Client, error) {
	return p.ClientForGenerationValidated(ctx, clusterID, generation, create, nil)
}

// ClientForGenerationValidated additionally checks the authoritative active
// pointer immediately before returning a cached client and again after a
// blocking constructor. This lets another Manager replica activate or delete
// a generation without leaving this process's lifecycle cache permanently
// stale.
func (p *CachedProvider) ClientForGenerationValidated(ctx context.Context, clusterID, generation uuid.UUID, create func() (*Client, error), validate func() error) (*Client, error) {
	return p.ClientForGenerationValidatedEpoch(ctx, clusterID, generation, 0, create, validate)
}

// ClientForGenerationValidatedEpoch is the secure lifecycle-aware acquisition
// path. The epoch is obtained from the same globally-serial pointer snapshot as
// generation. A lower epoch can never resurrect a permanently retired ID.
func (p *CachedProvider) ClientForGenerationValidatedEpoch(ctx context.Context, clusterID, generation uuid.UUID, lifecycleEpoch int64, create func() (*Client, error), validate func() error) (*Client, error) {
	c := p.getGenerationClientTTL(clusterID, generation)
	c.mu.Lock()
	defer c.mu.Unlock()
	if validate != nil {
		if err := p.validateStableLifecycle(clusterID, validate); err != nil {
			p.clearCell(ctx, c, false)
			return nil, err
		}
		if !p.validatedGenerationCanActivateEpoch(clusterID, generation, lifecycleEpoch) {
			p.clearCell(ctx, c, true)
			return nil, ErrClientClosed
		}
		p.authorizeValidatedGenerationEpoch(clusterID, generation, lifecycleEpoch)
		c.revoked = false
	}
	if c.revoked || !p.generationAllowed(clusterID, generation) {
		return nil, ErrClientClosed
	}

	if c.client != nil {
		// A connection generation is an immutable, explicitly revoked security
		// snapshot. Long-running backup, restore, and repair workers retain the
		// returned pointer for the duration of their run, so replacing and closing
		// it merely because the generic cache TTL elapsed (or because a topology
		// probe failed transiently) revokes an otherwise-authorized in-flight
		// worker. Keep the same transport until an explicit generation rotation,
		// cluster deletion, host-set change, or provider shutdown revokes it.
		//
		// Host discovery remains periodic. A proven host-set change is returned to
		// the cluster service so it can commit a new immutable generation. A probe
		// error is not proof that the active generation is invalid; the caller can
		// continue using the already-authorized client and normal request retries.
		if c.hostsTTL.Before(timeutc.Now()) {
			changed, err := c.client.CheckHostsChanged(ctx)
			switch {
			case err != nil:
				p.logger.Error(ctx, "Cannot refresh immutable-generation client topology; retaining active client", "error", err)
				c.hostsTTL = timeutc.Now().Add(hostsValidity)
			case changed:
				return nil, ErrCachedHostsChanged
			default:
				c.hostsTTL = timeutc.Now().Add(hostsValidity)
			}
		}
		return c.client, nil
	}
	previous := c.client
	c.client = nil
	c.ttl = time.Time{}
	c.hostsTTL = time.Time{}
	if previous != nil {
		logutil.LogOnError(ctx, p.logger, previous.Close, "Couldn't close replaced scylla client generation")
	}
	client, err := create()
	if err != nil {
		return nil, err
	}
	if validate != nil {
		if err := p.validateStableLifecycle(clusterID, validate); err != nil {
			logutil.LogOnError(ctx, p.logger, client.Close, "Couldn't close client whose generation is no longer active")
			return nil, err
		}
		if !p.validatedGenerationCanActivateEpoch(clusterID, generation, lifecycleEpoch) {
			logutil.LogOnError(ctx, p.logger, client.Close, "Couldn't close client whose generation was retired during validation")
			return nil, ErrClientClosed
		}
		p.authorizeValidatedGenerationEpoch(clusterID, generation, lifecycleEpoch)
		c.revoked = false
	}
	// Recheck after the potentially blocking constructor. DeleteCluster may
	// have permanently revoked the cluster while trust/token were being loaded.
	if c.revoked || !p.generationAllowed(clusterID, generation) {
		logutil.LogOnError(ctx, p.logger, client.Close, "Couldn't close client created after cluster revocation")
		return nil, ErrClientClosed
	}
	if client.Config().ConnectionGeneration != generation {
		logutil.LogOnError(ctx, p.logger, client.Close, "Couldn't close mismatched scylla client generation")
		return nil, errors.Errorf("connection generation changed while creating Agent client: expected %s, got %s", generation, client.Config().ConnectionGeneration)
	}
	if lifecycleEpoch > 0 && client.Config().ConnectionLifecycleEpoch != lifecycleEpoch {
		logutil.LogOnError(ctx, p.logger, client.Close, "Couldn't close mismatched scylla client lifecycle epoch")
		return nil, errors.Errorf("connection lifecycle epoch changed while creating Agent client: expected %d, got %d", lifecycleEpoch, client.Config().ConnectionLifecycleEpoch)
	}
	c.client = client
	c.ttl = timeutc.Now().Add(p.validity)
	c.hostsTTL = timeutc.Now().Add(hostsValidity)
	return client, nil
}

func (p *CachedProvider) validateStableLifecycle(clusterID uuid.UUID, validate func() error) error {
	for {
		p.mu.Lock()
		version := uint64(0)
		if state := p.clusterStates[clusterID]; state != nil {
			version = state.lifecycleVersion
		}
		p.mu.Unlock()
		if err := validate(); err != nil {
			return err
		}
		p.mu.Lock()
		state := p.clusterStates[clusterID]
		stable := (state == nil && version == 0) || (state != nil && state.lifecycleVersion == version)
		p.mu.Unlock()
		if stable {
			return nil
		}
		// A revocation was published while the serial validation was in flight.
		// Repeat so a pre-delete result cannot clear the newer lifecycle state.
	}
}

func (p *CachedProvider) authorizeValidatedGenerationEpoch(clusterID, generation uuid.UUID, lifecycleEpoch int64) {
	if lifecycleEpoch <= 0 {
		p.authorizeValidatedGeneration(clusterID, generation)
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	state := p.ensureClusterStateLocked(clusterID)
	if lifecycleEpoch < state.highestLifecycleEpoch {
		return
	}
	if state.provisionalRevocation {
		state.provisionalRevocation = false
		state.revoked = false
		state.deletedGeneration = uuid.Nil
	}
	if lifecycleEpoch > state.highestLifecycleEpoch || state.revoked {
		for old := range state.allowed {
			state.retiredGenerations[old] = struct{}{}
		}
		if state.deletedGeneration != uuid.Nil {
			state.retiredGenerations[state.deletedGeneration] = struct{}{}
		}
		state.allowed = make(map[uuid.UUID]struct{})
		state.revoked = false
		state.deletedGeneration = uuid.Nil
		state.highestLifecycleEpoch = lifecycleEpoch
	}
	if _, retired := state.retiredGenerations[generation]; !retired {
		state.allowed[generation] = struct{}{}
	}
}

func (p *CachedProvider) validatedGenerationCanActivateEpoch(clusterID, generation uuid.UUID, lifecycleEpoch int64) bool {
	if lifecycleEpoch <= 0 {
		return p.validatedGenerationCanActivate(clusterID, generation)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	state := p.clusterStates[clusterID]
	if state == nil {
		return true
	}
	if lifecycleEpoch < state.highestLifecycleEpoch {
		return false
	}
	if state.revoked && !state.provisionalRevocation {
		return false
	}
	if _, retired := state.retiredGenerations[generation]; retired {
		return false
	}
	// Secure cluster IDs are never reusable after a tombstone. A nondeleted
	// generation at a higher epoch is therefore corrupt/unauthorized state, not
	// a lifecycle to admit. Fresh processes initialize an existing live row
	// through ResetClusterEpoch before serving clients.
	return lifecycleEpoch == state.highestLifecycleEpoch
}

func (p *CachedProvider) ensureClusterStateLocked(clusterID uuid.UUID) *clusterGenerationState {
	state := p.clusterStates[clusterID]
	if state == nil {
		state = &clusterGenerationState{
			allowed:            make(map[uuid.UUID]struct{}),
			retiredGenerations: make(map[uuid.UUID]struct{}),
		}
		p.clusterStates[clusterID] = state
	}
	if state.allowed == nil {
		state.allowed = make(map[uuid.UUID]struct{})
	}
	if state.retiredGenerations == nil {
		state.retiredGenerations = make(map[uuid.UUID]struct{})
	}
	return state
}

func (p *CachedProvider) authorizeValidatedGeneration(clusterID, generation uuid.UUID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := p.clusterStates[clusterID]
	if state == nil {
		state = &clusterGenerationState{
			allowed:            make(map[uuid.UUID]struct{}),
			retiredGenerations: make(map[uuid.UUID]struct{}),
		}
		p.clusterStates[clusterID] = state
	}
	if state.revoked {
		return
	}
	if _, retired := state.retiredGenerations[generation]; retired {
		return
	}
	if state.allowed == nil {
		state.allowed = make(map[uuid.UUID]struct{})
	}
	state.allowed[generation] = struct{}{}
}

func (p *CachedProvider) validatedGenerationCanActivate(clusterID, generation uuid.UUID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := p.clusterStates[clusterID]
	if state == nil {
		return true
	}
	if state.revoked {
		return false
	}
	_, retired := state.retiredGenerations[generation]
	return !retired
}

func (p *CachedProvider) clearCell(ctx context.Context, cell *clientTTL, retire bool) {
	client := cell.client
	cell.client = nil
	cell.ttl = time.Time{}
	cell.hostsTTL = time.Time{}
	if retire {
		cell.revoked = true
	}
	if client != nil {
		logutil.LogOnError(ctx, p.logger, client.Close, "Couldn't close rejected scylla client generation")
	}
}

func (p *CachedProvider) generationAllowed(clusterID, generation uuid.UUID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := p.clusterStates[clusterID]
	if state == nil {
		// Preserve the generic provider's compatibility for callers that do not
		// use the cluster service lifecycle registration.
		return true
	}
	if state.revoked {
		return false
	}
	_, ok := state.allowed[generation]
	return ok
}

// ResetCluster registers a freshly created cluster ID. A revoked ID is
// permanently retired and cannot be reset.
func (p *CachedProvider) ResetCluster(clusterID, generation uuid.UUID) {
	p.mu.Lock()
	if state := p.clusterStates[clusterID]; state != nil && state.revoked {
		p.mu.Unlock()
		return
	}
	p.clusterStates[clusterID] = &clusterGenerationState{
		allowed:            map[uuid.UUID]struct{}{generation: {}},
		retiredGenerations: make(map[uuid.UUID]struct{}),
	}
	p.mu.Unlock()
}

// ResetClusterEpoch authorizes a confirmed active nondeleted lifecycle.
// Generations from lower epochs remain permanently retired.
func (p *CachedProvider) ResetClusterEpoch(clusterID, generation uuid.UUID, lifecycleEpoch int64) {
	p.mu.Lock()
	state := p.ensureClusterStateLocked(clusterID)
	if state.revoked && !state.provisionalRevocation {
		p.mu.Unlock()
		return
	}
	if lifecycleEpoch < state.highestLifecycleEpoch {
		p.mu.Unlock()
		return
	}
	if state.highestLifecycleEpoch != 0 && lifecycleEpoch > state.highestLifecycleEpoch {
		p.mu.Unlock()
		return
	}
	if lifecycleEpoch > state.highestLifecycleEpoch || state.revoked {
		for old := range state.allowed {
			state.retiredGenerations[old] = struct{}{}
		}
		if state.deletedGeneration != uuid.Nil {
			state.retiredGenerations[state.deletedGeneration] = struct{}{}
		}
		state.allowed = make(map[uuid.UUID]struct{})
		state.highestLifecycleEpoch = lifecycleEpoch
		state.revoked = false
		state.provisionalRevocation = false
		state.deletedGeneration = uuid.Nil
	}
	if _, retired := state.retiredGenerations[generation]; !retired {
		state.allowed[generation] = struct{}{}
	}
	p.mu.Unlock()
}

// ActivateGeneration authorizes a newly committed generation without ever
// clearing a cluster deletion tombstone.
func (p *CachedProvider) ActivateGeneration(clusterID, generation uuid.UUID) {
	p.mu.Lock()
	state := p.clusterStates[clusterID]
	if state == nil {
		state = &clusterGenerationState{
			allowed:            make(map[uuid.UUID]struct{}),
			retiredGenerations: make(map[uuid.UUID]struct{}),
		}
		p.clusterStates[clusterID] = state
	}
	if !state.revoked {
		state.allowed[generation] = struct{}{}
	}
	p.mu.Unlock()
}

func (p *CachedProvider) getGenerationClientTTL(clusterID, generation uuid.UUID) *clientTTL {
	key := clientGenerationKey{clusterID: clusterID, generation: generation}
	p.mu.Lock()
	c, ok := p.generationClients[key]
	if !ok {
		c = new(clientTTL)
		p.generationClients[key] = c
	}
	p.mu.Unlock()
	return c
}

// Ensures that clientTTL with given clusterID is present in the cache.
func (p *CachedProvider) getClientTTL(clusterID uuid.UUID) *clientTTL {
	p.mu.Lock()
	c, ok := p.clients[clusterID]
	if !ok {
		c = new(clientTTL)
		p.clients[clusterID] = c
	}
	p.mu.Unlock()
	return c
}

// Invalidate revokes the current client for clusterID while retaining the
// stable cache cell. Keeping one cell prevents a concurrent Client call from
// finishing on a detached entry that no later invalidation can reach.
func (p *CachedProvider) Invalidate(clusterID uuid.UUID) {
	p.mu.Lock()
	c := p.clients[clusterID]
	var generationCells []*clientTTL
	for key, cell := range p.generationClients {
		if key.clusterID == clusterID {
			generationCells = append(generationCells, cell)
		}
	}
	p.mu.Unlock()
	if c != nil {
		generationCells = append(generationCells, c)
	}
	for _, cell := range generationCells {
		cell.mu.Lock()
		client := cell.client
		cell.client = nil
		cell.ttl = time.Time{}
		cell.hostsTTL = time.Time{}
		// Cluster-wide invalidation clears clients but leaves stable cells
		// reusable. Permanent retirement is generation-specific below.
		cell.mu.Unlock()
		if client != nil {
			logutil.LogOnError(context.Background(), p.logger, client.Close, "Couldn't close invalidated scylla client")
		}
	}
}

// RefreshGeneration clears an expired/host-changed client while leaving the
// same active generation authorized for one fresh construction.
func (p *CachedProvider) RefreshGeneration(clusterID, generation uuid.UUID) {
	key := clientGenerationKey{clusterID: clusterID, generation: generation}
	p.mu.Lock()
	cell := p.generationClients[key]
	p.mu.Unlock()
	if cell == nil {
		return
	}
	cell.mu.Lock()
	p.clearCell(context.Background(), cell, false)
	cell.mu.Unlock()
}

// InvalidateGeneration revokes only one superseded immutable generation.
// It cannot tombstone a concurrently activated newer generation.
func (p *CachedProvider) InvalidateGeneration(clusterID, generation uuid.UUID) {
	key := clientGenerationKey{clusterID: clusterID, generation: generation}
	p.mu.Lock()
	cell := p.generationClients[key]
	if cell == nil {
		// Retirement is durable even if a reader has captured the old bundle but
		// has not reached the provider yet.
		cell = new(clientTTL)
		p.generationClients[key] = cell
	}
	if state := p.clusterStates[clusterID]; state != nil {
		delete(state.allowed, generation)
		if state.retiredGenerations == nil {
			state.retiredGenerations = make(map[uuid.UUID]struct{})
		}
		state.retiredGenerations[generation] = struct{}{}
	}
	p.mu.Unlock()
	cell.mu.Lock()
	client := cell.client
	cell.client = nil
	cell.ttl = time.Time{}
	cell.hostsTTL = time.Time{}
	cell.revoked = true
	cell.mu.Unlock()
	if client != nil {
		logutil.LogOnError(context.Background(), p.logger, client.Close, "Couldn't close superseded scylla client generation")
	}
}

// RevokeCluster permanently tombstones a deleted cluster lifecycle and
// revokes every cached generation. The tombstone is checked both before and
// after client construction, so a reader paused before deletion cannot
// repopulate credential-bearing transports afterwards.
func (p *CachedProvider) RevokeCluster(clusterID, deletedGeneration uuid.UUID) {
	p.revokeCluster(clusterID, deletedGeneration, 0)
}

// RevokeClusterEpoch publishes a proven lifecycle tombstone and revokes every
// cached client from that epoch or any lower epoch.
func (p *CachedProvider) RevokeClusterEpoch(clusterID, deletedGeneration uuid.UUID, lifecycleEpoch int64) {
	p.revokeCluster(clusterID, deletedGeneration, lifecycleEpoch)
}

func (p *CachedProvider) revokeCluster(clusterID, deletedGeneration uuid.UUID, lifecycleEpoch int64) {
	p.mu.Lock()
	state := p.ensureClusterStateLocked(clusterID)
	if lifecycleEpoch > 0 && lifecycleEpoch < state.highestLifecycleEpoch {
		p.mu.Unlock()
		return
	}
	if lifecycleEpoch > 0 && lifecycleEpoch == state.highestLifecycleEpoch && !state.revoked && len(state.allowed) != 0 {
		// A nondeleted generation in this epoch has already been proven. This is
		// a delayed callback for the tombstone that preceded it.
		p.mu.Unlock()
		return
	}
	if lifecycleEpoch > state.highestLifecycleEpoch {
		state.highestLifecycleEpoch = lifecycleEpoch
	}
	state.revoked = true
	state.provisionalRevocation = false
	state.lifecycleVersion++
	state.deletedGeneration = deletedGeneration
	if state.retiredGenerations == nil {
		state.retiredGenerations = make(map[uuid.UUID]struct{})
	}
	for generation := range state.allowed {
		state.retiredGenerations[generation] = struct{}{}
	}
	state.retiredGenerations[deletedGeneration] = struct{}{}
	state.allowed = nil
	legacy := p.clients[clusterID]
	var generationCells []*clientTTL
	for key, cell := range p.generationClients {
		if key.clusterID == clusterID {
			generationCells = append(generationCells, cell)
		}
	}
	p.mu.Unlock()
	if legacy != nil {
		generationCells = append(generationCells, legacy)
	}
	for _, cell := range generationCells {
		cell.mu.Lock()
		client := cell.client
		cell.client = nil
		cell.ttl = time.Time{}
		cell.hostsTTL = time.Time{}
		cell.revoked = true
		cell.mu.Unlock()
		if client != nil {
			logutil.LogOnError(context.Background(), p.logger, client.Close, "Couldn't close revoked cluster client")
		}
	}
}

// ProvisionallyRevokeCluster closes all local transports after an
// indeterminate delete without advancing the proven epoch. A fresh stable
// serial validation can recover the old active generation when the CAS did
// not apply; an applied tombstone remains hidden by the repository.
func (p *CachedProvider) ProvisionallyRevokeCluster(clusterID, candidateDeletedGeneration uuid.UUID) {
	p.mu.Lock()
	state := p.ensureClusterStateLocked(clusterID)
	state.revoked = true
	state.provisionalRevocation = true
	state.deletedGeneration = candidateDeletedGeneration
	state.lifecycleVersion++
	legacy := p.clients[clusterID]
	var cells []*clientTTL
	for key, cell := range p.generationClients {
		if key.clusterID == clusterID {
			cells = append(cells, cell)
		}
	}
	p.mu.Unlock()
	if legacy != nil {
		cells = append(cells, legacy)
	}
	for _, cell := range cells {
		cell.mu.Lock()
		client := cell.client
		cell.client = nil
		cell.ttl = time.Time{}
		cell.hostsTTL = time.Time{}
		// The lifecycle state, not a permanent per-generation tombstone, gates
		// recovery after a definitely-not-applied outcome.
		cell.revoked = false
		cell.mu.Unlock()
		if client != nil {
			logutil.LogOnError(context.Background(), p.logger, client.Close, "Couldn't close provisionally revoked cluster client")
		}
	}
}

// Close removes all clients and closes them to clear up any resources.
func (p *CachedProvider) Close() error {
	p.mu.Lock()
	clients := make([]*clientTTL, 0, len(p.clients))
	for _, c := range p.clients {
		clients = append(clients, c)
	}
	for _, c := range p.generationClients {
		clients = append(clients, c)
	}
	p.mu.Unlock()

	for _, c := range clients {
		c.mu.Lock()
		client := c.client
		c.client = nil
		c.ttl = time.Time{}
		c.hostsTTL = time.Time{}
		c.mu.Unlock()
		if client != nil {
			logutil.LogOnError(context.Background(), p.logger, client.Close, "Couldn't close scylla client")
		}
	}

	return nil
}
