// Copyright (C) 2026 ScyllaDB

package cluster

import (
	"bytes"
	"context"
	"slices"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/scylla-manager/v3/pkg/schema/table"
	"github.com/scylladb/scylla-manager/v3/pkg/secrets"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// ErrConnectionGenerationMismatch is returned whenever the cluster row and
// its single-row connection bundle are not the same committed generation.
// Consumers must retry; they must never fall back to independently stored
// fields while a generation is present.
var ErrConnectionGenerationMismatch = errors.New("cluster connection generation is unavailable or does not match")

// ErrConnectionCommitConflict means another writer changed the active
// generation after this mutation read it. Callers must refetch and retry the
// whole preflight; overwriting the winner would violate the snapshot proof.
var ErrConnectionCommitConflict = errors.New("cluster connection generation changed concurrently")

// ErrConnectionCommitIndeterminate means a pointer LWT timed out and a fresh
// global-serial read could not prove whether it committed. The staged
// immutable row is retained and callers must not claim that the old
// generation is still active or blindly retry the mutation.
var ErrConnectionCommitIndeterminate = errors.New("cluster connection generation commit outcome is indeterminate")

type expectedConnectionGenerationContextKey struct{}

// WithExpectedConnectionGeneration pins every connection acquisition made
// with ctx to one immutable generation. Multi-protocol workflows set it from
// the first Agent client they acquire so later CQL, Alternator and config-cache
// reads cannot silently cross a concurrent rotation.
func WithExpectedConnectionGeneration(ctx context.Context, generation uuid.UUID) context.Context {
	return context.WithValue(ctx, expectedConnectionGenerationContextKey{}, generation)
}

// ExpectedConnectionGeneration returns the immutable generation pinned in
// ctx, when present.
func ExpectedConnectionGeneration(ctx context.Context) (uuid.UUID, bool) {
	generation, ok := ctx.Value(expectedConnectionGenerationContextKey{}).(uuid.UUID)
	return generation, ok
}

func requireExpectedConnectionGeneration(ctx context.Context, c *Cluster) error {
	expected, ok := ExpectedConnectionGeneration(ctx)
	if !ok || expected == c.ConnectionGeneration {
		return nil
	}
	return errors.Wrapf(ErrConnectionCommitConflict, "expected generation %s, active generation %s", expected, c.ConnectionGeneration)
}

// stageImmutableConnectionBundle inserts a generation-qualified row exactly
// once. On an ambiguous result it reads the exact key and accepts only
// byte-for-byte identical contents. It never overwrites a generation.
func (s *Service) stageImmutableConnectionBundle(ctx context.Context, b *secrets.ConnectionBundle) error {
	value, err := b.MarshalBinary()
	if err != nil {
		return err
	}
	_, key := b.Key()
	stmt, names := qb.Insert(table.SecureConnectionBundle.Name()).Columns("cluster_id", "key", "value").Unique().ToCql()
	q := s.session.ContextQuery(ctx, stmt, names).
		BindMap(qb.M{"cluster_id": b.ClusterID, "key": key, "value": value}).
		Consistency(gocql.All).
		SerialConsistency(gocql.Serial).
		Idempotent(true)
	applied, casErr := q.ExecCASRelease()
	if casErr == nil && applied {
		return nil
	}

	// Both a CAS conflict and an ambiguous transport/timeout result are
	// resolved by an exact normal-consistency read of the immutable key.
	resolveCtx, cancel := s.connectionResolutionContext(ctx)
	defer cancel()
	var stored []byte
	readErr := table.SecureConnectionBundle.GetQueryContext(resolveCtx, s.session, "value").Consistency(gocql.All).BindMap(qb.M{
		"cluster_id": b.ClusterID,
		"key":        key,
	}).GetRelease(&stored)
	if readErr == nil && bytes.Equal(stored, value) {
		return nil
	}
	if readErr == nil {
		return errors.Wrap(ErrConnectionCommitConflict, "immutable generation key contains different data")
	}
	if casErr != nil {
		return errors.Wrapf(casErr, "stage immutable generation (resolution failed: %v)", readErr)
	}
	return errors.Wrapf(ErrConnectionCommitConflict, "immutable generation exists but cannot be resolved: %v", readErr)
}

// compareAndSwapConnectionGeneration is the sole activation point. The full
// candidate row (including mirrored public endpoint fields) changes only when
// the previously read generation still matches. A timeout is resolved by
// reading the pointer: candidate means success, expected means confirmed
// failure, and any other/unavailable result stays explicitly indeterminate.
func (s *Service) compareAndSwapConnectionGeneration(ctx context.Context, c *Cluster, expected uuid.UUID, create bool) error {
	if !create && c.expectedLifecycleEpoch == nil {
		return errors.New("missing expected lifecycle epoch for connection generation update")
	}
	row := clusterConnectionRow(c)
	var (
		q *gocql.Query
	)
	if create {
		stmt, names := table.Cluster.InsertBuilder().Unique().ToCql()
		q = s.session.ContextQuery(ctx, stmt, names).BindStruct(row).Query
	} else {
		columns := table.Cluster.Metadata().Columns
		setColumns := make([]string, 0, len(columns)-1)
		for _, column := range columns {
			if column != "id" {
				setColumns = append(setColumns, column)
			}
		}
		builder := qb.Update(table.Cluster.Name()).
			Set(setColumns...).
			Where(qb.Eq("id"))
		bind := qb.M{}
		conditions := []qb.Cmp{qb.EqNamed("lifecycle_epoch", "expected_lifecycle_epoch")}
		bind["expected_lifecycle_epoch"] = *c.expectedLifecycleEpoch
		if expected == uuid.Nil {
			conditions = append(conditions, qb.EqLit("connection_generation", "null"))
		} else {
			conditions = append(conditions, qb.EqNamed("connection_generation", "expected_connection_generation"))
			bind["expected_connection_generation"] = expected
		}
		builder = builder.If(conditions...)
		stmt, names := builder.ToCql()
		q = s.session.ContextQuery(ctx, stmt, names).BindStructMap(row, bind).Query
	}
	q = q.SerialConsistency(gocql.Serial).Idempotent(true).WithContext(ctx)
	defer q.Release()
	applied, casErr := q.MapScanCAS(map[string]interface{}{})
	if casErr == nil && applied {
		return nil
	}

	resolveCtx, cancel := s.connectionResolutionContext(ctx)
	defer cancel()
	var active connectionPointer
	readErr := table.Cluster.GetQueryContext(resolveCtx, s.session, "connection_generation", "connection_deleted", "lifecycle_epoch").
		Consistency(gocql.Serial).
		BindMap(qb.M{"id": c.ID}).
		GetRelease(&active)
	expectedEpoch := int64(0)
	if c.expectedLifecycleEpoch != nil {
		expectedEpoch = *c.expectedLifecycleEpoch
	}
	expectedPointer := connectionPointer{Generation: expected, Epoch: expectedEpoch}
	return classifyConnectionPointerCAS(connectionPointer{
		Generation: c.ConnectionGeneration,
		Deleted:    c.ConnectionDeleted,
		Epoch:      c.LifecycleEpoch,
	}, expectedPointer, applied, casErr, active, readErr)
}

type connectionPointer struct {
	Generation uuid.UUID `db:"connection_generation"`
	Deleted    bool      `db:"connection_deleted"`
	Epoch      int64     `db:"lifecycle_epoch"`
}

func clusterConnectionRow(c *Cluster) *Cluster {
	row := *c
	// AuthToken belongs exclusively to the immutable secret bundle. Clearing
	// this historical public-row column prevents old code/readers from
	// consuming a connection-affecting plaintext duplicate.
	row.AuthToken = ""
	return &row
}

func classifyConnectionPointerCAS(candidate, expected connectionPointer, applied bool, casErr error, active connectionPointer, readErr error) error {
	if casErr == nil && applied {
		return nil
	}
	if readErr == nil && active == candidate {
		return nil
	}
	if casErr == nil && !applied {
		return errors.Wrapf(ErrConnectionCommitConflict, "expected generation %s at lifecycle epoch %d is no longer active", expected.Generation, expected.Epoch)
	}
	if readErr == nil && active == expected {
		return errors.Wrap(casErr, "connection generation CAS was not applied")
	}
	return errors.Wrapf(ErrConnectionCommitIndeterminate, "CAS error: %v; active read: %v", casErr, readErr)
}

// classifyConnectionGenerationCAS keeps the focused ambiguity classifier
// tests/API stable; production additionally compares lifecycle epoch and
// deleted state through classifyConnectionPointerCAS.
func classifyConnectionGenerationCAS(candidate, expected uuid.UUID, applied bool, casErr error, active uuid.UUID, readErr error) error {
	return classifyConnectionPointerCAS(
		connectionPointer{Generation: candidate},
		connectionPointer{Generation: expected},
		applied, casErr,
		connectionPointer{Generation: active}, readErr,
	)
}

// commitConnectionGeneration stages the immutable full tuple before invoking
// the sole pointer activation operation. A pointer failure never deletes the
// staged candidate and never rewrites the previous pointer.
func (s *Service) commitConnectionGeneration(ctx context.Context, c *Cluster, bundle *secrets.ConnectionBundle, expected uuid.UUID, create bool) error {
	stage := s.stageConnectionBundle
	if stage == nil {
		stage = s.stageImmutableConnectionBundle
	}
	if err := stage(ctx, bundle); err != nil {
		return errors.Wrap(err, "stage immutable connection generation")
	}
	c.ConnectionGeneration = bundle.Generation
	swap := s.swapConnectionGeneration
	if swap == nil {
		swap = s.compareAndSwapConnectionGeneration
	}
	if err := swap(ctx, c, expected, create); err != nil {
		// The staged row is intentionally retained. It is not active without the
		// pointer and deleting it could race an ambiguous CAS that did apply.
		return errors.Wrap(err, "activate connection generation")
	}
	return nil
}

func (s *Service) connectionResolutionContext(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := s.timeoutConfig.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return context.WithTimeout(context.WithoutCancel(parent), timeout)
}

// postCommitConnectionContext detaches durable post-commit reconciliation
// from request cancellation and replaces the precondition generation captured
// by REST middleware with the generation that just became authoritative.
func (s *Service) postCommitConnectionContext(parent context.Context, generation uuid.UUID) (context.Context, context.CancelFunc) {
	return s.connectionResolutionContext(WithExpectedConnectionGeneration(parent, generation))
}

func connectionBundleFromCluster(c *Cluster, generation uuid.UUID) *secrets.ConnectionBundle {
	return &secrets.ConnectionBundle{
		ClusterID:                 c.ID,
		Generation:                generation,
		PreviousGeneration:        c.PreviousConnectionGeneration,
		LifecycleEpoch:            c.LifecycleEpoch,
		Deleted:                   c.ConnectionDeleted,
		Host:                      c.Host,
		KnownHosts:                slices.Clone(c.KnownHosts),
		Port:                      c.Port,
		AuthToken:                 c.AuthToken,
		ForceTLSDisabled:          c.ForceTLSDisabled,
		ForceNonSSLSessionPort:    c.ForceNonSSLSessionPort,
		CQLUsername:               c.Username,
		CQLPassword:               c.Password,
		CQLClientCertificate:      slices.Clone(c.SSLUserCertFile),
		CQLClientPrivateKey:       slices.Clone(c.SSLUserKeyFile),
		CQLCA:                     slices.Clone(c.CQLCAFile),
		CQLServerName:             c.CQLServerName,
		AlternatorAccessKeyID:     c.AlternatorAccessKeyID,
		AlternatorSecretAccessKey: c.AlternatorSecretAccessKey,
		AlternatorCA:              slices.Clone(c.AlternatorCAFile),
		AlternatorServerName:      c.AlternatorServerName,
		AgentCA:                   slices.Clone(c.AgentCAFile),
		AgentServerName:           c.AgentServerName,
	}
}

func applyConnectionBundle(c *Cluster, b *secrets.ConnectionBundle) {
	c.ConnectionGeneration = b.Generation
	c.PreviousConnectionGeneration = b.PreviousGeneration
	c.LifecycleEpoch = b.LifecycleEpoch
	c.ConnectionDeleted = b.Deleted
	c.Host = b.Host
	c.KnownHosts = slices.Clone(b.KnownHosts)
	c.Port = b.Port
	c.AuthToken = b.AuthToken
	c.ForceTLSDisabled = b.ForceTLSDisabled
	c.ForceNonSSLSessionPort = b.ForceNonSSLSessionPort
	c.Username = b.CQLUsername
	c.Password = b.CQLPassword
	c.SSLUserCertFile = slices.Clone(b.CQLClientCertificate)
	c.SSLUserKeyFile = slices.Clone(b.CQLClientPrivateKey)
	c.CQLCAFile = slices.Clone(b.CQLCA)
	c.CQLServerName = b.CQLServerName
	c.AlternatorAccessKeyID = b.AlternatorAccessKeyID
	c.AlternatorSecretAccessKey = b.AlternatorSecretAccessKey
	c.AlternatorCAFile = slices.Clone(b.AlternatorCA)
	c.AlternatorServerName = b.AlternatorServerName
	c.AgentCAFile = slices.Clone(b.AgentCA)
	c.AgentServerName = b.AgentServerName
}

func deletedConnectionBundle(clusterID, generation, previous uuid.UUID, lifecycleEpoch int64) *secrets.ConnectionBundle {
	return &secrets.ConnectionBundle{
		ClusterID:          clusterID,
		Generation:         generation,
		PreviousGeneration: previous,
		LifecycleEpoch:     lifecycleEpoch,
		Deleted:            true,
	}
}

// mergeClusterConnection preserves omitted write-only fields from the active
// generation while applying every operational endpoint/policy value from the
// request. Explicit deletion remains a separate service operation.
func mergeClusterConnection(dst *Cluster, active *secrets.ConnectionBundle) {
	if active == nil {
		return
	}
	dst.ConnectionGeneration = active.Generation
	if dst.AuthToken == "" {
		dst.AuthToken = active.AuthToken
	}
	if dst.Username == "" && dst.Password == "" {
		dst.Username, dst.Password = active.CQLUsername, active.CQLPassword
	}
	if dst.deleteCQLCredentials {
		dst.Username, dst.Password = "", ""
	}
	if len(dst.SSLUserCertFile) == 0 && len(dst.SSLUserKeyFile) == 0 {
		dst.SSLUserCertFile = slices.Clone(active.CQLClientCertificate)
		dst.SSLUserKeyFile = slices.Clone(active.CQLClientPrivateKey)
	}
	if dst.deleteSSLUserCert {
		dst.SSLUserCertFile, dst.SSLUserKeyFile = nil, nil
	}
	if len(dst.CQLCAFile) == 0 && dst.CQLServerName == "" {
		dst.CQLCAFile = slices.Clone(active.CQLCA)
		dst.CQLServerName = active.CQLServerName
	}
	if dst.deleteCQLTrust {
		dst.CQLCAFile, dst.CQLServerName = nil, ""
	}
	if dst.AlternatorAccessKeyID == "" && dst.AlternatorSecretAccessKey == "" {
		dst.AlternatorAccessKeyID = active.AlternatorAccessKeyID
		dst.AlternatorSecretAccessKey = active.AlternatorSecretAccessKey
	}
	if dst.deleteAlternatorCredentials {
		dst.AlternatorAccessKeyID, dst.AlternatorSecretAccessKey = "", ""
	}
	if len(dst.AlternatorCAFile) == 0 && dst.AlternatorServerName == "" {
		dst.AlternatorCAFile = slices.Clone(active.AlternatorCA)
		dst.AlternatorServerName = active.AlternatorServerName
	}
	if dst.deleteAlternatorTrust {
		dst.AlternatorCAFile, dst.AlternatorServerName = nil, ""
	}
	if len(dst.AgentCAFile) == 0 && dst.AgentServerName == "" {
		dst.AgentCAFile = slices.Clone(active.AgentCA)
		dst.AgentServerName = active.AgentServerName
	}
}

func (s *Service) getConnectionBundle(c *Cluster) (*secrets.ConnectionBundle, error) {
	b := secrets.NewConnectionBundle(c.ID)
	b.Generation = c.ConnectionGeneration
	var err error
	if s.connectionBundleStore != nil {
		err = s.connectionBundleStore.Get(b)
	} else {
		var value []byte
		_, key := b.Key()
		err = table.SecureConnectionBundle.GetQuery(s.session, "value").Consistency(gocql.All).BindMap(qb.M{
			"cluster_id": c.ID,
			"key":        key,
		}).GetRelease(&value)
		if err == nil {
			err = b.UnmarshalBinary(value)
		}
	}
	if err != nil {
		if c.ConnectionGeneration != uuid.Nil {
			return nil, errors.Wrap(ErrConnectionGenerationMismatch, "load active connection bundle")
		}
		return nil, err
	}
	if c.ConnectionGeneration == uuid.Nil {
		return nil, errors.Wrap(ErrConnectionGenerationMismatch, "bundle exists without a cluster generation")
	}
	if b.Generation != c.ConnectionGeneration {
		return nil, errors.Wrapf(ErrConnectionGenerationMismatch, "cluster generation %s, bundle generation %s", c.ConnectionGeneration, b.Generation)
	}
	return b, nil
}

func (s *Service) hydrateClusterConnection(c *Cluster) error {
	if c.ConnectionGeneration != uuid.Nil {
		b, err := s.getConnectionBundle(c)
		if err != nil {
			return err
		}
		applyConnectionBundle(c, b)
		return nil
	}
	// This branch is greenfield-only. A nil pointer is never hydrated from the
	// legacy cluster or generic-secrets tables.
	return errors.Wrap(ErrConnectionGenerationMismatch, "cluster has no active connection generation")
}
