// Copyright (C) 2026 ScyllaDB

package secrets

import (
	"crypto/tls"
	"encoding/json"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/multierr"
)

// ConnectionBundle is the complete immutable-generation view of every value
// that can affect a connection to one managed cluster. It is deliberately
// stored as one generic-secrets row so readers can observe only the old or the
// new tuple, never a mixture assembled from independently rotated entries.
//
// The cluster table carries ConnectionGeneration. A bundle is usable only
// when both generations match; this makes the short row/bundle commit window
// fail closed.
type ConnectionBundle struct {
	// ClusterID is embedded even though the physical partition is derived from
	// it. Readers verify this identity before using any credential material.
	ClusterID          uuid.UUID `json:"cluster_id"`
	Generation         uuid.UUID `json:"generation"`
	PreviousGeneration uuid.UUID `json:"previous_generation"`
	LifecycleEpoch     int64     `json:"lifecycle_epoch"`
	// Deleted is an immutable lifecycle tombstone. Tombstones intentionally
	// contain no endpoint or credential material and remain addressable so ID
	// reuse cannot race wildcard secret cleanup.
	Deleted    bool     `json:"deleted,omitempty"`
	Host       string   `json:"host"`
	KnownHosts []string `json:"known_hosts"`
	Port       int      `json:"port"`

	AuthToken                 string `json:"auth_token"`
	ForceTLSDisabled          bool   `json:"force_tls_disabled"`
	ForceNonSSLSessionPort    bool   `json:"force_non_ssl_session_port"`
	CQLUsername               string `json:"cql_username"`
	CQLPassword               string `json:"cql_password"`
	CQLClientCertificate      []byte `json:"cql_client_certificate"`
	CQLClientPrivateKey       []byte `json:"cql_client_private_key"`
	CQLCA                     []byte `json:"cql_ca"`
	CQLServerName             string `json:"cql_server_name"`
	AlternatorAccessKeyID     string `json:"alternator_access_key_id"`
	AlternatorSecretAccessKey string `json:"alternator_secret_access_key"`
	AlternatorCA              []byte `json:"alternator_ca"`
	AlternatorServerName      string `json:"alternator_server_name"`
	AgentCA                   []byte `json:"agent_ca"`
	AgentServerName           string `json:"agent_server_name"`
}

var _ store.Entry = &ConnectionBundle{}

// NewConnectionBundle returns an empty connection bundle entry.
func NewConnectionBundle(clusterID uuid.UUID) *ConnectionBundle {
	return &ConnectionBundle{ClusterID: clusterID}
}

// Key is generation-specific. Rows are immutable and retained so a reader
// that captured an older cluster pointer can finish using that exact tuple.
func (b *ConnectionBundle) Key() (clusterID uuid.UUID, key string) {
	return b.ClusterID, ConnectionBundleKey(b.Generation)
}

// ConnectionBundleKey returns the immutable row key for one generation.
func ConnectionBundleKey(generation uuid.UUID) string {
	return "connection_bundle/" + generation.String()
}

// Validate rejects incomplete security pairs before a bundle can become
// active. Endpoint-specific requirements are additionally proved by the
// cluster service preflight using protected Agent NodeInfo.
func (b *ConnectionBundle) Validate() error {
	var errs error
	if b.ClusterID == uuid.Nil {
		errs = multierr.Append(errs, errors.New("missing cluster ID"))
	}
	if b.Generation == uuid.Nil {
		errs = multierr.Append(errs, errors.New("missing generation"))
	}
	if b.LifecycleEpoch <= 0 {
		errs = multierr.Append(errs, errors.New("missing lifecycle epoch"))
	}
	if b.PreviousGeneration == b.Generation {
		errs = multierr.Append(errs, errors.New("generation cannot refer to itself as previous"))
	}
	if b.Deleted {
		if b.Host != "" || len(b.KnownHosts) != 0 || b.Port != 0 || b.AuthToken != "" ||
			b.ForceTLSDisabled || b.ForceNonSSLSessionPort || b.CQLUsername != "" || b.CQLPassword != "" ||
			len(b.CQLClientCertificate) != 0 || len(b.CQLClientPrivateKey) != 0 || len(b.CQLCA) != 0 || b.CQLServerName != "" ||
			b.AlternatorAccessKeyID != "" || b.AlternatorSecretAccessKey != "" || len(b.AlternatorCA) != 0 || b.AlternatorServerName != "" ||
			len(b.AgentCA) != 0 || b.AgentServerName != "" {
			errs = multierr.Append(errs, errors.New("deleted connection bundle contains operational material"))
		}
		return errors.Wrap(errs, "invalid connection bundle")
	}
	if strings.TrimSpace(b.Host) == "" {
		errs = multierr.Append(errs, errors.New("missing host"))
	}
	if strings.TrimSpace(b.AuthToken) == "" {
		errs = multierr.Append(errs, errors.New("missing Agent auth token"))
	}
	if len(b.AgentCA) == 0 || strings.TrimSpace(b.AgentServerName) == "" {
		errs = multierr.Append(errs, errors.New("missing mandatory Agent CA or server name"))
	} else {
		t := NewAgentTLSTrust(b.ClusterID)
		t.CA, t.ServerName = b.AgentCA, b.AgentServerName
		errs = multierr.Append(errs, errors.Wrap(t.Validate(), "invalid Agent TLS trust"))
	}
	if (b.CQLUsername == "") != (b.CQLPassword == "") {
		errs = multierr.Append(errs, errors.New("incomplete CQL credentials"))
	}
	if (len(b.CQLClientCertificate) == 0) != (len(b.CQLClientPrivateKey) == 0) {
		errs = multierr.Append(errs, errors.New("incomplete CQL client identity"))
	} else if len(b.CQLClientCertificate) != 0 {
		_, err := tls.X509KeyPair(b.CQLClientCertificate, b.CQLClientPrivateKey)
		errs = multierr.Append(errs, errors.Wrap(err, "invalid CQL client identity"))
	}
	if (len(b.CQLCA) == 0) != (strings.TrimSpace(b.CQLServerName) == "") {
		errs = multierr.Append(errs, errors.New("incomplete CQL TLS trust"))
	} else if len(b.CQLCA) != 0 {
		t := NewCQLTLSTrust(b.ClusterID)
		t.CA, t.ServerName = b.CQLCA, b.CQLServerName
		errs = multierr.Append(errs, errors.Wrap(t.Validate(), "invalid CQL TLS trust"))
	}
	if (b.AlternatorAccessKeyID == "") != (b.AlternatorSecretAccessKey == "") {
		errs = multierr.Append(errs, errors.New("incomplete Alternator credentials"))
	}
	if (len(b.AlternatorCA) == 0) != (strings.TrimSpace(b.AlternatorServerName) == "") {
		errs = multierr.Append(errs, errors.New("incomplete Alternator TLS trust"))
	} else if len(b.AlternatorCA) != 0 {
		t := NewAlternatorTLSTrust(b.ClusterID)
		t.CA, t.ServerName = b.AlternatorCA, b.AlternatorServerName
		errs = multierr.Append(errs, errors.Wrap(t.Validate(), "invalid Alternator TLS trust"))
	}
	return errors.Wrap(errs, "invalid connection bundle")
}

func (b *ConnectionBundle) MarshalBinary() ([]byte, error) {
	if err := b.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(b)
}

func (b *ConnectionBundle) UnmarshalBinary(data []byte) error {
	expectedClusterID := b.ClusterID
	expectedGeneration := b.Generation
	if err := json.Unmarshal(data, b); err != nil {
		return err
	}
	if expectedClusterID != uuid.Nil && b.ClusterID != expectedClusterID {
		return errors.Errorf("connection bundle cluster ID mismatch: expected %s, got %s", expectedClusterID, b.ClusterID)
	}
	if expectedGeneration != uuid.Nil && b.Generation != expectedGeneration {
		return errors.Errorf("connection bundle generation mismatch: expected %s, got %s", expectedGeneration, b.Generation)
	}
	return b.Validate()
}
