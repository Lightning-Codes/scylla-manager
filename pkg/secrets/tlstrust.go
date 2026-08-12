// Copyright (C) 2026 ScyllaDB

package secrets

import (
	"crypto/x509"
	"encoding/json"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

// TLSTrust contains the trust anchor and expected certificate identity for one
// cluster protocol. Protocol is intentionally not serialized; it selects the
// dedicated key in the generic secrets table.
type TLSTrust struct {
	ClusterID  uuid.UUID `json:"-"`
	Protocol   string    `json:"-"`
	CA         []byte    `json:"ca"`
	ServerName string    `json:"server_name"`
}

const (
	CQLProtocol        = "cql"
	AlternatorProtocol = "alternator"
	AgentProtocol      = "agent"
)

var _ store.Entry = &TLSTrust{}

// NewCQLTLSTrust creates a CQL trust entry.
func NewCQLTLSTrust(clusterID uuid.UUID) *TLSTrust {
	return &TLSTrust{ClusterID: clusterID, Protocol: CQLProtocol}
}

// NewAlternatorTLSTrust creates an Alternator trust entry.
func NewAlternatorTLSTrust(clusterID uuid.UUID) *TLSTrust {
	return &TLSTrust{ClusterID: clusterID, Protocol: AlternatorProtocol}
}

// NewAgentTLSTrust creates a Manager Agent trust entry.
func NewAgentTLSTrust(clusterID uuid.UUID) *TLSTrust {
	return &TLSTrust{ClusterID: clusterID, Protocol: AgentProtocol}
}

func (v *TLSTrust) Key() (clusterID uuid.UUID, key string) {
	switch v.Protocol {
	case CQLProtocol:
		key = "cql_tls_trust"
	case AlternatorProtocol:
		key = "alternator_tls_trust"
	case AgentProtocol:
		key = "agent_tls_trust"
	}
	return v.ClusterID, key
}

// Validate verifies that the entry contains a usable CA bundle and an
// explicit certificate server name.
func (v *TLSTrust) Validate() error {
	if len(v.CA) == 0 {
		return errors.New("missing CA certificate")
	}
	if strings.TrimSpace(v.ServerName) == "" {
		return errors.New("missing server name")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(v.CA) {
		return errors.New("CA file contains no certificates")
	}
	return nil
}

func (v *TLSTrust) MarshalBinary() (data []byte, err error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

func (v *TLSTrust) UnmarshalBinary(data []byte) error {
	if err := json.Unmarshal(data, v); err != nil {
		return err
	}
	return v.Validate()
}
