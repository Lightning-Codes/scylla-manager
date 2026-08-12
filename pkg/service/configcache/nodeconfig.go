// Copyright (C) 2024 ScyllaDB

package configcache

import (
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
)

// NodeConfig keeps the node current node configuration together with the TLS details per different type of connection.
type NodeConfig struct {
	*scyllaclient.NodeInfo

	cqlTLSConfig        *TLSConfigWithAddress
	alternatorTLSConfig *TLSConfigWithAddress
	Rack                string
	Datacenter          string
}

// NewNodeConfig creates and initializes new node configuration struct containing TLS configuration of CQL and Alternator.
func NewNodeConfig(c *cluster.Cluster, nodeInfo *scyllaclient.NodeInfo, secretsStore store.Store, host, dc, rack string) (config NodeConfig, err error) {
	if nodeInfo == nil {
		return NodeConfig{}, errors.New("building node config: node info is unavailable")
	}
	if nodeInfo.ClientEncryptionEnabled && c.ForceTLSDisabled {
		return NodeConfig{}, errors.New("building node config: CQL TLS is advertised but force TLS disabled is set")
	}
	if (nodeInfo.CqlPasswordProtected || nodeInfo.ClientEncryptionRequireAuth) && !nodeInfo.ClientEncryptionEnabled {
		return NodeConfig{}, errors.New("building node config: CQL authentication requires TLS")
	}
	if nodeInfo.AlternatorEnforceAuthorization && !nodeInfo.AlternatorEncryptionEnabled() {
		return NodeConfig{}, errors.New("building node config: Alternator authentication requires TLS")
	}
	cqlTLS, err := newCQLTLSConfigIfEnabled(c, nodeInfo, secretsStore, host)
	if err != nil {
		return NodeConfig{}, errors.Wrap(err, "building node config")
	}
	alternatorTLS, err := newAlternatorTLSConfigIfEnabled(c, nodeInfo, secretsStore, host)
	if err != nil {
		return NodeConfig{}, errors.Wrap(err, "building node config")
	}
	return NodeConfig{
		NodeInfo:            nodeInfo,
		cqlTLSConfig:        cqlTLS,
		alternatorTLSConfig: alternatorTLS,
		Datacenter:          dc,
		Rack:                rack,
	}, nil
}

// CQLTLSConfig is a getter of TLS configuration for CQL session.
func (nc NodeConfig) CQLTLSConfig() *TLSConfigWithAddress {
	return nc.cqlTLSConfig
}

// AlternatorTLSConfig is a getter of TLS configuration for Alternator session.
func (nc NodeConfig) AlternatorTLSConfig() *TLSConfigWithAddress {
	return nc.alternatorTLSConfig
}
