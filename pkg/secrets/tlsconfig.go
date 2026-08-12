// Copyright (C) 2026 ScyllaDB

package secrets

import (
	"crypto/tls"
	"crypto/x509"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/store"
)

// LoadTLSConfig loads a protocol-specific trust entry and builds a strict TLS
// configuration. It never falls back to system roots or disabled verification.
func LoadTLSConfig(s store.Store, trust *TLSTrust) (*tls.Config, error) {
	if err := s.Get(trust); err != nil {
		return nil, errors.Wrap(err, "load TLS trust")
	}
	return TLSConfig(trust)
}

// TLSConfig builds a strict TLS configuration from already loaded trust.
func TLSConfig(trust *TLSTrust) (*tls.Config, error) {
	if err := trust.Validate(); err != nil {
		return nil, errors.Wrap(err, "validate TLS trust")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(trust.CA) {
		return nil, errors.New("TLS trust contains no CA certificates")
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    pool,
		ServerName: trust.ServerName,
	}, nil
}
