// Copyright (C) 2017 ScyllaDB

package cluster

import (
	"context"
	"crypto/tls"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/secrets"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/multierr"
)

// Cluster specifies a cluster properties.
type Cluster struct {
	ID uuid.UUID `json:"id"`
	// ConnectionGeneration binds this row to one complete ConnectionBundle.
	// It is never part of the public API.
	ConnectionGeneration         uuid.UUID         `json:"-"`
	ConnectionDeleted            bool              `json:"-"`
	LifecycleEpoch               int64             `json:"-"`
	PreviousConnectionGeneration uuid.UUID         `json:"-" db:"-"`
	Name                         string            `json:"name"`
	Labels                       map[string]string `json:"labels"`
	Host                         string            `json:"host"` // The initial contact point for SM (DNS or IP)
	KnownHosts                   []string          `json:"-"`    // Hosts discovered by connecting to Host (IPs)
	Port                         int               `json:"port,omitempty"`
	AuthToken                    string            `json:"auth_token,omitempty" db:"-"`

	ForceTLSDisabled       bool `json:"force_tls_disabled"`
	ForceNonSSLSessionPort bool `json:"force_non_ssl_session_port"`

	Username                  string `json:"username,omitempty" db:"-"`
	Password                  string `json:"password,omitempty" db:"-"`
	AlternatorAccessKeyID     string `json:"alternator_access_key_id,omitempty" db:"-"`
	AlternatorSecretAccessKey string `json:"alternator_secret_access_key,omitempty" db:"-"`
	SSLUserCertFile           []byte `json:"ssl_user_cert_file,omitempty" db:"-"`
	SSLUserKeyFile            []byte `json:"ssl_user_key_file,omitempty" db:"-"`
	CQLCAFile                 []byte `json:"cql_ca_file,omitempty" db:"-"`
	CQLServerName             string `json:"cql_server_name,omitempty" db:"-"`
	AlternatorCAFile          []byte `json:"alternator_ca_file,omitempty" db:"-"`
	AlternatorServerName      string `json:"alternator_server_name,omitempty" db:"-"`
	AgentCAFile               []byte `json:"agent_ca_file,omitempty" db:"-"`
	AgentServerName           string `json:"agent_server_name,omitempty" db:"-"`
	AuthTokenSet              bool   `json:"auth_token_set,omitempty" db:"-"`
	CQLCredentialsSet         bool   `json:"cql_credentials_set,omitempty" db:"-"`
	AlternatorCredentialsSet  bool   `json:"alternator_credentials_set,omitempty" db:"-"`
	SSLUserCertSet            bool   `json:"ssl_user_cert_set,omitempty" db:"-"`
	CQLCASet                  bool   `json:"cql_ca_set,omitempty" db:"-"`
	AlternatorCASet           bool   `json:"alternator_ca_set,omitempty" db:"-"`
	AgentCASet                bool   `json:"agent_ca_set,omitempty" db:"-"`
	WithoutRepair             bool   `json:"without_repair,omitempty" db:"-"`

	deleteCQLCredentials        bool
	deleteAlternatorCredentials bool
	deleteSSLUserCert           bool
	deleteCQLTrust              bool
	deleteAlternatorTrust       bool
	expectedLifecycleEpoch      *int64
}

// SecretDeletion selects write-only connection values to clear in one atomic
// generation mutation.
type SecretDeletion struct {
	CQLCredentials         bool
	AlternatorCredentials  bool
	SSLUserCert            bool
	CQLTrust               bool
	AlternatorTrust        bool
	ExpectedLifecycleEpoch *int64
}

type expectedLifecycleEpochContextKey struct{}

// WithExpectedLifecycleEpoch binds a mutation to the lifecycle observed by
// request admission. It prevents a paused request for lifecycle N from
// mutating a cluster ID that was deleted and recreated at lifecycle N+1.
func WithExpectedLifecycleEpoch(ctx context.Context, epoch int64) context.Context {
	return context.WithValue(ctx, expectedLifecycleEpochContextKey{}, epoch)
}

// ExpectedLifecycleEpoch returns the request-admission lifecycle precondition.
func ExpectedLifecycleEpoch(ctx context.Context) (int64, bool) {
	epoch, ok := ctx.Value(expectedLifecycleEpochContextKey{}).(int64)
	return epoch, ok
}

func (c *Cluster) requireExpectedLifecycleEpoch(ctx context.Context, active int64) error {
	expected := c.expectedLifecycleEpoch
	if expected == nil {
		if epoch, ok := ExpectedLifecycleEpoch(ctx); ok {
			expected = &epoch
		}
	}
	if expected != nil && *expected != active {
		return errors.Wrapf(ErrConnectionCommitConflict, "expected lifecycle epoch %d, active lifecycle epoch %d", *expected, active)
	}
	return nil
}

// SetExpectedLifecycleEpoch makes an API mutation conditional on the cluster
// lifecycle observed before request handling. Normal A->B rotations retain
// the epoch; a stale request admitted before deletion is rejected.
func (c *Cluster) SetExpectedLifecycleEpoch(epoch int64) {
	c.expectedLifecycleEpoch = &epoch
}

// String returns cluster Name or ID if Name is empty.
func (c *Cluster) String() string {
	if c == nil {
		return ""
	}
	if c.Name != "" {
		return c.Name
	}
	return c.ID.String()
}

func (c *Cluster) Validate() error {
	if c == nil {
		return errors.Wrap(util.ErrNilPtr, "invalid filter")
	}

	var errs error
	if c.AuthToken == "" {
		errs = multierr.Append(errs, errors.New("missing Agent auth token"))
	}
	if _, err := uuid.Parse(c.Name); err == nil {
		errs = multierr.Append(errs, errors.New("name cannot be an UUID"))
	}
	if c.Username == "" && c.Password != "" {
		errs = multierr.Append(errs, errors.New("missing user"))
	}
	if c.Username != "" && c.Password == "" {
		errs = multierr.Append(errs, errors.New("missing password"))
	}
	if c.AlternatorAccessKeyID == "" && c.AlternatorSecretAccessKey != "" {
		errs = multierr.Append(errs, errors.New("missing alternator access key ID"))
	}
	if c.AlternatorAccessKeyID != "" && c.AlternatorSecretAccessKey == "" {
		errs = multierr.Append(errs, errors.New("missing alternator secret access key"))
	}
	if len(c.SSLUserCertFile) != 0 && len(c.SSLUserKeyFile) == 0 {
		errs = multierr.Append(errs, errors.New("missing SSL user key"))
	}
	if len(c.SSLUserKeyFile) != 0 && len(c.SSLUserCertFile) == 0 {
		errs = multierr.Append(errs, errors.New("missing SSL user cert"))
	}
	if len(c.SSLUserCertFile) != 0 {
		_, err := tls.X509KeyPair(c.SSLUserCertFile, c.SSLUserKeyFile)
		errs = multierr.Append(errs, errors.Wrap(err, "invalid SSL user key pair"))
	}
	if len(c.CQLCAFile) == 0 && c.CQLServerName != "" {
		errs = multierr.Append(errs, errors.New("missing CQL CA file"))
	}
	if len(c.CQLCAFile) != 0 && c.CQLServerName == "" {
		errs = multierr.Append(errs, errors.New("missing CQL server name"))
	}
	if len(c.CQLCAFile) != 0 {
		trust := secrets.NewCQLTLSTrust(c.ID)
		trust.CA = c.CQLCAFile
		trust.ServerName = c.CQLServerName
		errs = multierr.Append(errs, errors.Wrap(trust.Validate(), "invalid CQL TLS trust"))
	}
	if c.ForceTLSDisabled && len(c.CQLCAFile) != 0 {
		errs = multierr.Append(errs, errors.New("force TLS disabled conflicts with CQL TLS trust"))
	}
	if len(c.AlternatorCAFile) == 0 && c.AlternatorServerName != "" {
		errs = multierr.Append(errs, errors.New("missing Alternator CA file"))
	}
	if len(c.AlternatorCAFile) != 0 && c.AlternatorServerName == "" {
		errs = multierr.Append(errs, errors.New("missing Alternator server name"))
	}
	if len(c.AlternatorCAFile) != 0 {
		trust := secrets.NewAlternatorTLSTrust(c.ID)
		trust.CA = c.AlternatorCAFile
		trust.ServerName = c.AlternatorServerName
		errs = multierr.Append(errs, errors.Wrap(trust.Validate(), "invalid Alternator TLS trust"))
	}
	if len(c.AgentCAFile) == 0 && c.AgentServerName != "" {
		errs = multierr.Append(errs, errors.New("missing Agent CA file"))
	}
	if len(c.AgentCAFile) != 0 && c.AgentServerName == "" {
		errs = multierr.Append(errs, errors.New("missing Agent server name"))
	}
	if len(c.AgentCAFile) != 0 {
		trust := secrets.NewAgentTLSTrust(c.ID)
		trust.CA = c.AgentCAFile
		trust.ServerName = c.AgentServerName
		errs = multierr.Append(errs, errors.Wrap(trust.Validate(), "invalid Agent TLS trust"))
	}

	return util.ErrValidate(errors.Wrap(errs, "invalid cluster"))
}

// Filter filters Clusters.
type Filter struct {
	Name string
}

func (f *Filter) Validate() error {
	if f == nil {
		return util.ErrNilPtr
	}

	var err error
	if _, e := uuid.Parse(f.Name); e == nil {
		err = multierr.Append(err, errors.New("name cannot be an UUID"))
	}

	return err
}

// Node represents single node in a cluster.
type Node struct {
	Datacenter        string
	Address           string
	ShardNum          uint
	PrometheusAddress string
	PrometheusPort    int
}
