// Copyright (C) 2017 ScyllaDB

package server

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"github.com/scylladb/scylla-manager/v3/pkg/service/restore"

	"github.com/scylladb/scylla-manager/v3/pkg/config"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/healthcheck"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
	"github.com/scylladb/scylla-manager/v3/pkg/util/cfgutil"
)

// DBConfig specifies Scylla Manager backend database configuration options.
type DBConfig struct {
	Hosts                         []string      `yaml:"hosts"`
	SSL                           bool          `yaml:"ssl"`
	User                          string        `yaml:"user"`
	Password                      string        `yaml:"password"`
	UserFile                      string        `yaml:"user_file"`
	PasswordFile                  string        `yaml:"password_file"`
	LocalDC                       string        `yaml:"local_dc"`
	Keyspace                      string        `yaml:"keyspace"`
	MigrateTimeout                time.Duration `yaml:"migrate_timeout"`
	MigrateMaxWaitSchemaAgreement time.Duration `yaml:"migrate_max_wait_schema_agreement"`
	ReplicationFactor             int           `yaml:"replication_factor"`
	Timeout                       time.Duration `yaml:"timeout"`
	TokenAware                    bool          `yaml:"token_aware"`
	Port                          int           `yaml:"port"`

	// InitAddr specifies address used to create manager keyspace and tables.
	InitAddr string
}

// SSLConfig specifies Scylla Manager backend database SSL configuration options.
type SSLConfig struct {
	CertFile     string `yaml:"cert_file"`
	Validate     bool   `yaml:"validate"`
	ServerName   string `yaml:"server_name"`
	UserCertFile string `yaml:"user_cert_file"`
	UserKeyFile  string `yaml:"user_key_file"`
}

// Config contains configuration structure for scylla manager.
type Config struct {
	HTTP               string                     `yaml:"http"`
	HTTPS              string                     `yaml:"https"`
	TLSVersion         config.TLSVersion          `yaml:"tls_version"`
	TLSCertFile        string                     `yaml:"tls_cert_file"`
	TLSKeyFile         string                     `yaml:"tls_key_file"`
	TLSCAFile          string                     `yaml:"tls_ca_file"`
	Prometheus         string                     `yaml:"prometheus"`
	Debug              string                     `yaml:"debug"`
	ClientCacheTimeout time.Duration              `yaml:"client_cache_timeout"`
	Logger             config.LogConfig           `yaml:"logger"`
	Database           DBConfig                   `yaml:"database"`
	SSL                SSLConfig                  `yaml:"ssl"`
	Healthcheck        healthcheck.Config         `yaml:"healthcheck"`
	ConfigCache        configcache.Config         `yaml:"config_cache"`
	Backup             backup.Config              `yaml:"backup"`
	Restore            restore.Config             `yaml:"restore"`
	Repair             repair.Config              `yaml:"repair"`
	TimeoutConfig      scyllaclient.TimeoutConfig `yaml:"agent_client"`
}

func DefaultConfig() Config {
	return Config{
		TLSVersion: config.TLSv12,
		Prometheus: ":5090",
		Debug:      "127.0.0.1:5112",
		Logger:     DefaultLogConfig(),
		Database: DBConfig{
			Hosts:                         []string{"127.0.0.1"},
			Keyspace:                      "scylla_manager",
			MigrateTimeout:                30 * time.Second,
			MigrateMaxWaitSchemaAgreement: 5 * time.Minute,
			ReplicationFactor:             1,
			Timeout:                       1 * time.Second,
			TokenAware:                    true,
			Port:                          9042,
		},
		SSL: SSLConfig{
			Validate: true,
		},
		Healthcheck:        healthcheck.DefaultConfig(),
		ClientCacheTimeout: 15 * time.Minute,
		Backup:             backup.DefaultConfig(),
		Restore:            restore.DefaultConfig(),
		Repair:             repair.DefaultConfig(),
		TimeoutConfig:      scyllaclient.DefaultTimeoutConfig(),
		ConfigCache:        configcache.DefaultConfig(),
	}
}

// ParseConfigFiles takes list of configuration file paths and returns parsed
// config struct with merged configuration from all provided files.
func ParseConfigFiles(files []string) (Config, error) {
	c := DefaultConfig()
	if err := cfgutil.ParseYAML(&c, DefaultConfig(), files...); err != nil {
		return c, err
	}
	if err := c.resolveDatabaseCredentials(); err != nil {
		return c, errors.Wrap(err, "database credentials")
	}
	return c, nil
}

func (c Config) Validate() error {
	if c.HTTP == "" && c.HTTPS == "" {
		return errors.New("missing http or https")
	}
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return errors.New("tls_cert_file and tls_key_file must be configured together")
	}
	if c.TLSCAFile != "" {
		if c.HTTPS == "" {
			return errors.New("https is required when tls_ca_file is configured")
		}
		if c.HTTP != "" {
			return errors.New("http must be disabled when tls_ca_file is configured")
		}
		if c.TLSCertFile == "" {
			return errors.New("tls_cert_file and tls_key_file are required when tls_ca_file is configured")
		}
	}
	if strings.Contains(c.Database.Keyspace, "\"") {
		return errors.New("database.keyspace contains quotes")
	}
	if len(c.Database.Hosts) == 0 {
		return errors.New("missing database.hosts")
	}
	if c.Database.ReplicationFactor <= 0 {
		return errors.New("invalid database.replication_factor <= 0")
	}
	if (c.Database.User == "") != (c.Database.Password == "") {
		return errors.New("database user and password must be configured together")
	}
	if c.Database.User != "" {
		if !c.Database.SSL {
			return errors.New("database credentials require database.ssl=true")
		}
		if !c.SSL.Validate {
			return errors.New("database credentials require ssl.validate=true")
		}
		if c.SSL.CertFile == "" {
			return errors.New("database credentials require ssl.cert_file with the database CA")
		}
		if strings.TrimSpace(c.SSL.ServerName) == "" {
			return errors.New("database credentials require ssl.server_name")
		}
	}
	if (c.SSL.UserCertFile == "") != (c.SSL.UserKeyFile == "") {
		return errors.New("ssl.user_cert_file and ssl.user_key_file must be configured together")
	}
	if c.SSL.UserCertFile != "" {
		if !c.Database.SSL || !c.SSL.Validate || c.SSL.CertFile == "" || strings.TrimSpace(c.SSL.ServerName) == "" {
			return errors.New("database client identity requires database.ssl=true, ssl.validate=true, ssl.cert_file, and ssl.server_name")
		}
	}
	if c.SSL.ServerName != "" && (!c.Database.SSL || !c.SSL.Validate) {
		return errors.New("ssl.server_name requires database.ssl=true and ssl.validate=true")
	}
	if err := c.Backup.Validate(); err != nil {
		return errors.Wrap(err, "backup")
	}
	if err := c.Repair.Validate(); err != nil {
		return errors.Wrap(err, "repair")
	}

	return nil
}

func (c *Config) resolveDatabaseCredentials() error {
	db := &c.Database
	if db.User != "" && db.UserFile != "" {
		return errors.New("user and user_file are mutually exclusive")
	}
	if db.Password != "" && db.PasswordFile != "" {
		return errors.New("password and password_file are mutually exclusive")
	}
	if (db.UserFile == "") != (db.PasswordFile == "") {
		return errors.New("user_file and password_file must be configured together")
	}
	if db.UserFile == "" {
		return nil
	}
	var err error
	db.User, err = readCredentialFile(db.UserFile)
	if err != nil {
		return errors.Wrap(err, "user_file")
	}
	db.Password, err = readCredentialFile(db.PasswordFile)
	if err != nil {
		db.User = ""
		return errors.Wrap(err, "password_file")
	}
	return nil
}

func readCredentialFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("not a regular file")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return "", errors.Errorf("permissions %04o allow group or other access", info.Mode().Perm())
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}
	v := strings.TrimSuffix(strings.TrimSuffix(string(b), "\n"), "\r")
	if strings.TrimSpace(v) == "" {
		return "", errors.New("file is empty")
	}
	return v, nil
}

// HasTLSCert returns true iff TLSCertFile or TLSKeyFile is set.
func (c Config) HasTLSCert() bool {
	return c.TLSCertFile != "" || c.TLSKeyFile != ""
}

// Obfuscate returns Config with secrets replaced with ******.
func Obfuscate(c Config) Config {
	c.Database.User = strings.Repeat("*", len(c.Database.User))
	c.Database.Password = strings.Repeat("*", len(c.Database.Password))
	return c
}
