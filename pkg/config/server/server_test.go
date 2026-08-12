// Copyright (C) 2017 ScyllaDB

package server_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/service/configcache"
	"github.com/scylladb/scylla-manager/v3/pkg/service/restore"
	"github.com/scylladb/scylla-manager/v3/pkg/util/schedules"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/scylladb/scylla-manager/v3/pkg/config"
	"github.com/scylladb/scylla-manager/v3/pkg/config/server"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/healthcheck"
	"github.com/scylladb/scylla-manager/v3/pkg/service/repair"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
)

var configCmpOpts = cmp.Options{
	testutils.UUIDComparer(),
	cmpopts.IgnoreUnexported(server.DBConfig{}),
	cmpopts.IgnoreTypes(zap.AtomicLevel{}),
	cmpopts.IgnoreFields(schedules.Cron{}, "inner"),
}

func TestFileBackedDatabaseCredentials(t *testing.T) {
	dir := t.TempDir()
	userFile := filepath.Join(dir, "user")
	passwordFile := filepath.Join(dir, "password")
	if err := os.WriteFile(userFile, []byte("manager\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(passwordFile, []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configFile := filepath.Join(dir, "manager.yaml")
	configYAML := "database:\n  ssl: true\n  user_file: " + userFile + "\n  password_file: " + passwordFile + "\nssl:\n  cert_file: /run/secrets/database/ca.crt\n  validate: true\n"
	if err := os.WriteFile(configFile, []byte(configYAML), 0o600); err != nil {
		t.Fatal(err)
	}

	c, err := server.ParseConfigFiles([]string{configFile})
	if err != nil {
		t.Fatal(err)
	}
	if c.Database.User != "manager" || c.Database.Password != "secret" {
		t.Fatal("file-backed credentials were not resolved")
	}
	o := server.Obfuscate(c)
	if strings.Contains(o.Database.User, "manager") || strings.Contains(o.Database.Password, "secret") {
		t.Fatal("obfuscated config exposed credential contents")
	}
}

func TestFileBackedDatabaseCredentialsRejectUnsafeInput(t *testing.T) {
	dir := t.TempDir()
	secure := filepath.Join(dir, "secure")
	unsafe := filepath.Join(dir, "unsafe")
	if err := os.WriteFile(secure, []byte("value"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(unsafe, []byte("value"), 0o644); err != nil {
		t.Fatal(err)
	}

	for name, databaseYAML := range map[string]string{
		"mixed inline and file": "  user: inline\n  user_file: " + secure + "\n  password: value\n",
		"missing password file": "  user_file: " + secure + "\n",
		"unsafe permissions":    "  user_file: " + secure + "\n  password_file: " + unsafe + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			configFile := filepath.Join(dir, strings.ReplaceAll(name, " ", "-")+".yaml")
			if err := os.WriteFile(configFile, []byte("database:\n"+databaseYAML), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := server.ParseConfigFiles([]string{configFile}); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestManagerAPIMTLSValidation(t *testing.T) {
	valid := server.DefaultConfig()
	valid.HTTP = ""
	valid.HTTPS = ":5081"
	valid.TLSCertFile = "server.crt"
	valid.TLSKeyFile = "server.key"
	valid.TLSCAFile = "client-ca.crt"
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid mTLS configuration rejected: %v", err)
	}

	for name, mutate := range map[string]func(*server.Config){
		"plaintext listener": func(c *server.Config) { c.HTTP = ":5080" },
		"missing https": func(c *server.Config) {
			c.HTTPS = ""
		},
		"missing serving key": func(c *server.Config) { c.TLSKeyFile = "" },
		"missing serving identity": func(c *server.Config) {
			c.TLSCertFile = ""
			c.TLSKeyFile = ""
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := valid
			mutate(&c)
			if err := c.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestDatabaseCredentialsRequireVerifiedTLS(t *testing.T) {
	valid := server.DefaultConfig()
	valid.HTTP = ":5080"
	valid.Database.User = "manager"
	valid.Database.Password = "secret"
	valid.Database.SSL = true
	valid.SSL.Validate = true
	valid.SSL.CertFile = "/run/secrets/database/ca.crt"
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid authenticated database TLS rejected: %v", err)
	}
	for name, mutate := range map[string]func(*server.Config){
		"plaintext":           func(c *server.Config) { c.Database.SSL = false },
		"unverified TLS":      func(c *server.Config) { c.SSL.Validate = false },
		"missing pinned CA":   func(c *server.Config) { c.SSL.CertFile = "" },
		"incomplete identity": func(c *server.Config) { c.SSL.UserCertFile = "client.crt" },
	} {
		t.Run(name, func(t *testing.T) {
			c := valid
			mutate(&c)
			if err := c.Validate(); err == nil {
				t.Fatal("unsafe database authentication configuration accepted")
			}
		})
	}
}

func TestConfigModification(t *testing.T) {
	t.Parallel()

	c, err := server.ParseConfigFiles([]string{"testdata/scylla-manager.yaml"})
	if err != nil {
		t.Fatal(err)
	}

	golden := server.Config{
		HTTP:               "127.0.0.1:80",
		HTTPS:              "127.0.0.1:443",
		TLSVersion:         "TLSv1.3",
		TLSCertFile:        "tls.cert",
		TLSKeyFile:         "tls.key",
		TLSCAFile:          "ca.cert",
		Prometheus:         "127.0.0.1:9090",
		Debug:              "127.0.0.1:112",
		ClientCacheTimeout: 15 * time.Minute,
		Logger: config.LogConfig{
			Config: log.Config{
				Mode:  log.StderrMode,
				Level: zap.NewAtomicLevelAt(zapcore.DebugLevel),
			},
		},
		Database: server.DBConfig{
			Hosts:                         []string{"172.16.1.10", "172.16.1.20"},
			SSL:                           true,
			User:                          "user",
			Port:                          9042,
			Password:                      "password",
			LocalDC:                       "local",
			Keyspace:                      "scylla_manager",
			MigrateTimeout:                30 * time.Second,
			MigrateMaxWaitSchemaAgreement: 5 * time.Minute,
			ReplicationFactor:             3,
			Timeout:                       1 * time.Second,
			TokenAware:                    false,
		},
		SSL: server.SSLConfig{
			CertFile:     "ca.pem",
			Validate:     true,
			UserCertFile: "ssl.cert",
			UserKeyFile:  "ssl.key",
		},
		Healthcheck: healthcheck.Config{
			MaxTimeout:         time.Second,
			NodeInfoTTL:        time.Second,
			CQLPingCron:        schedules.MustCron("* 5,15 * * * *", time.Time{}),
			RESTPingCron:       schedules.MustCron("* 5,15 * * * *", time.Time{}),
			AlternatorPingCron: schedules.MustCron("* 5,15 * * * *", time.Time{}),
		},
		Backup: backup.Config{
			DiskSpaceFreeMinPercent:   1,
			LongPollingTimeoutSeconds: 5,
			AgeMax:                    24 * time.Hour,
		},
		Restore: restore.Config{
			DiskSpaceFreeMinPercent:   1,
			LongPollingTimeoutSeconds: 5,
		},
		Repair: repair.Config{
			StatusTimeout:                   time.Hour,
			PollInterval:                    500 * time.Millisecond,
			LongPollingTimeoutSeconds:       5,
			AgeMax:                          12 * time.Hour,
			GracefulStopTimeout:             60 * time.Second,
			ForceRepairType:                 repair.TypeAuto,
			Murmur3PartitionerIgnoreMSBBits: 12,
		},
		TimeoutConfig: scyllaclient.TimeoutConfig{
			Timeout:     45 * time.Second,
			MaxTimeout:  5 * time.Hour,
			ListTimeout: 7 * time.Minute,
			Backoff: scyllaclient.BackoffConfig{
				WaitMin:    5 * time.Second,
				WaitMax:    20 * time.Second,
				MaxRetries: 12,
				Multiplier: 8,
				Jitter:     0.6,
			},
			InteractiveBackoff: scyllaclient.BackoffConfig{
				WaitMin:    2 * time.Second,
				MaxRetries: 4,
			},
			PoolDecayDuration: time.Hour,
		},
		ConfigCache: configcache.Config{
			UpdateFrequency: 5 * time.Minute,
		},
	}

	if diff := cmp.Diff(c, golden, configCmpOpts); diff != "" {
		t.Fatal(diff)
	}
}

func TestDefaultConfig(t *testing.T) {
	c, err := server.ParseConfigFiles([]string{"../../../dist/etc/scylla-manager.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	e := server.DefaultConfig()

	if diff := cmp.Diff(c, e, configCmpOpts, cmpopts.IgnoreFields(server.Config{}, "HTTP", "HTTPS")); diff != "" {
		t.Fatal(diff)
	}
}
