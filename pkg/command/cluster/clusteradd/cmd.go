// Copyright (C) 2017 ScyllaDB

package clusteradd

import (
	_ "embed"
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/command/flag"
	"github.com/scylladb/scylla-manager/v3/pkg/managerclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/clipper"
	"github.com/scylladb/scylla-manager/v3/pkg/util/fsutil"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

//go:embed res.yaml
var res []byte

type command struct {
	cobra.Command

	client *managerclient.Client

	id                        string
	name                      string
	label                     flag.Label
	host                      string
	port                      int64
	authToken                 string
	username                  string
	password                  string
	alternatorAccessKeyID     string
	alternatorSecretAccessKey string
	sslUserCertFile           string
	sslUserKeyFile            string
	cqlCAFile                 string
	cqlServerName             string
	alternatorCAFile          string
	alternatorServerName      string
	agentCAFile               string
	agentServerName           string
	withoutRepair             bool
	forceTLSDisabled          bool
	forceNonSSLSessionPort    bool
}

func NewCommand(client *managerclient.Client) *cobra.Command {
	cmd := &command{
		client: client,
	}
	if err := yaml.Unmarshal(res, &cmd.Command); err != nil {
		panic(err)
	}
	cmd.init()
	cmd.RunE = func(_ *cobra.Command, _ []string) error {
		return cmd.run()
	}
	return &cmd.Command
}

func (cmd *command) init() {
	defer flag.MustSetUsages(&cmd.Command, res, "host")

	w := cmd.Flags()
	w.StringVarP(&cmd.id, "id", "i", "", "")
	w.StringVarP(&cmd.name, "name", "n", "", "")
	w.Var(&cmd.label, "label", "")
	w.StringVar(&cmd.host, "host", "", "")
	w.Int64Var(&cmd.port, "port", 10001, "")
	w.StringVar(&cmd.authToken, "auth-token", "", "")
	w.StringVarP(&cmd.username, "username", "u", "", "")
	w.StringVarP(&cmd.password, "password", "p", "", "")
	w.StringVar(&cmd.alternatorAccessKeyID, "alternator-access-key-id", "", "")
	w.StringVar(&cmd.alternatorSecretAccessKey, "alternator-secret-access-key", "", "")
	w.StringVar(&cmd.sslUserCertFile, "ssl-user-cert-file", "", "")
	w.StringVar(&cmd.sslUserKeyFile, "ssl-user-key-file", "", "")
	w.StringVar(&cmd.cqlCAFile, "cql-ca-file", "", "")
	w.StringVar(&cmd.cqlServerName, "cql-server-name", "", "")
	w.StringVar(&cmd.alternatorCAFile, "alternator-ca-file", "", "")
	w.StringVar(&cmd.alternatorServerName, "alternator-server-name", "", "")
	w.StringVar(&cmd.agentCAFile, "agent-ca-file", "", "")
	w.StringVar(&cmd.agentServerName, "agent-server-name", "", "")
	w.BoolVar(&cmd.withoutRepair, "without-repair", false, "")
	w.BoolVar(&cmd.forceTLSDisabled, "force-tls-disabled", false, "")
	w.BoolVar(&cmd.forceNonSSLSessionPort, "force-non-ssl-session-port", false, "")
}

func (cmd *command) run() error {
	if cmd.id != "" {
		clusters, err := cmd.client.ListClusters(cmd.Context())
		if err != nil {
			return err
		}
		for _, c := range clusters {
			if c.ID == cmd.id {
				return errors.Errorf("Cluster ID %q in use", cmd.id)
			}
		}
	}

	c := &managerclient.Cluster{
		ID:                        cmd.id,
		Name:                      cmd.name,
		Labels:                    cmd.label.NewLabels(),
		Host:                      cmd.host,
		AuthToken:                 cmd.authToken,
		Username:                  cmd.username,
		Password:                  cmd.password,
		AlternatorAccessKeyID:     cmd.alternatorAccessKeyID,
		AlternatorSecretAccessKey: cmd.alternatorSecretAccessKey,
		WithoutRepair:             cmd.withoutRepair,
		ForceTLSDisabled:          cmd.forceTLSDisabled,
		ForceNonSslSessionPort:    cmd.forceNonSSLSessionPort,
	}
	if cmd.port != 10001 {
		c.Port = cmd.port
	}

	if cmd.username != "" && cmd.password == "" {
		return errors.New("missing flag \"password\"")
	}
	if cmd.password != "" && cmd.username == "" {
		return errors.New("missing flag \"username\"")
	}

	if cmd.alternatorAccessKeyID != "" && cmd.alternatorSecretAccessKey == "" {
		return errors.New("missing flag \"alternator-secret-access-key\"")
	}
	if cmd.alternatorSecretAccessKey != "" && cmd.alternatorAccessKeyID == "" {
		return errors.New("missing flag \"alternator-access-key-id\"")
	}

	if cmd.sslUserCertFile != "" && cmd.sslUserKeyFile == "" {
		return errors.New("missing flag \"ssl-user-key-file\"")
	}
	if cmd.sslUserKeyFile != "" && cmd.sslUserCertFile == "" {
		return errors.New("missing flag \"ssl-user-cert-file\"")
	}
	if cmd.authToken == "" {
		return errors.New("missing flag \"auth-token\"")
	}

	w := cmd.OutOrStdout()

	if cmd.sslUserCertFile != "" {
		b0, err := fsutil.ReadFile(cmd.sslUserCertFile)
		if err != nil {
			return err
		}
		c.SslUserCertFile = b0

		b1, err := fsutil.ReadFile(cmd.sslUserKeyFile)
		if err != nil {
			return err
		}
		c.SslUserKeyFile = b1
	}

	trustPairs := []struct {
		caFile     string
		serverName string
		protocol   string
		set        func([]byte, string)
	}{
		{cmd.cqlCAFile, cmd.cqlServerName, "CQL", func(ca []byte, serverName string) {
			c.CqlCaFile, c.CqlServerName = ca, serverName
		}},
		{cmd.alternatorCAFile, cmd.alternatorServerName, "Alternator", func(ca []byte, serverName string) {
			c.AlternatorCaFile, c.AlternatorServerName = ca, serverName
		}},
		{cmd.agentCAFile, cmd.agentServerName, "Agent", func(ca []byte, serverName string) {
			c.AgentCaFile, c.AgentServerName = ca, serverName
		}},
	}
	for _, pair := range trustPairs {
		ca, err := readTLSTrustPair(pair.caFile, pair.serverName, pair.protocol)
		if err != nil {
			return err
		}
		if len(ca) != 0 {
			pair.set(ca, pair.serverName)
		}
	}
	if len(c.AgentCaFile) == 0 {
		return errors.New("missing flags \"agent-ca-file\" and \"agent-server-name\"")
	}

	id, err := cmd.client.CreateCluster(cmd.Context(), c)
	if err != nil {
		return err
	}

	fmt.Fprintln(w, id)

	w = cmd.OutOrStderr()
	if cmd.forceTLSDisabled {
		fmt.Fprintln(w, clusterForceTLSDisabledWarning)
	}
	if cmd.forceNonSSLSessionPort {
		fmt.Fprintln(w, clusterNonSSLPortWarning)
	}

	if err := clusterAddedMessage(w, id, cmd.name); err != nil {
		return err
	}

	if cmd.authToken == "" {
		fmt.Fprintln(w, clusterAddNoAuthTokenWarning)
	}

	return nil
}

func readTLSTrustPair(caFile, serverName, protocol string) ([]byte, error) {
	if caFile == "" && serverName != "" {
		return nil, errors.Errorf("missing %s CA file", protocol)
	}
	if caFile != "" && serverName == "" {
		return nil, errors.Errorf("missing %s server name", protocol)
	}
	if caFile == "" {
		return nil, nil
	}
	ca, err := fsutil.ReadFile(caFile)
	if err != nil {
		return nil, errors.Wrapf(err, "read %s CA file", protocol)
	}
	return ca, nil
}

func clusterAddedMessage(w io.Writer, id, name string) error {
	nameOrID := func() string {
		if name != "" {
			return name
		}
		return id
	}

	nameOrPlaceholder := func() string {
		if name != "" {
			return name
		}
		return "<name>"
	}

	messageLines := []string{
		"Cluster added! You can set it as default, by exporting its name or ID as env variable:",
		"$ export SCYLLA_MANAGER_CLUSTER=" + id,
		"$ export SCYLLA_MANAGER_CLUSTER=" + nameOrPlaceholder(),
		"",
		"Now run:",
		"$ sctool status -c " + nameOrID(),
		"$ sctool tasks -c " + nameOrID(),
	}

	return clipper.Say(w, messageLines...)
}

const clusterAddNoAuthTokenWarning = `
WARNING! Scylla data may be exposed
Protect it by specifying auth_token in /etc/scylla-manager-agent/scylla-manager-agent.yaml on Scylla nodes`

const clusterForceTLSDisabledWarning = `WARNING! TLS is explicitly disabled on cluster sessions (--force-tls-disabled).`

const clusterNonSSLPortWarning = `WARNING! Cluster session is going to always use non SSL port (--force-non-ssl-session-port).`
