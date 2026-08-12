// Copyright (C) 2017 ScyllaDB

package clusterupdate

import (
	_ "embed"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/command/flag"
	"github.com/scylladb/scylla-manager/v3/pkg/managerclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/fsutil"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

//go:embed res.yaml
var res []byte

type command struct {
	cobra.Command

	client *managerclient.Client

	cluster                     string
	name                        string
	label                       flag.Label
	host                        string
	port                        int64
	authToken                   string
	username                    string
	password                    string
	alternatorAccessKeyID       string
	alternatorSecretAccessKey   string
	sslUserCertFile             string
	sslUserKeyFile              string
	cqlCAFile                   string
	cqlServerName               string
	alternatorCAFile            string
	alternatorServerName        string
	agentCAFile                 string
	agentServerName             string
	deleteCQLCredentials        bool
	deleteAlternatorCredentials bool
	deleteSSLUserCert           bool
	deleteCQLCA                 bool
	deleteAlternatorCA          bool
	forceTLSDisabled            bool
	forceNonSSLSessionPort      bool
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
	defer flag.MustSetUsages(&cmd.Command, res, "cluster")

	w := flag.Wrap(cmd.Flags())
	w.Cluster(&cmd.cluster)
	w.Unwrap().StringVarP(&cmd.name, "name", "n", "", "")
	w.Unwrap().Var(&cmd.label, "label", "")
	w.Unwrap().StringVar(&cmd.host, "host", "", "")
	w.Unwrap().Int64Var(&cmd.port, "port", 10001, "")
	w.Unwrap().StringVar(&cmd.authToken, "auth-token", "", "")
	w.Unwrap().StringVarP(&cmd.username, "username", "u", "", "")
	w.Unwrap().StringVarP(&cmd.password, "password", "p", "", "")
	w.Unwrap().StringVar(&cmd.alternatorAccessKeyID, "alternator-access-key-id", "", "")
	w.Unwrap().StringVar(&cmd.alternatorSecretAccessKey, "alternator-secret-access-key", "", "")
	w.Unwrap().StringVar(&cmd.sslUserCertFile, "ssl-user-cert-file", "", "")
	w.Unwrap().StringVar(&cmd.sslUserKeyFile, "ssl-user-key-file", "", "")
	w.Unwrap().StringVar(&cmd.cqlCAFile, "cql-ca-file", "", "")
	w.Unwrap().StringVar(&cmd.cqlServerName, "cql-server-name", "", "")
	w.Unwrap().StringVar(&cmd.alternatorCAFile, "alternator-ca-file", "", "")
	w.Unwrap().StringVar(&cmd.alternatorServerName, "alternator-server-name", "", "")
	w.Unwrap().StringVar(&cmd.agentCAFile, "agent-ca-file", "", "")
	w.Unwrap().StringVar(&cmd.agentServerName, "agent-server-name", "", "")
	w.Unwrap().BoolVar(&cmd.deleteCQLCredentials, "delete-cql-credentials", false, "")
	w.Unwrap().BoolVar(&cmd.deleteAlternatorCredentials, "delete-alternator-credentials", false, "")
	w.Unwrap().BoolVar(&cmd.deleteSSLUserCert, "delete-ssl-user-cert", false, "")
	w.Unwrap().BoolVar(&cmd.deleteCQLCA, "delete-cql-ca", false, "")
	w.Unwrap().BoolVar(&cmd.deleteAlternatorCA, "delete-alternator-ca", false, "")
	w.Unwrap().BoolVar(&cmd.forceTLSDisabled, "force-tls-disabled", false, "")
	w.Unwrap().BoolVar(&cmd.forceNonSSLSessionPort, "force-non-ssl-session-port", false, "")
}

func (cmd *command) run() error {
	cluster, err := cmd.client.GetCluster(cmd.Context(), cmd.cluster)
	if err != nil {
		return err
	}

	ok := false
	if cmd.Flags().Changed("name") {
		cluster.Name = cmd.name
		ok = true
	}
	if cmd.Flags().Changed("label") {
		cluster.Labels = cmd.label.ApplyDiff(cluster.Labels)
		ok = true
	}
	if cmd.Flags().Changed("host") {
		cluster.Host = cmd.host
		ok = true
	}
	if cmd.Flags().Changed("port") {
		cluster.Port = cmd.port
		ok = true
	}
	if cmd.Flags().Changed("username") {
		cluster.Username = cmd.username
		ok = true
	}
	if cmd.Flags().Changed("password") {
		cluster.Password = cmd.password
		ok = true
	}
	if cmd.Flags().Changed("alternator-access-key-id") {
		cluster.AlternatorAccessKeyID = cmd.alternatorAccessKeyID
		ok = true
	}
	if cmd.Flags().Changed("alternator-secret-access-key") {
		cluster.AlternatorSecretAccessKey = cmd.alternatorSecretAccessKey
		ok = true
	}
	if cmd.Flags().Changed("auth-token") {
		cluster.AuthToken = cmd.authToken
		ok = true
	}
	if cmd.Flags().Changed("force-tls-disabled") {
		cluster.ForceTLSDisabled = cmd.forceTLSDisabled
		ok = true
	}
	if cmd.Flags().Changed("force-non-ssl-session-port") {
		cluster.ForceNonSslSessionPort = cmd.forceNonSSLSessionPort
		ok = true
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

	if cmd.Flags().Changed("ssl-user-cert-file") {
		if cmd.sslUserKeyFile == "" {
			return errors.New("missing flag \"ssl-user-key-file\"")
		}
		b, err := fsutil.ReadFile(cmd.sslUserCertFile)
		if err != nil {
			return err
		}
		cluster.SslUserCertFile = b
		ok = true
	}
	if cmd.Flags().Changed("ssl-user-key-file") {
		if cmd.sslUserCertFile == "" {
			return errors.New("missing flag \"ssl-user-cert-file\"")
		}
		b, err := fsutil.ReadFile(cmd.sslUserKeyFile)
		if err != nil {
			return err
		}
		cluster.SslUserKeyFile = b
		ok = true
	}

	trustPairs := []struct {
		caFlag         string
		serverNameFlag string
		caFile         string
		serverName     string
		protocol       string
		remove         bool
		set            func([]byte, string)
	}{
		{"cql-ca-file", "cql-server-name", cmd.cqlCAFile, cmd.cqlServerName, "CQL", cmd.deleteCQLCA, func(ca []byte, serverName string) {
			cluster.CqlCaFile, cluster.CqlServerName = ca, serverName
		}},
		{"alternator-ca-file", "alternator-server-name", cmd.alternatorCAFile, cmd.alternatorServerName, "Alternator", cmd.deleteAlternatorCA, func(ca []byte, serverName string) {
			cluster.AlternatorCaFile, cluster.AlternatorServerName = ca, serverName
		}},
		{"agent-ca-file", "agent-server-name", cmd.agentCAFile, cmd.agentServerName, "Agent", false, func(ca []byte, serverName string) {
			cluster.AgentCaFile, cluster.AgentServerName = ca, serverName
		}},
	}
	for _, pair := range trustPairs {
		caChanged := cmd.Flags().Changed(pair.caFlag)
		serverNameChanged := cmd.Flags().Changed(pair.serverNameFlag)
		if caChanged != serverNameChanged {
			return errors.Errorf("flags \"%s\" and \"%s\" must be changed together", pair.caFlag, pair.serverNameFlag)
		}
		if !caChanged {
			continue
		}
		if pair.remove {
			return errors.Errorf("cannot set and delete %s TLS trust in one update", pair.protocol)
		}
		ca, err := readTLSTrustPair(pair.caFile, pair.serverName, pair.protocol)
		if err != nil {
			return err
		}
		pair.set(ca, pair.serverName)
		ok = true
	}

	deleteOptions := managerclient.ClusterSecretDeleteOptions{
		CQLCredentials:        cmd.deleteCQLCredentials,
		AlternatorCredentials: cmd.deleteAlternatorCredentials,
		SSLUserCert:           cmd.deleteSSLUserCert,
		CQLCA:                 cmd.deleteCQLCA,
		AlternatorCA:          cmd.deleteAlternatorCA,
	}
	deleteAny := deleteOptions.CQLCredentials || deleteOptions.AlternatorCredentials || deleteOptions.SSLUserCert ||
		deleteOptions.CQLCA || deleteOptions.AlternatorCA
	if !ok && !deleteAny {
		return errors.New("nothing to do")
	}
	if ok {
		if err := cmd.client.UpdateCluster(cmd.Context(), cluster); err != nil {
			return err
		}
	}
	if deleteAny {
		return cmd.client.DeleteClusterSecretsWithOptions(cmd.Context(), cmd.cluster, deleteOptions)
	}
	return nil
}

func readTLSTrustPair(caFile, serverName, protocol string) ([]byte, error) {
	if caFile == "" {
		return nil, errors.Errorf("%s CA file cannot be empty", protocol)
	}
	if serverName == "" {
		return nil, errors.Errorf("%s server name cannot be empty", protocol)
	}
	ca, err := fsutil.ReadFile(caFile)
	if err != nil {
		return nil, errors.Wrapf(err, "read %s CA file", protocol)
	}
	return ca, nil
}
