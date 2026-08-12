// Copyright (C) 2017 ScyllaDB

package main

import (
	"crypto/tls"
	"net/url"
	"os"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg"
	"github.com/scylladb/scylla-manager/v3/pkg/command/flag"
	"github.com/scylladb/scylla-manager/v3/pkg/managerclient"
	"github.com/scylladb/scylla-manager/v3/pkg/util/cfgutil"
	"github.com/spf13/cobra"
)

type rootCommand struct {
	cobra.Command

	client *managerclient.Client

	apiURL        string
	apiCertFile   string
	apiKeyFile    string
	apiCAFile     string
	apiServerName string
}

func newRootCommand(client *managerclient.Client) *cobra.Command {
	cmd := &rootCommand{
		Command: cobra.Command{
			Use:   "sctool",
			Short: "Scylla Manager " + pkg.Version(),
			Long:  "Scylla Manager " + pkg.Version() + ".\n\nDocumentation is available online at https://manager.docs.scylladb.com/.",
		},
		client: client,
	}
	cmd.init()
	cmd.PersistentPreRunE = func(_ *cobra.Command, _ []string) error {
		return cmd.preRun()
	}
	return &cmd.Command
}

func (cmd *rootCommand) init() {
	w := flag.Wrap(cmd.PersistentFlags())
	w.GlobalAPIURL(&cmd.apiURL, apiURL())
	w.GlobalAPICertFile(&cmd.apiCertFile)
	w.GlobalAPIKeyFile(&cmd.apiKeyFile)
	w.GlobalAPICAFile(&cmd.apiCAFile)
	w.GlobalAPIServerName(&cmd.apiServerName)
}

func (cmd *rootCommand) preRun() error {
	if cmd.IsAdditionalHelpTopicCommand() {
		return nil
	}

	if cmd.apiCertFile != "" && cmd.apiKeyFile == "" {
		return errors.New("missing --api-key-file flag")
	}
	if cmd.apiKeyFile != "" && cmd.apiCertFile == "" {
		return errors.New("missing --api-cert-file flag")
	}
	if cmd.apiCAFile == "" && cmd.apiServerName != "" {
		return errors.New("missing --api-ca-file flag")
	}
	if cmd.apiCAFile != "" && cmd.apiServerName == "" {
		return errors.New("missing --api-server-name flag")
	}
	parsedURL, err := url.Parse(cmd.apiURL)
	if err != nil {
		return errors.Wrap(err, "parse Manager API URL")
	}
	if (cmd.apiCAFile != "" || cmd.apiCertFile != "") && parsedURL.Scheme != "https" {
		return errors.New("Manager API URL must use https when TLS options are configured")
	}

	var opts []managerclient.Option
	var certificate *tls.Certificate
	if cmd.apiCertFile != "" {
		cert, err := tls.LoadX509KeyPair(cmd.apiCertFile, cmd.apiKeyFile)
		if err != nil {
			return errors.Wrap(err, "load client certificate")
		}
		certificate = &cert
	}
	if cmd.apiCAFile != "" {
		ca, err := os.ReadFile(cmd.apiCAFile)
		if err != nil {
			return errors.Wrap(err, "load Manager API CA")
		}
		tlsConfig, err := managerclient.StrictTLSConfig(ca, cmd.apiServerName, certificate)
		if err != nil {
			return err
		}
		opts = append(opts, managerclient.WithTLSConfig(tlsConfig))
	} else if certificate != nil {
		tlsConfig := managerclient.DefaultTLSConfig()
		tlsConfig.Certificates = []tls.Certificate{*certificate}
		opts = append(opts, managerclient.WithTLSConfig(tlsConfig))
	}

	c, err := managerclient.NewClient(cmd.apiURL, opts...)
	if err != nil {
		return err
	}

	*cmd.client = c
	return nil
}

func apiURL() string {
	if v := os.Getenv("SCYLLA_MANAGER_API_URL"); v != "" {
		return v
	}

	c := &struct {
		HTTP  string `yaml:"http"`
		HTTPS string `yaml:"https"`
	}{}
	if err := cfgutil.PermissiveParseYAML(&c, "/etc/scylla-manager/scylla-manager.yaml"); err == nil {
		if v := baseURL(c.HTTP, c.HTTPS); v != "" {
			return v
		}
	}
	return "http://127.0.0.1:5080/api/v1"
}
