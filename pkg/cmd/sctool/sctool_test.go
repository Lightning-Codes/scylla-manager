// Copyright (C) 2017 ScyllaDB

package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestCommandTree(t *testing.T) {
	t.Parallel()

	printCommandTree(buildCommand(), "")
}

func TestTLSOptionsRequireHTTPSAPIURL(t *testing.T) {
	cmd := &rootCommand{
		Command:       cobra.Command{Run: func(*cobra.Command, []string) {}},
		apiURL:        "http://manager.example/api/v1",
		apiCAFile:     "ca.crt",
		apiServerName: "manager.example",
	}
	if err := cmd.preRun(); err == nil || !strings.Contains(err.Error(), "must use https") {
		t.Fatalf("expected plaintext downgrade rejection, got %v", err)
	}
}

func printCommandTree(cmd *cobra.Command, prefix string) {
	s := fmt.Sprint(prefix, " ", cmd.Name())
	if cmd.Runnable() {
		if cmd.Deprecated == "" {
			fmt.Println(s)
		} else {
			fmt.Println("DEPRECATED ", s)
		}
	}
	for _, c := range cmd.Commands() {
		printCommandTree(c, s)
	}
}
