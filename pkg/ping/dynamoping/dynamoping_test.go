// Copyright (C) 2017 ScyllaDB

package dynamoping

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/scylladb/scylla-manager/v3/pkg/ping"
)

func TestPingTimeout(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				select {
				case <-time.After(time.Second):
					return
				case <-done:
					return
				}
			}()
			select {
			case <-done:
				return
			default:
			}
		}
	}()

	config := Config{
		Addr:    "http://" + l.Addr().String(),
		Timeout: 250 * time.Millisecond,
	}

	t.Run("simple", func(t *testing.T) {
		d, err := SimplePing(context.Background(), config)
		if err != ping.ErrTimeout {
			t.Errorf("simplePing() error %s, expected timeout", err)
		}
		if a, b := epsilonRange(config.Timeout); d < a || d > b {
			t.Errorf("simplePing() not within expected time margin %v got %v", config.Timeout, d)
		}
	})

	t.Run("query", func(t *testing.T) {
		d, err := QueryPing(context.Background(), config)
		if err != ping.ErrTimeout {
			t.Errorf("QueryPing() error %s, expected timeout", err)
		}
		if a, b := epsilonRange(config.Timeout); d < a || d > b {
			t.Errorf("QueryPing() not within expected time margin %v got %v", config.Timeout, d)
		}
	})
}

func TestAuthenticatedQueryPingIsSigned(t *testing.T) {
	var authorization string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		_, _ = w.Write([]byte(`{"Count":0,"Items":[],"ScannedCount":0}`))
	}))
	defer server.Close()

	config := Config{
		Addr:                   server.URL,
		Timeout:                time.Second,
		RequiresAuthentication: true,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test-access", SecretAccessKey: "test-secret"}, nil
		}),
	}
	if _, err := QueryPing(context.Background(), config); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(authorization, "AWS4-HMAC-SHA256 ") {
		t.Fatalf("query was not SigV4 signed: %q", authorization)
	}
}

func TestAuthenticatedQueryPingRequiresCredentials(t *testing.T) {
	_, err := QueryPing(context.Background(), Config{
		Addr:                   "http://127.0.0.1",
		Timeout:                time.Second,
		RequiresAuthentication: true,
	})
	if err != ErrAlternatorQueryPingNotSupported {
		t.Fatalf("expected missing credentials error, got %v", err)
	}
}

func epsilonRange(d time.Duration) (time.Duration, time.Duration) {
	e := time.Duration(float64(d) * 1.05)
	return d - e, d + e
}
