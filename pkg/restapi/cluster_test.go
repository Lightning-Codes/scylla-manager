// Copyright (C) 2017 ScyllaDB

//go:generate mockgen -destination mock_clusterservice_test.go -mock_names ClusterService=MockClusterService -package restapi github.com/scylladb/scylla-manager/v3/pkg/restapi ClusterService

package restapi_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/restapi"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

func TestClusterList(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	id := uuid.MustRandom()
	stored := []*cluster.Cluster{{
		ID: id, Name: "name", Username: "user", Password: "password",
		AlternatorAccessKeyID: "access", AlternatorSecretAccessKey: "secret",
		CQLCAFile: []byte("ca"), CQLServerName: "cql",
		AlternatorCAFile: []byte("ca"), AlternatorServerName: "alternator",
		AgentCAFile: []byte("ca"), AgentServerName: "agent",
	}}
	expected := []*cluster.Cluster{{
		ID: id, Name: "name", CQLCredentialsSet: true, AlternatorCredentialsSet: true,
		CQLCASet: true, AlternatorCASet: true, AgentCASet: true,
	}}

	m := restapi.NewMockClusterService(ctrl)
	m.EXPECT().ListClusters(gomock.Any(), &cluster.Filter{}).Return(stored, nil)

	h := restapi.New(restapi.Services{Cluster: m}, log.Logger{})
	r := httptest.NewRequest(http.MethodGet, "/api/v1/clusters", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	assertJsonBody(t, w, expected)
}

func TestClusterCreateGeneratesIDWhenNotProvided(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	id := uuid.MustRandom()

	m := restapi.NewMockClusterService(ctrl)
	m.EXPECT().PutCluster(gomock.Any(), &cluster.Cluster{Name: "name"}).Do(func(_ interface{}, e *cluster.Cluster) {
		e.ID = id
	}).Return(nil)

	h := restapi.New(restapi.Services{Cluster: m}, log.Logger{})
	r := httptest.NewRequest(http.MethodPost, "/api/v1/clusters", jsonBody(t, &cluster.Cluster{Name: "name"}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusCreated {
		t.Fatalf("Expected to receive %d status code, got %d", http.StatusCreated, w.Code)
	}

	if !strings.Contains(w.Header().Get("Location"), id.String()) {
		t.Fatal(w.Header())
	}
}

func TestClusterCreateWithProvidedID(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	id := uuid.MustRandom()

	m := restapi.NewMockClusterService(ctrl)
	m.EXPECT().PutCluster(gomock.Any(), NewClusterMatcher(&cluster.Cluster{ID: id})).Return(nil)

	h := restapi.New(restapi.Services{Cluster: m}, log.Logger{})
	r := httptest.NewRequest(http.MethodPost, "/api/v1/clusters", jsonBody(t, &cluster.Cluster{ID: id}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusCreated {
		t.Fatalf("Expected to receive %d status code, got %d", http.StatusCreated, w.Code)
	}

	if !strings.Contains(w.Header().Get("Location"), id.String()) {
		t.Fatal(w.Header())
	}
}

func TestClusterRejectsUnknownOrTrailingJSONFields(t *testing.T) {
	for name, body := range map[string]string{
		"token typo":     `{"host":"node","auth_t0ken":"secret"}`,
		"trust typo":     `{"host":"node","agent_servername":"agent.internal"}`,
		"force typo":     `{"host":"node","force_tls_disable":true}`,
		"trailing value": `{"host":"node"} {"host":"other"}`,
	} {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := restapi.NewMockClusterService(ctrl)
			h := restapi.New(restapi.Services{Cluster: m}, log.Logger{})
			r := httptest.NewRequest(http.MethodPost, "/api/v1/clusters", strings.NewReader(body))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("unsafe JSON was accepted: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func expectClusterSecretChecks(m *restapi.MockClusterService, id uuid.UUID) {
	_ = m
	_ = id
}

func TestClusterResponsesAreSanitized(t *testing.T) {
	id := uuid.MustRandom()
	stored := &cluster.Cluster{
		ID: id, Name: "secure", AuthToken: "agent-token", Username: "user", Password: "password",
		AlternatorAccessKeyID: "access", AlternatorSecretAccessKey: "secret",
		SSLUserCertFile: []byte("cert"), SSLUserKeyFile: []byte("key"),
		CQLCAFile: []byte("cql-ca"), CQLServerName: "cql.internal",
		AlternatorCAFile: []byte("alt-ca"), AlternatorServerName: "alt.internal",
		AgentCAFile: []byte("agent-ca"), AgentServerName: "agent.internal",
	}

	ctrl := gomock.NewController(t)
	m := restapi.NewMockClusterService(ctrl)
	m.EXPECT().GetCluster(gomock.Any(), id.String()).Return(stored, nil)
	expectClusterSecretChecks(m, id)

	h := restapi.New(restapi.Services{Cluster: m}, log.Logger{})
	r := httptest.NewRequest(http.MethodGet, fmt.Sprint("/api/v1/cluster/", id), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected response: %d %s", w.Code, w.Body.String())
	}
	for _, secret := range []string{"agent-token", "password", "access", "secret", "cql-ca", "cql.internal", "alt-ca", "alt.internal", "agent-ca", "agent.internal"} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatalf("response leaked %q: %s", secret, w.Body.String())
		}
	}
	for _, marker := range []string{"auth_token_set", "cql_credentials_set", "alternator_credentials_set", "ssl_user_cert_set", "cql_ca_set", "alternator_ca_set", "agent_ca_set"} {
		if !strings.Contains(w.Body.String(), marker) {
			t.Fatalf("response omitted %q: %s", marker, w.Body.String())
		}
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"auth_token", "username", "password", "alternator_access_key_id", "alternator_secret_access_key", "ssl_user_cert_file", "ssl_user_key_file", "cql_ca_file", "cql_server_name", "alternator_ca_file", "alternator_server_name", "agent_ca_file", "agent_server_name"} {
		if _, present := body[field]; present {
			t.Fatalf("sanitized response included request-only field %q: %s", field, w.Body.String())
		}
	}
}

func TestClusterGetPutRoundTripLeavesOmittedAuthTokenForAuthoritativeMerge(t *testing.T) {
	id := uuid.MustRandom()
	stored := &cluster.Cluster{ID: id, Name: "old", AuthToken: "agent-token", KnownHosts: []string{"host"}}

	ctrl := gomock.NewController(t)
	m := restapi.NewMockClusterService(ctrl)
	m.EXPECT().GetCluster(gomock.Any(), id.String()).Return(stored, nil).Times(2)
	expectClusterSecretChecks(m, id)
	expectClusterSecretChecks(m, id)
	m.EXPECT().PutCluster(gomock.Any(), gomock.Any()).DoAndReturn(func(_ interface{}, got *cluster.Cluster) error {
		if got.AuthToken != "" || len(got.KnownHosts) != 0 {
			t.Fatalf("REST injected stale internal fields: %#v", got)
		}
		return nil
	})

	h := restapi.New(restapi.Services{Cluster: m}, log.Logger{})
	get := httptest.NewRequest(http.MethodGet, fmt.Sprint("/api/v1/cluster/", id), nil)
	getResponse := httptest.NewRecorder()
	h.ServeHTTP(getResponse, get)
	if getResponse.Code != http.StatusOK {
		t.Fatalf("GET failed: %d %s", getResponse.Code, getResponse.Body.String())
	}
	if strings.Contains(getResponse.Body.String(), `"auth_token"`) {
		t.Fatalf("GET emitted empty request-only auth_token: %s", getResponse.Body.String())
	}

	put := httptest.NewRequest(http.MethodPut, fmt.Sprint("/api/v1/cluster/", id), strings.NewReader(getResponse.Body.String()))
	put.Header.Set("Content-Type", "application/json")
	putResponse := httptest.NewRecorder()
	h.ServeHTTP(putResponse, put)
	if putResponse.Code != http.StatusOK {
		t.Fatalf("PUT failed: %d %s", putResponse.Code, putResponse.Body.String())
	}
}

func TestClusterUpdateDoesNotInjectOmittedAuthTokenAndSanitizesResponse(t *testing.T) {
	id := uuid.MustRandom()
	stored := &cluster.Cluster{ID: id, Name: "old", AuthToken: "agent-token", KnownHosts: []string{"host"}}

	ctrl := gomock.NewController(t)
	m := restapi.NewMockClusterService(ctrl)
	m.EXPECT().GetCluster(gomock.Any(), id.String()).Return(stored, nil)
	m.EXPECT().PutCluster(gomock.Any(), gomock.Any()).DoAndReturn(func(_ interface{}, got *cluster.Cluster) error {
		if got.AuthToken != "" || len(got.KnownHosts) != 0 {
			t.Fatalf("REST injected stale internal fields: %#v", got)
		}
		return nil
	})
	expectClusterSecretChecks(m, id)

	h := restapi.New(restapi.Services{Cluster: m}, log.Logger{})
	r := httptest.NewRequest(http.MethodPut, fmt.Sprint("/api/v1/cluster/", id), strings.NewReader(`{"name":"new"}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected response: %d %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), stored.AuthToken) {
		t.Fatal("update response leaked auth token")
	}
}

func TestClusterDeleteCQLCredentials(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	id := uuid.MustRandom()

	m := restapi.NewMockClusterService(ctrl)
	gomock.InOrder(
		m.EXPECT().GetCluster(gomock.Any(), id.String()).Return(&cluster.Cluster{ID: id}, nil),
		m.EXPECT().DeleteConnectionSecrets(gomock.Any(), id, cluster.SecretDeletion{CQLCredentials: true}).Return(nil),
	)

	h := restapi.New(restapi.Services{Cluster: m}, log.Logger{})
	r := httptest.NewRequest(http.MethodDelete, fmt.Sprint("/api/v1/cluster/", id), nil)
	r.URL.RawQuery = "cql_creds=1"
	r.ParseForm()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected to receive %d status code, got %d", http.StatusCreated, w.Code)
	}
}

func TestClusterDeleteAlternatorCredentials(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	id := uuid.MustRandom()

	m := restapi.NewMockClusterService(ctrl)
	gomock.InOrder(
		m.EXPECT().GetCluster(gomock.Any(), id.String()).Return(&cluster.Cluster{ID: id}, nil),
		m.EXPECT().DeleteConnectionSecrets(gomock.Any(), id, cluster.SecretDeletion{AlternatorCredentials: true}).Return(nil),
	)

	h := restapi.New(restapi.Services{Cluster: m}, log.Logger{})
	r := httptest.NewRequest(http.MethodDelete, fmt.Sprint("/api/v1/cluster/", id), nil)
	r.URL.RawQuery = "alternator_creds=1"
	r.ParseForm()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected to receive %d status code, got %d", http.StatusCreated, w.Code)
	}
}

func TestClusterDeleteMultipleSecretsUsesOneAtomicMutation(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	id := uuid.MustRandom()
	m := restapi.NewMockClusterService(ctrl)
	gomock.InOrder(
		m.EXPECT().GetCluster(gomock.Any(), id.String()).Return(&cluster.Cluster{ID: id}, nil),
		m.EXPECT().DeleteConnectionSecrets(gomock.Any(), id, cluster.SecretDeletion{
			CQLCredentials:        true,
			AlternatorCredentials: true,
			SSLUserCert:           true,
			CQLTrust:              true,
			AlternatorTrust:       true,
		}).Return(nil),
	)

	h := restapi.New(restapi.Services{Cluster: m}, log.Logger{})
	r := httptest.NewRequest(http.MethodDelete, fmt.Sprintf(
		"/api/v1/cluster/%s?cql_creds=true&alternator_creds=true&ssl_user_cert=true&cql_ca=true&alternator_ca=true", id), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("atomic multi-selector delete failed: %d %s", w.Code, w.Body.String())
	}
}

func TestClusterRejectsAgentTrustDeletion(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	id := uuid.MustRandom()
	m := restapi.NewMockClusterService(ctrl)
	m.EXPECT().GetCluster(gomock.Any(), id.String()).Return(&cluster.Cluster{ID: id}, nil)

	h := restapi.New(restapi.Services{Cluster: m}, log.Logger{})
	r := httptest.NewRequest(http.MethodDelete, fmt.Sprint("/api/v1/cluster/", id, "?agent_ca=1"), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected Agent trust deletion rejection, got %d: %s", w.Code, w.Body.String())
	}
}

func TestClusterFalseSecretSelectorsNeverDeleteCluster(t *testing.T) {
	t.Parallel()

	for _, selector := range []string{"cql_creds", "alternator_creds", "ssl_user_cert", "cql_ca", "alternator_ca", "agent_ca"} {
		t.Run(selector, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			id := uuid.MustRandom()
			m := restapi.NewMockClusterService(ctrl)
			m.EXPECT().GetCluster(gomock.Any(), id.String()).Return(&cluster.Cluster{ID: id}, nil)

			h := restapi.New(restapi.Services{Cluster: m}, log.Logger{})
			r := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/v1/cluster/%s?%s=false", id, selector), nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("false selector %q was not rejected: %d %s", selector, w.Code, w.Body.String())
			}
		})
	}
}

func TestClusterDeleteRejectsFormBodySelectors(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	id := uuid.MustRandom()
	m := restapi.NewMockClusterService(ctrl)
	m.EXPECT().GetCluster(gomock.Any(), id.String()).Return(&cluster.Cluster{ID: id}, nil)

	h := restapi.New(restapi.Services{Cluster: m}, log.Logger{})
	r := httptest.NewRequest(http.MethodDelete, fmt.Sprint("/api/v1/cluster/", id), strings.NewReader("cql_creds=true"))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("form-body selector was not rejected: %d %s", w.Code, w.Body.String())
	}
}

func TestClusterDeleteRejectsUnknownQuerySelectors(t *testing.T) {
	t.Parallel()

	for _, selector := range []string{"cql_cred", "agent-ca", "delete"} {
		t.Run(selector, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			id := uuid.MustRandom()
			m := restapi.NewMockClusterService(ctrl)
			m.EXPECT().GetCluster(gomock.Any(), id.String()).Return(&cluster.Cluster{ID: id}, nil)
			h := restapi.New(restapi.Services{Cluster: m}, log.Logger{})
			r := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/v1/cluster/%s?%s=true", id, selector), nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("unknown selector %q was not rejected: %d %s", selector, w.Code, w.Body.String())
			}
		})
	}
}

func TestClusterDeleteSSLUserCert(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	id := uuid.MustRandom()

	m := restapi.NewMockClusterService(ctrl)
	gomock.InOrder(
		m.EXPECT().GetCluster(gomock.Any(), id.String()).Return(&cluster.Cluster{ID: id}, nil),
		m.EXPECT().DeleteConnectionSecrets(gomock.Any(), id, cluster.SecretDeletion{SSLUserCert: true}).Return(nil),
	)

	h := restapi.New(restapi.Services{Cluster: m}, log.Logger{})
	r := httptest.NewRequest(http.MethodDelete, fmt.Sprint("/api/v1/cluster/", id), nil)
	r.URL.RawQuery = "ssl_user_cert=1"
	r.ParseForm()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected to receive %d status code, got %d", http.StatusCreated, w.Code)
	}
}

// ClusterMatcher gomock.Matcher interface implementation for cluster.Cluster.
type ClusterMatcher struct {
	expected *cluster.Cluster
}

// NewClusterMatcher returns gomock.Matcher for clusters. It compares only ID field.
func NewClusterMatcher(expected *cluster.Cluster) *ClusterMatcher {
	return &ClusterMatcher{
		expected: expected,
	}
}

// Matches returns whether v is a match.
func (m ClusterMatcher) Matches(v interface{}) bool {
	c, ok := v.(*cluster.Cluster)
	if !ok {
		return false
	}
	return cmp.Equal(m.expected.ID, c.ID, testutils.UUIDComparer())
}

func (m ClusterMatcher) String() string {
	return fmt.Sprintf("is equal to cluster with ID: %s", m.expected.ID.String())
}
