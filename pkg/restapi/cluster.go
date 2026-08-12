// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/secrets"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
)

type clusterFilter struct {
	svc ClusterService
}

func (h clusterFilter) clusterCtx(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.svc == nil {
			next.ServeHTTP(w, r)
			return
		}

		clusterID, err := url.QueryUnescape(chi.URLParam(r, "cluster_id"))
		if err != nil {
			respondBadRequest(w, r, errors.New("invalid encoding in cluster ID"))
		}

		if clusterID == "" {
			respondBadRequest(w, r, errors.New("missing cluster ID"))
			return
		}

		c, err := h.svc.GetCluster(r.Context(), clusterID)
		if err != nil {
			respondError(w, r, errors.Wrapf(err, "load cluster %q", clusterID))
			return
		}

		ctx := r.Context()
		ctx = context.WithValue(ctx, ctxClusterID, c.ID)
		ctx = context.WithValue(ctx, ctxCluster, c)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type clusterHandler clusterFilter

func newClusterHandler(svc ClusterService) *chi.Mux {
	m := chi.NewMux()
	h := clusterHandler{
		svc: svc,
	}

	m.Route("/clusters", func(r chi.Router) {
		r.Get("/", h.listClusters)
		r.Post("/", h.createCluster)
	})
	m.Route("/cluster/{cluster_id}", func(r chi.Router) {
		r.Use(clusterFilter(h).clusterCtx)
		r.Get("/", h.loadCluster)
		r.Put("/", h.updateCluster)
		r.Delete("/", h.deleteCluster)
	})
	return m
}

func (h clusterHandler) listClusters(w http.ResponseWriter, r *http.Request) {
	ids, err := h.svc.ListClusters(r.Context(), &cluster.Filter{})
	if err != nil {
		respondError(w, r, errors.Wrap(err, "list clusters"))
		return
	}

	for i, c := range ids {
		ids[i], err = h.sanitizedCluster(c)
		if err != nil {
			respondError(w, r, errors.Wrapf(err, "cluster %s: sanitize response", c.ID))
			return
		}
	}

	if len(ids) == 0 {
		render.Respond(w, r, []struct{}{})
		return
	}
	render.Respond(w, r, ids)
}

func (h clusterHandler) parseCluster(r *http.Request) (*cluster.Cluster, map[string]json.RawMessage, error) {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, nil, err
	}
	var c cluster.Cluster
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil {
		return nil, nil, err
	}
	return &c, fields, nil
}

func (h clusterHandler) createCluster(w http.ResponseWriter, r *http.Request) {
	newCluster, _, err := h.parseCluster(r)
	if err != nil {
		respondBadRequest(w, r, err)
		return
	}

	if err := h.svc.PutCluster(r.Context(), newCluster); err != nil {
		respondError(w, r, errors.Wrap(err, "create cluster"))
		return
	}

	location := r.URL.ResolveReference(&url.URL{
		Path: path.Join("cluster", newCluster.ID.String()),
	})
	w.Header().Set("Location", location.String())
	w.WriteHeader(http.StatusCreated)
}

func (h clusterHandler) loadCluster(w http.ResponseWriter, r *http.Request) {
	c := mustClusterFromCtx(r)
	out, err := h.sanitizedCluster(c)
	if err != nil {
		respondError(w, r, errors.Wrapf(err, "cluster %s: sanitize response", c.ID))
		return
	}
	render.Respond(w, r, out)
}

func (h clusterHandler) updateCluster(w http.ResponseWriter, r *http.Request) {
	c := mustClusterFromCtx(r)

	newCluster, fields, err := h.parseCluster(r)
	if err != nil {
		respondBadRequest(w, r, err)
		return
	}
	newCluster.ID = c.ID
	// Cluster.KnownHosts are not part of REST API definitions,
	// so we need to fill them based on current cluster state.
	newCluster.KnownHosts = c.KnownHosts
	if _, present := fields["auth_token"]; !present {
		newCluster.AuthToken = c.AuthToken
	}

	if err := h.svc.PutCluster(r.Context(), newCluster); err != nil {
		respondError(w, r, errors.Wrapf(err, "update cluster %q", c.ID))
		return
	}
	out, err := h.sanitizedCluster(newCluster)
	if err != nil {
		respondError(w, r, errors.Wrapf(err, "cluster %s: sanitize response", c.ID))
		return
	}
	render.Respond(w, r, out)
}

func (h clusterHandler) sanitizedCluster(c *cluster.Cluster) (*cluster.Cluster, error) {
	out := *c
	out.AuthTokenSet = c.AuthToken != ""
	out.AuthToken = ""
	out.Username = ""
	out.Password = ""
	out.AlternatorAccessKeyID = ""
	out.AlternatorSecretAccessKey = ""
	out.SSLUserCertFile = nil
	out.SSLUserKeyFile = nil
	out.CQLCAFile = nil
	out.CQLServerName = ""
	out.AlternatorCAFile = nil
	out.AlternatorServerName = ""
	out.AgentCAFile = nil
	out.AgentServerName = ""

	checks := []struct {
		name string
		set  *bool
		fn   func() (bool, error)
	}{
		{"CQL credentials", &out.CQLCredentialsSet, func() (bool, error) { return h.svc.CheckCQLCredentials(c.ID) }},
		{"Alternator credentials", &out.AlternatorCredentialsSet, func() (bool, error) { return h.svc.CheckAlternatorCredentials(c.ID) }},
		{"SSL user certificate", &out.SSLUserCertSet, func() (bool, error) { return h.svc.CheckSSLUserCert(c.ID) }},
		{"CQL CA", &out.CQLCASet, func() (bool, error) { return h.svc.CheckTLSTrust(c.ID, secrets.CQLProtocol) }},
		{"Alternator CA", &out.AlternatorCASet, func() (bool, error) { return h.svc.CheckTLSTrust(c.ID, secrets.AlternatorProtocol) }},
		{"Agent CA", &out.AgentCASet, func() (bool, error) { return h.svc.CheckTLSTrust(c.ID, secrets.AgentProtocol) }},
	}
	for _, check := range checks {
		set, err := check.fn()
		if err != nil {
			return nil, errors.Wrap(err, "check "+check.name)
		}
		*check.set = set
	}
	return &out, nil
}

func (h clusterHandler) deleteCluster(w http.ResponseWriter, r *http.Request) {
	c := mustClusterFromCtx(r)
	if r.ContentLength != 0 {
		respondBadRequest(w, r, errors.New("cluster deletion selectors must be supplied as query parameters; request bodies are not accepted"))
		return
	}
	query := r.URL.Query()
	allowedSelectors := map[string]bool{
		"cql_creds": true, "alternator_creds": true, "ssl_user_cert": true,
		"cql_ca": true, "alternator_ca": true, "agent_ca": true,
	}
	for key := range query {
		if !allowedSelectors[key] {
			respondBadRequest(w, r, errors.Errorf("unknown cluster deletion selector %q", key))
			return
		}
	}
	if query.Has("agent_ca") {
		respondBadRequest(w, r, errors.New("Agent TLS trust is mandatory; rotate it through PUT or delete the cluster"))
		return
	}
	selectorPresent := len(query) != 0

	var (
		deleteCQLCredentials        bool
		deleteAlternatorCredentials bool
		deleteSSLUserCert           bool
		deleteCQLCA                 bool
		deleteAlternatorCA          bool
		err                         error
	)

	if v := query.Get("cql_creds"); v != "" {
		deleteCQLCredentials, err = strconv.ParseBool(v)
		if err != nil {
			respondBadRequest(w, r, err)
			return
		}
	}
	if v := query.Get("alternator_creds"); v != "" {
		deleteAlternatorCredentials, err = strconv.ParseBool(v)
		if err != nil {
			respondBadRequest(w, r, err)
			return
		}
	}
	if v := query.Get("ssl_user_cert"); v != "" {
		deleteSSLUserCert, err = strconv.ParseBool(v)
		if err != nil {
			respondBadRequest(w, r, err)
			return
		}
	}
	for name, dst := range map[string]*bool{
		"cql_ca":        &deleteCQLCA,
		"alternator_ca": &deleteAlternatorCA,
	} {
		if v := query.Get(name); v != "" {
			*dst, err = strconv.ParseBool(v)
			if err != nil {
				respondBadRequest(w, r, err)
				return
			}
		}
	}

	deleteAny := deleteCQLCredentials || deleteAlternatorCredentials || deleteSSLUserCert || deleteCQLCA || deleteAlternatorCA
	if !selectorPresent {
		if err := h.svc.DeleteCluster(r.Context(), c.ID); err != nil {
			respondError(w, r, errors.Wrapf(err, "delete cluster %q", c.ID))
			return
		}
	}
	if selectorPresent && !deleteAny {
		respondBadRequest(w, r, errors.New("at least one cluster secret selector must be true"))
		return
	}
	if deleteCQLCredentials {
		if err := h.svc.DeleteCQLCredentials(r.Context(), c.ID); err != nil {
			respondError(w, r, errors.Wrapf(err, "delete CQL credentials for cluster %q", c.ID))
			return
		}
	}
	if deleteAlternatorCredentials {
		if err := h.svc.DeleteAlternatorCredentials(r.Context(), c.ID); err != nil {
			respondError(w, r, errors.Wrapf(err, "delete alternator credentials for cluster %q", c.ID))
			return
		}
	}
	if deleteSSLUserCert {
		if err := h.svc.DeleteSSLUserCert(r.Context(), c.ID); err != nil {
			respondError(w, r, errors.Wrapf(err, "delete SSL user cert for cluster %q", c.ID))
			return
		}
	}
	for protocol, remove := range map[string]bool{
		secrets.CQLProtocol:        deleteCQLCA,
		secrets.AlternatorProtocol: deleteAlternatorCA,
	} {
		if remove {
			if err := h.svc.DeleteTLSTrust(r.Context(), c.ID, protocol); err != nil {
				respondError(w, r, errors.Wrapf(err, "delete %s TLS trust for cluster %q", protocol, c.ID))
				return
			}
		}
	}
}
