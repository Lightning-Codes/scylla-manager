// Copyright (C) 2017 ScyllaDB
package restapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
)

func TestRespondErrorLogsInternalCauseWithoutLeakingIt(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request = request.WithContext(context.WithValue(request.Context(), loggerContextKey{}, log.NewDevelopment()))
	response := httptest.NewRecorder()

	respondError(response, request, errors.New("catalog parse failed"))

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", response.Code)
	}
	if strings.Contains(response.Body.String(), "catalog parse failed") {
		t.Fatal("internal cause leaked to API response")
	}
}

func TestRespondError(t *testing.T) {
	request, _ := http.NewRequest(http.MethodGet, "/", nil)

	t.Run("not found", func(t *testing.T) {
		err := errors.Wrap(gocql.ErrNotFound, "wrapped")
		response := httptest.NewRecorder()

		respondError(response, request, errors.Wrap(err, "specific_msg"))
		expected := `{"message":"get resource: specific_msg: wrapped: not found","details":"","trace_id":""}` + "\n"
		if diff := cmp.Diff(response.Body.String(), expected); diff != "" {
			t.Fatal(diff)
		}
		if response.Code != http.StatusNotFound {
			t.Errorf("Response status is wrong, got '%d' want '%d'", response.Code, http.StatusNotFound)
		}
	})

	t.Run("validation", func(t *testing.T) {
		err := util.ErrValidate(errors.New("some problem"))
		response := httptest.NewRecorder()

		respondError(response, request, errors.Wrap(err, "specific_msg"))
		expected := `{"message":"specific_msg: some problem","details":"","trace_id":""}` + "\n"
		if diff := cmp.Diff(response.Body.String(), expected); diff != "" {
			t.Fatal(diff)
		}
		if response.Code != http.StatusBadRequest {
			t.Errorf("Response status is wrong, got '%d' want '%d'", response.Code, http.StatusBadRequest)
		}
	})

	t.Run("internal errors", func(t *testing.T) {
		err := errors.Wrap(errors.New("unknown problem"), "wrapped")
		response := httptest.NewRecorder()

		respondError(response, request, errors.Wrap(err, "specific_msg"))
		expected := `{"message":"internal server error; see Manager logs with the trace ID","details":"","trace_id":""}` + "\n"
		if diff := cmp.Diff(response.Body.String(), expected); diff != "" {
			t.Fatal(diff)
		}

		if response.Code != http.StatusInternalServerError {
			t.Errorf("Response status is wrong, got '%d' want '%d'", response.Code, http.StatusInternalServerError)
		}
	})

	t.Run("data-plane validation does not expose endpoint identity", func(t *testing.T) {
		err := util.ErrValidate(errors.Wrap(cluster.ErrSecureConnectivity, "x509: certificate is valid for secret.internal.example"))
		response := httptest.NewRecorder()

		respondError(response, request, errors.Wrap(err, "update cluster"))
		expected := `{"message":"secure cluster connectivity validation failed; see Manager logs with the trace ID","details":"","trace_id":""}` + "\n"
		if diff := cmp.Diff(response.Body.String(), expected); diff != "" {
			t.Fatal(diff)
		}
		if response.Code != http.StatusBadRequest {
			t.Fatalf("unexpected status %d", response.Code)
		}
		if strings.Contains(response.Body.String(), "secret.internal.example") {
			t.Fatal("response exposed the configured server name")
		}
	})
}
