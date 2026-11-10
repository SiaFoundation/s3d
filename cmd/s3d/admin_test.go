package main

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"go.sia.tech/jape"
)

func TestAdminClientErrors(t *testing.T) {
	origFile := configFile
	t.Cleanup(func() { configFile = origFile })
	configFile = "s3d.yml"

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()

	client := adminClient{addr: addr, password: "password"}
	var opErr *net.OpError
	err = client.request(t.Context(), http.MethodGet, "/stats/uploads", nil, nil)
	if err == nil {
		t.Fatal("expected an error")
	} else if err.Error() != "s3d is not reachable at "+addr+", check that it is running and that adminAddress in s3d.yml is correct" {
		t.Fatal("unexpected", err)
	} else if !errors.As(err, &opErr) {
		t.Fatal("unexpected", err)
	}

	backend, _ := testutil.NewBackend(t)
	server := httptest.NewServer(jape.BasicAuth("password")(s3.NewAdmin(backend)))
	t.Cleanup(server.Close)
	client.addr = strings.TrimPrefix(server.URL, "http://")

	client.password = "wrong"
	err = client.request(t.Context(), http.MethodGet, "/stats/uploads", nil, nil)
	if err == nil {
		t.Fatal("expected an error")
	} else if err.Error() != "s3d rejected the admin password, check adminPassword in s3d.yml" {
		t.Fatal("unexpected", err)
	}

	client.password = "password"
	err = client.request(t.Context(), http.MethodPut, "/settings/snapshots", s3.SnapshotSchedule{Expression: "@daily"}, nil)
	if err == nil {
		t.Fatal("expected an error")
	} else if err.Error() != "a snapshot schedule requires at least one positive retention count" {
		t.Fatal("unexpected", err)
	}
}
