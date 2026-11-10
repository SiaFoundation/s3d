package main

import (
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
)

func TestAdminClientErrors(t *testing.T) {
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
	} else if !strings.Contains(err.Error(), "not reachable at "+addr) {
		t.Fatal("unexpected", err)
	} else if !errors.As(err, &opErr) {
		t.Fatal("unexpected", err)
	}
}
