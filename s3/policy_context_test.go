package s3

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestPolicyRequestContext checks the condition keys a request supplies.
func TestPolicyRequestContext(t *testing.T) {
	s := &s3{}
	r := httptest.NewRequest(http.MethodGet, "/bucket?prefix=public%2F&delimiter=%2F&max-keys=10", nil)
	r.RemoteAddr = "192.0.2.10:1234"
	r.Header.Set("Referer", "https://example.com")
	r.Header.Set("User-Agent", "test")
	r = r.WithContext(context.WithValue(r.Context(), contentSHA256Key{}, "UNSIGNED-PAYLOAD"))
	caller := *otherCaller
	caller.Region = "us-west-2"
	requestContext := s.requestPolicyContext(r, &caller)
	for key, want := range map[string]string{
		"aws:sourceip":            "192.0.2.10",
		"aws:securetransport":     "false",
		"aws:requestedregion":     "us-west-2",
		"aws:userid":              "other",
		"aws:principalaccount":    "other",
		"aws:principalarn":        "arn:aws:iam::other:root",
		"aws:principaltype":       "Account",
		"aws:referer":             "https://example.com",
		"aws:useragent":           "test",
		"s3:authtype":             "REST-HEADER",
		"s3:signatureversion":     "AWS4-HMAC-SHA256",
		"s3:x-amz-content-sha256": "UNSIGNED-PAYLOAD",
	} {
		if len(requestContext[key]) != 1 || requestContext[key][0] != want {
			t.Fatalf("%s: expected %q, got %v", key, want, requestContext[key])
		}
	}
	if requestContext["s3:signatureage"] != nil {
		t.Fatal("expected no signature age on a header-signed request")
	}
	if _, err := time.Parse(time.RFC3339Nano, requestContext["aws:currenttime"][0]); err != nil {
		t.Fatal(err)
	}
	access := &bucketAccess{request: r, requestContext: requestContext}
	forAction := func(action string) policyContext {
		t.Helper()
		requestContext, err := access.actionPolicyContext(action)
		if err != nil {
			t.Fatal(err)
		}
		return requestContext
	}
	list := forAction(actionListBucket)
	for key, want := range map[string]string{"s3:prefix": "public/", "s3:delimiter": "/", "s3:max-keys": "10"} {
		if len(list[key]) != 1 || list[key][0] != want {
			t.Fatalf("%s: expected %q, got %v", key, want, list[key])
		}
		if forAction(actionGetObject)[key] != nil {
			t.Fatalf("expected no %s on an object read", key)
		}
	}
	r.URL.RawQuery = ""
	if got := forAction(actionListBucket)["s3:max-keys"]; len(got) != 1 || got[0] != "1000" {
		t.Fatalf("expected the default max-keys, got %v", got)
	}
	r.URL.RawQuery = "versionId=null"
	if got := forAction(actionGetObjectVersion)["s3:versionid"]; len(got) != 1 || got[0] != "null" {
		t.Fatalf("expected the null version, got %v", got)
	}
	caller.Presigned = true
	caller.SignedAt = time.Now().Add(-time.Second)
	requestContext = s.requestPolicyContext(r, &caller)
	if got := requestContext["s3:authtype"][0]; got != "REST-QUERY-STRING" {
		t.Fatalf("expected REST-QUERY-STRING, got %q", got)
	}
	age, err := strconv.Atoi(requestContext["s3:signatureage"][0])
	if err != nil || age < 1000 || age > 3000 {
		t.Fatalf("expected a signature age of about 1s, got %v", requestContext["s3:signatureage"])
	}
	r.TLS = &tls.ConnectionState{Version: tls.VersionTLS13}
	requestContext = s.requestPolicyContext(r, nil)
	if got := requestContext["s3:tlsversion"]; len(got) != 1 || got[0] != "1.3" {
		t.Fatalf("expected TLS 1.3, got %v", got)
	} else if got := requestContext["aws:securetransport"][0]; got != "true" {
		t.Fatalf("expected secure transport, got %q", got)
	}
	for _, key := range []string{"aws:username", "aws:principalarn", "s3:signatureversion", "s3:signatureage", "s3:authtype", "aws:sourcevpc", "s3:existingobjecttag/team"} {
		if requestContext[key] != nil {
			t.Fatalf("expected no %s for an anonymous caller, got %v", key, requestContext[key])
		}
	}
}

// TestTrustedProxyContext checks that forwarding headers are only honored from
// trusted proxies.
func TestTrustedProxyContext(t *testing.T) {
	tests := []struct {
		name      string
		peer      string
		forwarded string
		proto     string
		wantIP    string
		wantTLS   string
		trust     bool
	}{
		{"unconfigured", "127.0.0.1:1", "192.0.2.10", "https", "127.0.0.1", "false", false},
		{"untrusted peer", "198.51.100.1:1", "192.0.2.10", "https", "198.51.100.1", "false", true},
		{"trusted peer", "127.0.0.1:1", "192.0.2.10", "https", "192.0.2.10", "true", true},
		{"spoofed left hop", "127.0.0.1:1", "203.0.113.10, 192.0.2.10", "https", "192.0.2.10", "true", true},
		{"trusted chain", "127.0.0.1:1", "192.0.2.10, 127.0.0.2", "http", "192.0.2.10", "false", true},
		{"invalid forwarding", "127.0.0.1:1", "garbage", "https, http", "", "false", true},
		{"IPv6 client", "127.0.0.1:1", "2001:db8::1", "https", "2001:db8::1", "true", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := &s3{}
			if test.trust {
				s.trustedProxies = []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}
			}
			r := httptest.NewRequest(http.MethodGet, "/bucket", nil)
			r.RemoteAddr = test.peer
			r.Header.Set("X-Forwarded-For", test.forwarded)
			r.Header.Set("X-Forwarded-Proto", test.proto)
			requestContext := s.requestPolicyContext(r, nil)
			if got := strings.Join(requestContext["aws:sourceip"], ""); got != test.wantIP {
				t.Fatalf("expected source IP %q, got %q", test.wantIP, got)
			} else if got := requestContext["aws:securetransport"][0]; got != test.wantTLS {
				t.Fatalf("expected secure transport %q, got %q", test.wantTLS, got)
			} else if requestContext["s3:tlsversion"] != nil {
				t.Fatal("expected no TLS version from a forwarded protocol")
			}
		})
	}
}
