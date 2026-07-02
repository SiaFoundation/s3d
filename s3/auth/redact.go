package auth

import (
	"net/url"
	"strings"
)

// redacted replaces a secret value in log output.
const redacted = "REDACTED"

// secretQueryParams are the presigned-URL query parameters that carry secret
// data and must be redacted before logging.
var secretQueryParams = []string{"X-Amz-Signature", "X-Amz-Security-Token"}

// RedactAuthorization returns a copy of an Authorization header value with its
// signature replaced by a placeholder for logging. The algorithm, credential
// and SignedHeaders list are kept; only the signature is secret.
func RedactAuthorization(v string) string {
	if v == "" {
		return ""
	}

	algorithm := ""
	params := ""
	for _, alg := range []string{AuthorizationAWS4HMACSHA256, AuthorizationAWS4ECDSAP256SHA256} {
		if strings.HasPrefix(v, alg) {
			algorithm = alg
			params = strings.TrimLeft(v[len(alg):], " ")
			break
		}
	}
	if algorithm == "" {
		var ok bool
		algorithm, params, ok = strings.Cut(v, " ")
		if !ok {
			return v
		}
	}
	if params == "" {
		return algorithm
	}

	fields := strings.Split(params, ",")
	for i, field := range fields {
		field = strings.TrimSpace(field)
		if key, _, ok := strings.Cut(field, "="); ok && strings.EqualFold(key, "Signature") {
			field = "Signature=" + redacted
		}
		fields[i] = field
	}
	return algorithm + " " + strings.Join(fields, ", ")
}

// RedactURL returns u as a string with secret presigned-URL query parameters
// redacted for logging. It is safe to call with a nil URL.
func RedactURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	q := u.Query()
	var changed bool
	for key := range q {
		for _, secret := range secretQueryParams {
			if strings.EqualFold(key, secret) {
				q.Set(key, redacted)
				changed = true
				break
			}
		}
	}
	if !changed {
		return u.String()
	}
	c := *u
	c.RawQuery = q.Encode()
	return c.String()
}
