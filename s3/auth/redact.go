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
