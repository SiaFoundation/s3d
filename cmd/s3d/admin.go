package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// errNotFound is returned by adminRequest when the admin API reports that the
// requested resource does not exist.
var errNotFound = errors.New("not found")

// requireAdminConfig exits if the admin API address or password are unset.
func requireAdminConfig() {
	if cfg.AdminAddress == "" {
		checkFatalError("missing admin configuration", errors.New("adminAddress is not set in the config file"))
	} else if cfg.AdminPassword == "" {
		checkFatalError("missing admin configuration", errors.New("adminPassword is not set in the config file"))
	}
}

// adminRequest sends a request to the admin API route and decodes the JSON
// response into out when it is non-nil. Cancellation is driven by ctx alone,
// with no client timeout, since some operations (e.g. flushing objects) can
// block for a long time.
func adminRequest(ctx context.Context, method, addr, password, route string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, "http://"+addr+route, nil)
	if err != nil {
		return fmt.Errorf("failed to build request: %w", err)
	}
	req.SetBasicAuth("", password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if err := adminResponseError(resp); err != nil {
		return err
	} else if out == nil {
		return nil
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}
	return nil
}

// adminResponseError returns an error describing a non-200 admin API response,
// including the response body when the server provided one. A missing resource
// is reported as errNotFound so callers can recognize it.
func adminResponseError(resp *http.Response) error {
	if resp.StatusCode == http.StatusOK {
		return nil
	} else if resp.StatusCode == http.StatusNotFound {
		return errNotFound
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10)) // 8 KiB
	if len(body) > 0 {
		return fmt.Errorf("unexpected status %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return fmt.Errorf("unexpected status %s", resp.Status)
}
