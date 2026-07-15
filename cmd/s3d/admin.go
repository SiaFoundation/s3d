package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// requireAdminConfig exits if the admin API address or password are unset.
func requireAdminConfig() {
	if cfg.AdminAddress == "" {
		checkFatalError("missing admin configuration", errors.New("adminAddress is not set in the config file"))
	} else if cfg.AdminPassword == "" {
		checkFatalError("missing admin configuration", errors.New("adminPassword is not set in the config file"))
	}
}

// postAdmin POSTs to the admin API route. Cancellation is driven by ctx alone,
// with no client timeout, since some operations (e.g. flushing objects) can
// block for a long time.
func postAdmin(ctx context.Context, addr, password, route string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+route, nil)
	if err != nil {
		return fmt.Errorf("failed to build request: %w", err)
	}
	req.SetBasicAuth("", password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()
	return adminResponseError(resp)
}

// adminResponseError returns an error describing a non-200 admin API response,
// including the response body when the server provided one.
func adminResponseError(resp *http.Response) error {
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10)) // 8 KiB
	if len(body) > 0 {
		return fmt.Errorf("unexpected status %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return fmt.Errorf("unexpected status %s", resp.Status)
}
