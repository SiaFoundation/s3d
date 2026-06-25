package main

import (
	"bytes"
	"context"
	"encoding/json"
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

// postAdmin POSTs to the admin API route, encoding body as JSON if non-nil.
// Cancellation is driven by ctx alone, with no client timeout, since some
// operations (e.g. flushing objects) can block for a long time.
func postAdmin(ctx context.Context, addr, password, route string, body any) error {
	var r io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to encode request: %w", err)
		}
		r = bytes.NewReader(buf)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+route, r)
	if err != nil {
		return fmt.Errorf("failed to build request: %w", err)
	}
	req.SetBasicAuth("", password)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if m := strings.TrimSpace(string(msg)); m != "" {
			return fmt.Errorf("unexpected status %s: %s", resp.Status, m)
		}
		return fmt.Errorf("unexpected status %s", resp.Status)
	}
	return nil
}
