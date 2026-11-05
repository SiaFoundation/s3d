package main

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// adminClient sends requests to the admin API of the running s3d instance.
type adminClient struct {
	addr     string
	password string
}

// newAdminClient returns a client for the configured admin API, exiting if the
// admin API address or password are unset.
func newAdminClient() adminClient {
	if cfg.AdminAddress == "" {
		checkFatalError("missing admin configuration", errors.New("adminAddress is not set in the config file"))
	} else if cfg.AdminPassword == "" {
		checkFatalError("missing admin configuration", errors.New("adminPassword is not set in the config file"))
	}
	return adminClient{
		addr:     cfg.AdminAddress,
		password: cfg.AdminPassword,
	}
}

// request sends a request to the admin API route, encoding body as JSON when
// it is not nil, and decodes the JSON response into out when out is not nil.
// Cancellation is driven by ctx alone, with no client timeout, since some
// operations can block for a long time.
func (c adminClient) request(ctx context.Context, method, route string, body, out any) error {
	var reqBody io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to encode request: %w", err)
		}
		reqBody = bytes.NewReader(buf)
	}

	req, err := http.NewRequestWithContext(ctx, method, "http://"+c.addr+route, reqBody)
	if err != nil {
		return fmt.Errorf("failed to build request: %w", err)
	}
	req.SetBasicAuth("", c.password)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

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

	if err := json.UnmarshalRead(resp.Body, out); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}
	return nil
}

// adminResponseError returns an error describing an admin API response whose
// status is not 200, including the response body when the server provided one.
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
