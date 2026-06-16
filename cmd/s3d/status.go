package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/SiaFoundation/s3d/s3"
)

const statusUsage = `Usage: s3d status

Print a basic overview of the running s3d instance.

Fetches the background upload pipeline stats from the admin API. Reads the
admin address and password from the loaded config file or S3D_CONFIG_FILE.`

func runStatus(ctx context.Context, cmd *flag.FlagSet) {
	if len(cmd.Args()) != 0 {
		cmd.Usage()
		os.Exit(1)
	}

	if cfg.AdminAddress == "" {
		checkFatalError("missing admin configuration", errors.New("adminAddress is not set in the config file"))
	} else if cfg.AdminPassword == "" {
		checkFatalError("missing admin configuration", errors.New("adminPassword is not set in the config file"))
	}

	stats, err := fetchUploadStats(ctx, cfg.AdminAddress, cfg.AdminPassword)
	checkFatalError("failed to fetch status", err)

	fmt.Println("Upload Pipeline")
	fmt.Printf("  Pending Objects:   %d\n", stats.PendingObjects)
	fmt.Printf("  Pending Size:      %s\n", humanBytes(stats.PendingSize))
	fmt.Printf("  Uploaded Objects:  %d\n", stats.UploadedObjects)
	fmt.Printf("  Uploaded Size:     %s\n", humanBytes(stats.UploadedSize))
	fmt.Printf("  Failed Uploads:    %d\n", stats.FailedUploads)
	fmt.Printf("  Orphaned Objects:  %d\n", stats.OrphanedObjects)
	fmt.Printf("  Multipart Uploads: %d\n", stats.MultipartUploads)
}

func fetchUploadStats(ctx context.Context, addr, password string) (s3.UploadStats, error) {
	url := "http://" + addr + "/stats/uploads"

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return s3.UploadStats{}, fmt.Errorf("failed to build request: %w", err)
	}
	req.SetBasicAuth("", password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return s3.UploadStats{}, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return s3.UploadStats{}, fmt.Errorf("unexpected status %s", resp.Status)
	}

	var stats s3.UploadStats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return s3.UploadStats{}, fmt.Errorf("failed to decode response: %w", err)
	}
	return stats, nil
}

// humanBytes formats n as a human-readable byte count using binary units.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
