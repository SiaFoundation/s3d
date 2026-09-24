package main

import (
	"context"
	"encoding/json/v2"
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

	requireAdminConfig()

	stats, err := fetchUploadStats(ctx, cfg.AdminAddress, cfg.AdminPassword)
	checkFatalError("failed to fetch status", err)

	fmt.Println("Upload Pipeline")
	fmt.Printf("  Pending Objects:   %d\n", stats.PendingObjects)
	fmt.Printf("  Pending Size:      %s\n", humanBytes(stats.PendingSize))
	fmt.Printf("  Uploaded Objects:  %d\n", stats.UploadedObjects)
	fmt.Printf("  Uploaded Size:     %s\n", humanBytes(stats.UploadedSize))
	fmt.Printf("  Unpinned Objects:  %d\n", stats.UnpinnedObjects)
	fmt.Printf("  Failed Uploads:    %d\n", stats.FailedUploads)
	fmt.Printf("  Orphaned Objects:  %d\n", stats.OrphanedObjects)
	fmt.Printf("  Multipart Uploads: %d\n", stats.MultipartUploads)

	t := stats.Transfer
	fmt.Println()
	fmt.Println("Transfer")
	fmt.Printf("  S3 Ingress:        %d active, %s/s\n", t.IngressActive, humanBytes(t.IngressRate))
	fmt.Printf("  Sia Upload:        %d active, %s/s\n", t.UploadActive, humanBytes(t.UploadRate))
	if cfg.Sia.DataShards > 0 {
		shards := int64(cfg.Sia.DataShards) + int64(cfg.Sia.ParityShards)
		fmt.Printf("  Sia Encoded:      ~%s/s\n", humanBytes(t.UploadRate*shards/int64(cfg.Sia.DataShards)))
	}
	fmt.Printf("  Received:          %s\n", humanBytes(t.IngressBytes))
	fmt.Printf("  Sent to Sia:       %s\n", humanBytes(t.UploadBytes))

	fmt.Println()
	fmt.Println("Local Buffer")
	if t.BufferLimit > 0 {
		pct := 100 * float64(t.BufferUsed) / float64(t.BufferLimit)
		fmt.Printf("  Used:              %s / %s, %.1f%%\n", humanBytes(t.BufferUsed), humanBytes(t.BufferLimit), pct)
		fmt.Printf("  Headroom:          %s\n", humanBytes(max(t.BufferLimit-t.BufferUsed, 0)))
	} else {
		fmt.Printf("  Used:              %s, no limit\n", humanBytes(t.BufferUsed))
	}

	if len(t.ActiveUploads) == 0 {
		return
	}
	fmt.Println()
	fmt.Println("Active Sia Uploads")
	for _, upload := range t.ActiveUploads {
		state := "uploading"
		if upload.Finalizing {
			state = "finalizing"
		}
		var pct float64
		if upload.Size > 0 {
			pct = 100 * float64(upload.Sent) / float64(upload.Size)
		}
		fmt.Printf("  %-10s %5.1f%%  %s / %s  %s\n",
			state, pct, humanBytes(upload.Sent), humanBytes(upload.Size), upload.Label)
	}
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
	if err := json.UnmarshalRead(resp.Body, &stats); err != nil {
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
