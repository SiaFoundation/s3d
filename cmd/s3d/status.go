package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/SiaFoundation/s3d/s3"
)

const statusUsage = `Usage: s3d status

Print a basic overview of the running s3d instance.

Fetches the background upload pipeline stats from the admin API.`

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
	fmt.Printf("  Sia Upload:        %d active, %s/s\n", len(t.ActiveUploads), humanBytes(t.UploadRate))
	fmt.Printf("  Received:          %s\n", humanBytes(t.IngressBytes))
	fmt.Printf("  Sent to Sia:       %s\n", humanBytes(t.UploadBytes))

	fmt.Println()
	fmt.Println("Local Buffer")
	if stats.BufferLimit > 0 {
		pct := 100 * float64(stats.BufferUsed) / float64(stats.BufferLimit)
		fmt.Printf("  Used:              %s / %s, %.1f%%\n", humanBytes(stats.BufferUsed), humanBytes(stats.BufferLimit), pct)
		fmt.Printf("  Headroom:          %s\n", humanBytes(max(stats.BufferLimit-stats.BufferUsed, 0)))
	} else {
		fmt.Printf("  Used:              %s, no limit\n", humanBytes(stats.BufferUsed))
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
		fmt.Printf("  %-10s %5.1f%%  %s / %s  %q\n",
			state, pct, humanBytes(upload.Sent), humanBytes(upload.Size), upload.Label)
	}
}

func fetchUploadStats(ctx context.Context, addr, password string) (s3.UploadStats, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var stats s3.UploadStats
	err := adminRequest(ctx, http.MethodGet, addr, password, "/stats/uploads", &stats)
	return stats, err
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
