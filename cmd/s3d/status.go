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
