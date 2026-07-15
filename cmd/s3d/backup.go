package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/SiaFoundation/s3d/s3"
)

const (
	backupUsage = `Usage: s3d backup <command>

Manage database backups uploaded to the Sia network via the admin API.

Commands:
  create    Create a backup
  list      List backups
  delete    Delete a backup`

	backupCreateUsage = `Usage: s3d backup create

Back up the database and upload it to the Sia network as a snapshot,
preventing the objects it references from being unpinned while the snapshot
exists.`

	backupListUsage = `Usage: s3d backup list

List the recorded database backups.`

	backupDeleteUsage = `Usage: s3d backup delete <id>

Delete the backup snapshot with the given id, unpinning its backup object and
releasing the objects it pinned.`
)

func runBackupCreate(ctx context.Context, cmd *flag.FlagSet) {
	if len(cmd.Args()) != 0 {
		cmd.Usage()
		os.Exit(1)
	}
	requireAdminConfig()

	// a backup blocks until complete and can take a while, so no timeout is
	// imposed beyond the parent context
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+cfg.AdminAddress+"/snapshots", nil)
	checkFatalError("failed to build request", err)
	req.SetBasicAuth("", cfg.AdminPassword)

	resp, err := http.DefaultClient.Do(req)
	checkFatalError("failed to send request", err)
	defer resp.Body.Close()
	checkFatalError("failed to create backup", adminResponseError(resp))

	var snap s3.Snapshot
	checkFatalError("failed to decode response", json.NewDecoder(resp.Body).Decode(&snap))
	fmt.Printf("Created backup %d with Sia object ID %s\n", snap.ID, snap.SiaObjectID)
}

func runBackupList(ctx context.Context, cmd *flag.FlagSet) {
	if len(cmd.Args()) != 0 {
		cmd.Usage()
		os.Exit(1)
	}
	requireAdminConfig()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+cfg.AdminAddress+"/snapshots", nil)
	checkFatalError("failed to build request", err)
	req.SetBasicAuth("", cfg.AdminPassword)

	resp, err := http.DefaultClient.Do(req)
	checkFatalError("failed to send request", err)
	defer resp.Body.Close()
	checkFatalError("failed to list backups", adminResponseError(resp))

	var snapshots []s3.Snapshot
	checkFatalError("failed to decode response", json.NewDecoder(resp.Body).Decode(&snapshots))

	if len(snapshots) == 0 {
		fmt.Println("No backups found.")
		return
	}
	for _, snap := range snapshots {
		fmt.Printf("%d\t%s\t%d objects\t%s\n", snap.ID, snap.CreatedAt.Format(time.RFC3339), snap.ObjectCount, snap.SiaObjectID)
	}
}

func runBackupDelete(ctx context.Context, cmd *flag.FlagSet) {
	if len(cmd.Args()) != 1 {
		cmd.Usage()
		os.Exit(1)
	}
	requireAdminConfig()

	id, err := strconv.ParseInt(cmd.Arg(0), 10, 64)
	checkFatalError("invalid snapshot id", err)
	if id <= 0 {
		checkFatalError("invalid snapshot id", fmt.Errorf("id must be positive: %d", id))
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, "http://"+cfg.AdminAddress+"/snapshots/"+strconv.FormatInt(id, 10), nil)
	checkFatalError("failed to build request", err)
	req.SetBasicAuth("", cfg.AdminPassword)

	resp, err := http.DefaultClient.Do(req)
	checkFatalError("failed to send request", err)
	defer resp.Body.Close()
	checkFatalError("failed to delete backup", adminResponseError(resp))
	fmt.Printf("Deleted backup %d\n", id)
}
