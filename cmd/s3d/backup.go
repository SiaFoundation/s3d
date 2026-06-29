package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

Manage SQLite database backups via the admin API.

Commands:
  create    Create a backup
  list      List backups
  delete    Delete a backup`

	backupCreateUsage = `Usage: s3d backup create <path>

Create a database backup at the given absolute path and record it as a snapshot,
preventing its objects from being unpinned while the backup exists.`

	backupListUsage = `Usage: s3d backup list

List the recorded database backups.`

	backupDeleteUsage = `Usage: s3d backup delete <id>

Delete the backup snapshot with the given id, releasing the objects it pinned.
The backup file on disk is left in place.`
)

// requireAdminConfig exits with an error if the admin API is not configured.
func requireAdminConfig() {
	if cfg.AdminAddress == "" {
		checkFatalError("missing admin configuration", errors.New("adminAddress is not set in the config file"))
	} else if cfg.AdminPassword == "" {
		checkFatalError("missing admin configuration", errors.New("adminPassword is not set in the config file"))
	}
}

func runBackupCreate(ctx context.Context, cmd *flag.FlagSet) {
	if len(cmd.Args()) != 1 {
		cmd.Usage()
		os.Exit(1)
	}
	requireAdminConfig()

	body, err := json.Marshal(s3.BackupSQLite3Request{Path: cmd.Arg(0)})
	checkFatalError("failed to encode request", err)

	// a backup blocks until complete and can take a while, so no timeout is
	// imposed beyond the parent context
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+cfg.AdminAddress+"/system/sqlite3/backup", bytes.NewReader(body))
	checkFatalError("failed to build request", err)
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("", cfg.AdminPassword)

	resp, err := http.DefaultClient.Do(req)
	checkFatalError("failed to send request", err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		checkFatalError("failed to create backup", fmt.Errorf("unexpected status %s", resp.Status))
	}
	fmt.Printf("Created backup at %s\n", cmd.Arg(0))
}

func runBackupList(ctx context.Context, cmd *flag.FlagSet) {
	if len(cmd.Args()) != 0 {
		cmd.Usage()
		os.Exit(1)
	}
	requireAdminConfig()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+cfg.AdminAddress+"/system/sqlite3/backups", nil)
	checkFatalError("failed to build request", err)
	req.SetBasicAuth("", cfg.AdminPassword)

	resp, err := http.DefaultClient.Do(req)
	checkFatalError("failed to send request", err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		checkFatalError("failed to list backups", fmt.Errorf("unexpected status %s", resp.Status))
	}

	var snapshots []s3.Snapshot
	checkFatalError("failed to decode response", json.NewDecoder(resp.Body).Decode(&snapshots))

	if len(snapshots) == 0 {
		fmt.Println("No backups found.")
		return
	}
	for _, snap := range snapshots {
		fmt.Printf("%d\t%s\t%d objects\t%s\n", snap.ID, snap.CreatedAt.Format(time.RFC3339), snap.ObjectCount, snap.Path)
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

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, "http://"+cfg.AdminAddress+"/system/sqlite3/backups/"+strconv.FormatInt(id, 10), nil)
	checkFatalError("failed to build request", err)
	req.SetBasicAuth("", cfg.AdminPassword)

	resp, err := http.DefaultClient.Do(req)
	checkFatalError("failed to send request", err)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		checkFatalError("failed to delete backup", fmt.Errorf("unexpected status %s", resp.Status))
	}
	fmt.Printf("Deleted backup %d\n", id)
}
