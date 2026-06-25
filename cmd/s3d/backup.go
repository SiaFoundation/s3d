package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3"
)

const (
	backupUsage = `Usage: s3d backup <command>

Manage backups of the SQLite metadata database.

Commands:
  create    Create a new backup
  list      List backups in the backups directory
  delete    Delete a backup`

	backupCreateUsage = `Usage: s3d backup create [path]

Create a backup of the SQLite metadata database. With no path, the backup is
written to a timestamped file in the backups directory.

Reads the admin address and password from the loaded config file or
S3D_CONFIG_FILE.`

	backupListUsage = `Usage: s3d backup list

List the backups in the backups directory.`

	backupDeleteUsage = `Usage: s3d backup delete <path>

Delete a backup from the backups directory.`

	// backupExt is the filename extension for managed backups.
	backupExt = ".sqlite3"
)

func backupsDir() string {
	if cfg.Backups.Directory != "" {
		return cfg.Backups.Directory
	}
	return filepath.Join(cfg.Directory, "backups")
}

func runBackupCreate(ctx context.Context, cmd *flag.FlagSet) {
	args := cmd.Args()
	if len(args) > 1 {
		cmd.Usage()
		os.Exit(1)
	}
	requireAdminConfig()

	var path string
	if len(args) == 1 {
		path = args[0]
		if !filepath.IsAbs(path) {
			if path == "." || path == ".." || filepath.Base(path) != path {
				checkFatalError("failed to create backup", fmt.Errorf("relative backup path must be a filename without path separators: %q", path))
			} else if filepath.Ext(path) != backupExt {
				checkFatalError("failed to create backup", fmt.Errorf("relative backup path must end in %q: %q", backupExt, path))
			}
			path = filepath.Join(backupsDir(), path)
		}
	} else {
		// ':' is invalid in filenames on some platforms
		name := "s3d-" + time.Now().UTC().Format("2006-01-02T15-04-05Z") + backupExt
		path = filepath.Join(backupsDir(), name)
	}

	abs, err := filepath.Abs(path)
	checkFatalError("failed to resolve backup path", err)

	checkFatalError("failed to create backup", postAdmin(ctx, cfg.AdminAddress, cfg.AdminPassword, "/system/sqlite3/backup", s3.BackupSQLite3Request{Path: abs}))
	fmt.Printf("Created backup %s\n", abs)
}

func runBackupList(cmd *flag.FlagSet) {
	if len(cmd.Args()) != 0 {
		cmd.Usage()
		os.Exit(1)
	}

	dir := backupsDir()
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		fmt.Printf("No backups found in %s\n", dir)
		return
	}
	checkFatalError("failed to read backups directory", err)

	var found bool
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != backupExt {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if !found {
			fmt.Printf("Backups in %s\n\n", dir)
			found = true
		}
		fmt.Printf("  %s\t%s\t%s\n", e.Name(), humanBytes(info.Size()), info.ModTime().Format(time.RFC3339))
	}
	if !found {
		fmt.Printf("No backups found in %s\n", dir)
	}
}

func runBackupDelete(cmd *flag.FlagSet) {
	args := cmd.Args()
	if len(args) != 1 {
		cmd.Usage()
		os.Exit(1)
	}

	dir, err := filepath.Abs(backupsDir())
	checkFatalError("failed to resolve backups directory", err)

	target := args[0]
	if !filepath.IsAbs(target) {
		target = filepath.Join(dir, target)
	}
	path, err := filepath.Abs(target)
	checkFatalError("failed to resolve backup path", err)

	if rel, err := filepath.Rel(dir, path); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		checkFatalError("failed to delete backup", fmt.Errorf("path is outside the backups directory: %q", args[0]))
	}

	if filepath.Ext(path) != backupExt {
		checkFatalError("failed to delete backup", fmt.Errorf("not a backup file: %q", path))
	}

	// never delete the live database
	if db, err := filepath.Abs(filepath.Join(cfg.Directory, "s3d.db")); err == nil && path == db {
		checkFatalError("failed to delete backup", fmt.Errorf("refusing to delete the live database: %q", path))
	}

	if info, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		checkFatalError("failed to delete backup", fmt.Errorf("backup does not exist: %q", path))
	} else if err != nil {
		checkFatalError("failed to stat backup", err)
	} else if info.IsDir() {
		checkFatalError("failed to delete backup", fmt.Errorf("not a backup file: %q", path))
	}

	checkFatalError("failed to delete backup", os.Remove(path))
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")
	_ = os.Remove(path + "-journal")
	fmt.Printf("Deleted backup %s\n", path)
}
