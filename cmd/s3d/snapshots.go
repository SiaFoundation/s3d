package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

const (
	snapshotsUsage = `Usage: s3d snapshots [command]

Manage database snapshots backed up to Sia.

Commands:
	create		Back up the database and upload it to Sia
	list		List snapshots
	delete		Delete a snapshot and unpin its backup
	restore		Restore the database from a snapshot on Sia
`

	snapshotsCreateUsage = `Usage: s3d snapshots create

Back up the database and upload it to Sia as a pinned snapshot object. Database
writes are blocked for the duration of the backup.

Reads the admin address and password from the loaded config file or
S3D_CONFIG_FILE.`

	snapshotsListUsage = `Usage: s3d snapshots list [--remote]

List the snapshots recorded by the running instance.

With --remote, enumerate the snapshots stored on the Sia network instead. This
reads the app key from the local database rather than the admin API, so it works
against a stopped daemon.`

	snapshotsDeleteUsage = `Usage: s3d snapshots delete <id>

Unpin a snapshot's backup object from Sia and remove its record, releasing the
orphaned objects it was withholding.

Reads the admin address and password from the loaded config file or
S3D_CONFIG_FILE.`

	snapshotsRestoreUsage = `Usage: s3d snapshots restore [--out <dir>] [--force] <sia object id|latest>

Restore the database from a snapshot stored on Sia.

Locates the snapshot by enumerating the account, downloads and decompresses its
backup, and writes it to the data directory. Refuses to overwrite an existing
database unless --force is set.

The app key is always read from the configured data directory, so this requires
an instance that has already run 's3d login'. With --out the restored database is
written to that directory instead, leaving the configured one untouched. Without
--out the configured data directory is overwritten, so the daemon must be
stopped.`
)

func runSnapshotsCreate(ctx context.Context, cmd *flag.FlagSet) {
	if len(cmd.Args()) != 0 {
		cmd.Usage()
		os.Exit(1)
	}
	requireAdminConfig()

	fmt.Println("Backing up the database and uploading it to Sia. This may take a while...")
	var snapshot s3.Snapshot
	err := adminRequest(ctx, http.MethodPost, cfg.AdminAddress, cfg.AdminPassword, "/snapshots", &snapshot)
	checkFatalError("failed to create snapshot", err)

	fmt.Printf("Created snapshot %d with %d objects.\n", snapshot.ID, snapshot.ObjectCount)
	fmt.Println("Sia object ID:", snapshot.SiaObjectID)
}

func runSnapshotsList(ctx context.Context, cmd *flag.FlagSet, remote bool) {
	if len(cmd.Args()) != 0 {
		cmd.Usage()
		os.Exit(1)
	}

	if remote {
		listRemoteSnapshots(ctx)
		return
	}

	requireAdminConfig()
	var snapshots []s3.Snapshot
	err := adminRequest(ctx, http.MethodGet, cfg.AdminAddress, cfg.AdminPassword, "/snapshots", &snapshots)
	checkFatalError("failed to list snapshots", err)

	if len(snapshots) == 0 {
		fmt.Println("No snapshots.")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tCREATED\tOBJECTS\tSIA OBJECT ID")
	for _, snap := range snapshots {
		fmt.Fprintf(w, "%d\t%s\t%d\t%s\n", snap.ID, snap.CreatedAt.Format(time.RFC3339), snap.ObjectCount, snap.SiaObjectID)
	}
	w.Flush()
}

func listRemoteSnapshots(ctx context.Context) {
	sdkClient, closeStore := openSDK()
	defer closeStore()

	fmt.Println("Enumerating snapshots on the Sia network. This may take a while...")
	snapshots, err := sia.ListRemoteSnapshots(ctx, sdkClient)
	checkFatalError("failed to list remote snapshots", err)

	if len(snapshots) == 0 {
		fmt.Println("No snapshots found on the network.")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "CREATED\tOBJECTS\tDB VERSION\tS3D VERSION\tSIA OBJECT ID")
	for _, snap := range snapshots {
		m := snap.Metadata
		fmt.Fprintf(w, "%s\t%d\t%d\t%s\t%s\n", m.CreatedAt.Format(time.RFC3339), m.ObjectCount, m.DBVersion, m.S3DVersion, snap.ObjectID)
	}
	w.Flush()
}

func runSnapshotsDelete(ctx context.Context, cmd *flag.FlagSet) {
	if len(cmd.Args()) != 1 {
		cmd.Usage()
		os.Exit(1)
	}
	requireAdminConfig()

	id, err := strconv.ParseInt(cmd.Arg(0), 10, 64)
	checkFatalError("invalid snapshot id", err)

	err = adminRequest(ctx, http.MethodDelete, cfg.AdminAddress, cfg.AdminPassword, "/snapshots/"+strconv.FormatInt(id, 10), nil)
	if errors.Is(err, errNotFound) {
		checkFatalError("failed to delete snapshot", fmt.Errorf("no snapshot with id %d", id))
	}
	checkFatalError("failed to delete snapshot", err)

	fmt.Printf("Deleted snapshot %d.\n", id)
}

func runSnapshotsRestore(ctx context.Context, cmd *flag.FlagSet, force bool, out string) {
	if len(cmd.Args()) != 1 {
		cmd.Usage()
		os.Exit(1)
	}
	target := cmd.Arg(0)

	destDir := restoreDir(cfg.Directory, out)
	dbPath := filepath.Join(destDir, "s3d.db")
	if _, err := os.Stat(dbPath); err == nil && !force {
		checkFatalError("failed to restore snapshot", fmt.Errorf("%s already exists, pass --force to overwrite it", dbPath))
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		checkFatalError("failed to check for an existing database", err)
	}

	sdkClient, closeStore := openSDK()
	defer closeStore()

	fmt.Println("Enumerating snapshots on the Sia network. This may take a while...")
	snapshots, err := sia.ListRemoteSnapshots(ctx, sdkClient)
	checkFatalError("failed to list remote snapshots", err)

	snap, err := selectSnapshot(snapshots, target)
	checkFatalError("failed to restore snapshot", err)

	checkFatalError("failed to create data directory", os.MkdirAll(destDir, 0700))

	// download to a temporary file so a failed restore cannot leave a
	// truncated database behind
	tmp, err := os.CreateTemp(destDir, "restore-*.tmp")
	checkFatalError("failed to create temporary file", err)
	defer os.Remove(tmp.Name())

	fmt.Println("Downloading snapshot", snap.ObjectID)
	if err := sia.DownloadSnapshot(sdkClient, snap, tmp); err != nil {
		tmp.Close()
		checkFatalError("failed to download snapshot", err)
	} else if err := tmp.Sync(); err != nil {
		tmp.Close()
		checkFatalError("failed to sync snapshot", err)
	} else if err := tmp.Close(); err != nil {
		checkFatalError("failed to close snapshot", err)
	}

	// the write ahead log and shared memory belong to the replaced database
	for _, sidecar := range []string{dbPath + "-wal", dbPath + "-shm"} {
		if err := os.Remove(sidecar); err != nil && !errors.Is(err, os.ErrNotExist) {
			checkFatalError("failed to remove stale sidecar", err)
		}
	}
	checkFatalError("failed to write database", os.Rename(tmp.Name(), dbPath))

	fmt.Printf("Restored %d objects from the snapshot taken at %s.\n", snap.Metadata.ObjectCount, snap.Metadata.CreatedAt.Format(time.RFC3339))
	fmt.Println("Start s3d to reconcile the restored database with the network.")
}

// restoreDir returns the directory a restore writes its database to. An empty
// out keeps the configured data directory, which the app key is always read
// from.
func restoreDir(dataDir, out string) string {
	if out == "" {
		return dataDir
	}
	return out
}

// selectSnapshot picks the snapshot matching target, which is either "latest"
// or a Sia object ID, and reports whether this build can read its database.
func selectSnapshot(snapshots []sia.RemoteSnapshot, target string) (sia.RemoteSnapshot, error) {
	if len(snapshots) == 0 {
		return sia.RemoteSnapshot{}, errors.New("no snapshots found on the network")
	}

	snap := snapshots[len(snapshots)-1]
	if target != "latest" {
		var objectID types.Hash256
		if err := objectID.UnmarshalText([]byte(target)); err != nil {
			return sia.RemoteSnapshot{}, fmt.Errorf("invalid Sia object id: %w", err)
		}

		idx := slices.IndexFunc(snapshots, func(s sia.RemoteSnapshot) bool {
			return s.ObjectID == objectID
		})
		if idx == -1 {
			return sia.RemoteSnapshot{}, fmt.Errorf("no snapshot with Sia object id %s", objectID)
		}
		snap = snapshots[idx]
	}

	if v := sqlite.SchemaVersion(); snap.Metadata.DBVersion > v {
		return sia.RemoteSnapshot{}, fmt.Errorf("snapshot database version %d is newer than this build supports (%d)", snap.Metadata.DBVersion, v)
	}
	return snap, nil
}

// openSDK opens the local database, builds an SDK client from the stored app
// key, and returns it with a function that closes the database.
func openSDK() (*sia.IndexdSDK, func()) {
	store, err := openStore(zap.NewNop())
	checkFatalError("failed to open database", err)

	appKey, indexerURL, err := store.AppKey()
	if errors.Is(err, sqlite.ErrNoAppKey) {
		store.Close()
		os.Stderr.WriteString("No app key found. Please run 's3d login' to register the app.\n")
		os.Exit(1)
	} else if err != nil {
		store.Close()
		checkFatalError("failed to get app key from database", err)
	}

	sdkClient, err := newSDKBuilder(indexerURL).SDK(appKey)
	if err != nil {
		store.Close()
		checkFatalError("failed to create SDK client", err)
	}
	return sia.NewSDK(sdkClient), func() { store.Close() }
}
