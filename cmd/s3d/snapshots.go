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
	"text/tabwriter"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

const (
	latestSnapshot = "latest"

	snapshotsUsage = `Usage: s3d snapshots [command]

Manage database snapshots backed up to Sia.

Commands:
	create		Back up the database and upload it to Sia
	list		List snapshots
	delete		Delete a snapshot and unpin its Sia object
	restore		Restore the database from a snapshot on Sia
`

	snapshotsCreateUsage = `Usage: s3d snapshots create

Flush any pending objects to Sia, then back up the database and upload it as a
pinned snapshot object. The flush can take a while.

Reads the admin address and password from the loaded config file or
S3D_CONFIG_FILE.`

	snapshotsListUsage = `Usage: s3d snapshots list [--remote]

List the snapshots recorded by the running instance. A snapshot is listed once
its pin is confirmed, so one just created may not appear yet.

With --remote, enumerate the snapshots stored on the Sia network instead. This
needs nothing but the app key, which makes it the way to find a snapshot when
the database is lost. It pages through every object event in the account, so it
takes longer the more you have stored, and prints nothing until it finishes.`

	snapshotsDeleteUsage = `Usage: s3d snapshots delete <sia object id>

Unpin a snapshot's Sia object from the network and remove its record, so the
objects it was withholding can be unpinned.

Snapshots are addressed by their Sia object ID, printed when a snapshot is
created and by 'snapshots list'. It is the only identifier that survives losing
the database.

Reads the admin address and password from the loaded config file or
S3D_CONFIG_FILE.`

	snapshotsRestoreUsage = `Usage: s3d snapshots restore [--out <dir>] <sia object id|latest>

Restore the database from a snapshot stored on Sia.

Given a Sia object ID, the snapshot is fetched directly in a single request.
Given "latest", the account is enumerated to find the newest snapshot, which
pages through every object event and so takes longer the more you have stored.
The object ID is printed when a snapshot is created and by 'snapshots list', so
keeping it somewhere safe makes recovery much faster.

The snapshot is then downloaded, decompressed and written to the data directory.
An existing database is moved aside with its write ahead log rather than
removed, so a restore can be undone.

The app key is read from the configured data directory, so this requires an
instance that has already run 's3d login'. With --out the restored database is
written to that directory instead of replacing the configured one. Without --out
the configured database is replaced, so the daemon must be stopped.`
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

	fmt.Printf("Created snapshot with %d objects.\n", snapshot.ObjectCount)
	fmt.Println("Sia object ID:", snapshot.SiaObjectID)
	fmt.Println("Keep this ID: it identifies the snapshot for restore and delete, and is the only")
	fmt.Println("identifier that survives losing the database.")
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
	fmt.Fprintln(w, "SIA OBJECT ID\tCREATED\tOBJECTS")
	for _, snap := range snapshots {
		fmt.Fprintf(w, "%s\t%s\t%d\n", snap.SiaObjectID, snap.CreatedAt.Format(time.RFC3339), snap.ObjectCount)
	}
	w.Flush()
}

func listRemoteSnapshots(ctx context.Context) {
	sdkClient := openSDK()

	fmt.Println("Enumerating snapshots on the Sia network. This may take a while...")
	snapshots, err := sia.ListRemoteSnapshots(ctx, sdkClient)
	checkFatalError("failed to list remote snapshots", err)

	if len(snapshots) == 0 {
		fmt.Println("No snapshots found on the network.")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "SIA OBJECT ID\tCREATED\tOBJECTS\tDB VERSION\tS3D VERSION")
	for _, snap := range snapshots {
		m := snap.Metadata
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\n", snap.ObjectID, m.CreatedAt.Format(time.RFC3339), m.ObjectCount, m.DBVersion, m.S3DVersion)
	}
	w.Flush()
}

func runSnapshotsDelete(ctx context.Context, cmd *flag.FlagSet) {
	if len(cmd.Args()) != 1 {
		cmd.Usage()
		os.Exit(1)
	}
	requireAdminConfig()

	var objectID types.Hash256
	checkFatalError("invalid Sia object id", objectID.UnmarshalText([]byte(cmd.Arg(0))))

	err := adminRequest(ctx, http.MethodDelete, cfg.AdminAddress, cfg.AdminPassword, "/snapshots/"+objectID.String(), nil)
	if errors.Is(err, errNotFound) {
		checkFatalError("failed to delete snapshot", fmt.Errorf("no snapshot with Sia object id %s", objectID))
	}
	checkFatalError("failed to delete snapshot", err)

	fmt.Println("Deleted snapshot", objectID)
}

func runSnapshotsRestore(ctx context.Context, cmd *flag.FlagSet, out string) {
	if len(cmd.Args()) != 1 {
		cmd.Usage()
		os.Exit(1)
	}
	target := cmd.Arg(0)

	destDir := cfg.Directory
	if out != "" {
		destDir = out
	}
	dbPath := filepath.Join(destDir, "s3d.db")

	sdkClient := openSDK()

	var snap sia.RemoteSnapshot
	if target == latestSnapshot {
		fmt.Println("Enumerating snapshots on the Sia network. This may take a while...")
		snapshots, err := sia.ListRemoteSnapshots(ctx, sdkClient)
		checkFatalError("failed to list remote snapshots", err)

		snap, err = selectSnapshot(snapshots, target)
		checkFatalError("failed to restore snapshot", err)
	} else {
		var objectID types.Hash256
		checkFatalError("invalid Sia object id", objectID.UnmarshalText([]byte(target)))

		fmt.Println("Fetching snapshot", objectID)
		fetched, err := sia.FetchRemoteSnapshot(ctx, sdkClient, objectID)
		checkFatalError("failed to fetch snapshot", err)

		snap, err = selectSnapshot([]sia.RemoteSnapshot{fetched}, target)
		checkFatalError("failed to restore snapshot", err)
	}

	checkFatalError("failed to create data directory", os.MkdirAll(destDir, 0700))

	// download to a temporary file so a failed restore cannot leave a
	// truncated database behind
	tmp, err := os.CreateTemp(destDir, "restore-*.tmp")
	checkFatalError("failed to create temporary file", err)
	defer os.Remove(tmp.Name())

	// remove the partial download before exiting
	checkFatalRestoreError := func(msg string, err error) {
		if err == nil {
			return
		}
		os.Remove(tmp.Name())
		checkFatalError(msg, err)
	}

	fmt.Println("Downloading snapshot", snap.ObjectID)
	if err := sia.DownloadSnapshot(sdkClient, snap, tmp); err != nil {
		tmp.Close()
		checkFatalRestoreError("failed to download snapshot", err)
	} else if err := tmp.Sync(); err != nil {
		tmp.Close()
		checkFatalRestoreError("failed to sync snapshot", err)
	} else if err := tmp.Close(); err != nil {
		checkFatalRestoreError("failed to close snapshot", err)
	}

	bakPath, err := backupExistingDatabase(dbPath)
	if bakPath != "" {
		fmt.Println("Moved the previous database to", bakPath)
	}
	checkFatalRestoreError("failed to move the existing database aside", err)
	checkFatalRestoreError("failed to write database", os.Rename(tmp.Name(), dbPath))

	fmt.Printf("Restored %d objects from the snapshot taken at %s.\n", snap.Metadata.ObjectCount, snap.Metadata.CreatedAt.Format(time.RFC3339))
	fmt.Println("Start s3d to reconcile the restored database with the network.")
}

// backupExistingDatabase moves the database at dbPath and its sidecars to a
// timestamped backup path and returns that path, which is empty when no
// database was moved.
func backupExistingDatabase(dbPath string) (string, error) {
	suffixes := []string{"", "-wal", "-shm"}

	var present bool
	for _, suffix := range suffixes {
		if _, err := os.Stat(dbPath + suffix); err == nil {
			present = true
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("failed to check for an existing database: %w", err)
		}
	}
	if !present {
		return "", nil
	}

	stamp := time.Now().UTC().Format("20060102T150405Z")
	bakPath := dbPath + ".bak-" + stamp
	// a second restore within the same second must not land on the previous
	// backup and mix the two sets of files
	for i := 1; ; i++ {
		if _, err := os.Stat(bakPath); errors.Is(err, os.ErrNotExist) {
			break
		} else if err != nil {
			return "", fmt.Errorf("failed to check for an existing backup: %w", err)
		}
		bakPath = fmt.Sprintf("%s.bak-%s.%d", dbPath, stamp, i)
	}

	var moved string
	for _, suffix := range suffixes {
		if err := os.Rename(dbPath+suffix, bakPath+suffix); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return moved, fmt.Errorf("failed to move %s aside: %w", dbPath+suffix, err)
		}
		if suffix == "" {
			moved = bakPath
		}
	}
	return moved, nil
}

// selectSnapshot picks the snapshot matching target, which is either
// latestSnapshot or a Sia object ID.
func selectSnapshot(snapshots []sia.RemoteSnapshot, target string) (sia.RemoteSnapshot, error) {
	if len(snapshots) == 0 {
		return sia.RemoteSnapshot{}, errors.New("no snapshots found on the network")
	}

	snap := snapshots[0]
	if target != latestSnapshot {
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

// openSDK builds an SDK client from the app key stored in the local database.
func openSDK() *sia.IndexdSDK {
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
	checkFatalError("failed to close database", store.Close())
	return sia.NewSDK(sdkClient)
}
