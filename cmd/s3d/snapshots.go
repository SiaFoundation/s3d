package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.sia.tech/core/types"
	sdk "go.sia.tech/siastorage"
)

const (
	snapshotsUsage = `Usage: s3d snapshots [command]

Manage database snapshots backed up to Sia.

Commands:
	create		Back up the database and upload it to Sia
	list		List snapshots
	delete		Delete a snapshot and unpin its Sia object
	restore		Restore a database from a snapshot on Sia
`

	snapshotsCreateUsage = `Usage: s3d snapshots create

Flush any pending objects to Sia, then back up the database and upload it as a
pinned snapshot object. The flush can take a while.`

	snapshotsListUsage = `Usage: s3d snapshots list

List the snapshots recorded by the running instance, newest first. The first
line reports whether the instance has finished syncing with the Sia network.`

	snapshotsDeleteUsage = `Usage: s3d snapshots delete <sia object id>

Unpin a snapshot's Sia object from the network and remove its record, so the
objects it was withholding can be unpinned.

Snapshots are addressed by their Sia object ID, printed when a snapshot is
created and by 'snapshots list'.`

	snapshotsRestoreUsage = `Usage: s3d snapshots restore [--out <dir>] [<sia object id>]

Restore a database from a snapshot stored on Sia into a new data directory.

Run it with no arguments for an interactive restore. It prompts for a recovery
phrase when this machine has no app key, lists the snapshots stored on the
network and asks which one to restore and where to put it. Listing enumerates
every object in the account, so it takes longer the more you have stored.

Given a Sia object ID the snapshot is fetched in a single request and written
to the --out directory, without enumerating and, on a machine that already
holds an authorized app key, without prompting. The object ID is printed when a
snapshot is created and by 'snapshots list'.

The destination must be empty or missing, so a restore can never overwrite an
existing database. Run 's3d config' for the restored directory before starting
s3d.`
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

	fmt.Printf("Created snapshot %s with %d objects.\n", snapshot.SiaObjectID, snapshot.ObjectCount)
	fmt.Println("Keep this ID, it is the only one that survives losing the database.")
}

func runSnapshotsList(ctx context.Context, cmd *flag.FlagSet) {
	if len(cmd.Args()) != 0 {
		cmd.Usage()
		os.Exit(1)
	}
	requireAdminConfig()

	var list s3.SnapshotList
	err := adminRequest(ctx, http.MethodGet, cfg.AdminAddress, cfg.AdminPassword, "/snapshots", &list)
	checkFatalError("failed to list snapshots", err)

	if list.Synced {
		fmt.Println("The listing is in sync with the Sia network.")
	} else {
		fmt.Println(ansiStyle("33", "Not yet in sync with the Sia network, the listing may be incomplete."))
	}

	if len(list.Snapshots) == 0 {
		fmt.Println("No snapshots.")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "SIA OBJECT ID\tCREATED\tOBJECTS")
	for _, snap := range list.Snapshots {
		fmt.Fprintf(w, "%s\t%s\t%d\n", snap.SiaObjectID, snap.CreatedAt.Format(time.RFC3339), snap.ObjectCount)
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
	checkFatalError("failed to delete snapshot", err)

	fmt.Println("Deleted snapshot", objectID)
}

func runSnapshotsRestore(ctx context.Context, cmd *flag.FlagSet, out string) {
	if len(cmd.Args()) > 1 {
		cmd.Usage()
		os.Exit(1)
	}
	interactive := len(cmd.Args()) == 0

	var objectID types.Hash256
	if !interactive {
		checkFatalError("invalid Sia object id", objectID.UnmarshalText([]byte(cmd.Arg(0))))
		if out == "" {
			checkFatalError("failed to restore snapshot", errors.New("--out is required when restoring by Sia object id"))
		}
	}

	destDir := out
	if destDir != "" {
		checkFatalError("invalid destination", prepareRestoreDir(destDir))
	}

	sdkClient := openRestoreSDK(ctx)

	var snap sia.RemoteSnapshot
	if interactive {
		fmt.Println("Enumerating snapshots on the Sia network. This may take a while...")
		snapshots, err := sia.ListRemoteSnapshots(ctx, sdkClient)
		checkFatalError("failed to list remote snapshots", err)
		snap = promptSnapshot(snapshots)
	} else {
		fmt.Println("Fetching snapshot", objectID)
		fetched, err := sia.FetchRemoteSnapshot(ctx, sdkClient, objectID)
		checkFatalError("failed to fetch snapshot", err)
		checkFatalError("failed to restore snapshot", checkSnapshotVersion(fetched))
		snap = fetched
	}

	if destDir == "" {
		destDir = promptRestoreDir()
	}
	dbPath := filepath.Join(destDir, "s3d.db")

	// download to a temporary file so a failed restore cannot leave a
	// truncated database behind
	tmp, err := os.CreateTemp(destDir, "restore-*.tmp")
	checkFatalError("failed to create temporary file", err)

	// remove the partial download before exiting
	checkFatalRestoreError := func(msg string, err error) {
		if err == nil {
			return
		}
		os.Remove(tmp.Name())
		checkFatalError(msg, err)
	}

	fmt.Println("Downloading snapshot", snap.ObjectID)
	if err := sia.DownloadSnapshot(ctx, sdkClient, snap, tmp); err != nil {
		tmp.Close()
		checkFatalRestoreError("failed to download snapshot", err)
	} else if err := tmp.Sync(); err != nil {
		tmp.Close()
		checkFatalRestoreError("failed to sync snapshot", err)
	} else if err := tmp.Close(); err != nil {
		checkFatalRestoreError("failed to close snapshot", err)
	}
	checkFatalRestoreError("failed to write database", os.Rename(tmp.Name(), dbPath))
	checkFatalError("failed to sync the destination", syncDir(destDir))

	fmt.Printf("Restored %d objects from the snapshot taken at %s.\n", snap.Metadata.ObjectCount, snap.Metadata.CreatedAt.Format(time.RFC3339))
	fmt.Println("Database written to", dbPath)
	fmt.Println("Run 's3d config' for this directory, then start s3d to reconcile it with the network.")
}

// openRestoreSDK builds an SDK client from the app key recorded in the
// configured data directory and prompts the user to log in when that key is
// missing or no longer authorized. No database is created either way.
func openRestoreSDK(ctx context.Context) *sia.IndexdSDK {
	appKey, indexerURL, found, err := existingAppKey()
	checkFatalError("failed to read the app key", err)

	if found {
		sdkClient, err := newSDKBuilder(indexerURL).SDK(appKey)
		if err == nil {
			return sia.NewSDK(sdkClient)
		} else if !errors.Is(err, sdk.ErrUnauthorized) {
			checkFatalError("failed to create SDK client", err)
		}
		fmt.Println(ansiStyle("33", "The app key in "+cfg.Directory+" is no longer authorized."))
	} else {
		fmt.Println("No app key found in", cfg.Directory)
	}
	fmt.Println("Log in to reach the snapshots stored on the Sia network.")
	fmt.Println("")

	indexerURL = promptIndexerURL()
	phrase := promptExistingRecoveryPhrase()

	sdkClient, err := registerSDK(ctx, indexerURL, phrase)
	checkFatalError("failed to log in", err)
	return sia.NewSDK(sdkClient)
}

// promptSnapshot prints the snapshots stored on the network, marking those
// this build cannot read, and reads the one to restore.
func promptSnapshot(snapshots []sia.RemoteSnapshot) sia.RemoteSnapshot {
	if len(snapshots) == 0 {
		checkFatalError("failed to restore snapshot", errors.New("no snapshots found on the network"))
	}

	var restorable int
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "#\tSIA OBJECT ID\tCREATED\tOBJECTS\tDB VERSION\tS3D VERSION")
	for i, snap := range snapshots {
		m, note := snap.Metadata, ""
		if checkSnapshotVersion(snap) != nil {
			note = "\tneeds a newer s3d"
		} else {
			restorable++
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%d\t%d\t%s%s\n", i+1, snap.ObjectID, m.CreatedAt.Format(time.RFC3339), m.ObjectCount, m.DBVersion, m.S3DVersion, note)
	}
	w.Flush()
	fmt.Println("")
	if restorable == 0 {
		checkFatalError("failed to restore snapshot", errors.New("every snapshot on the network needs a newer s3d"))
	}

	for {
		input := readInput(fmt.Sprintf("Snapshot to restore (1-%d)", len(snapshots)))
		n, err := strconv.Atoi(input)
		if err != nil || n < 1 || n > len(snapshots) {
			fmt.Println(ansiStyle("31", fmt.Sprintf("Enter a number between 1 and %d.", len(snapshots))))
			continue
		}
		if err := checkSnapshotVersion(snapshots[n-1]); err != nil {
			fmt.Println(ansiStyle("31", err.Error()))
			continue
		}
		return snapshots[n-1]
	}
}

// promptRestoreDir reads the directory to restore into, repeating until one is
// given that a restore cannot overwrite anything in.
func promptRestoreDir() string {
	for {
		dir := readInput("Directory to restore into")
		if dir == "" {
			continue
		} else if err := prepareRestoreDir(dir); err != nil {
			fmt.Println(ansiStyle("31", err.Error()))
			continue
		}
		return dir
	}
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	// windows does not support fsync on directories
	if runtime.GOOS == "windows" {
		return dir.Close()
	}
	return errors.Join(dir.Sync(), dir.Close())
}

// prepareRestoreDir creates dir when it is missing and reports an error when it
// already holds anything.
func prepareRestoreDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return os.MkdirAll(dir, 0700)
	} else if err != nil {
		return err
	} else if len(entries) != 0 {
		return fmt.Errorf("%s is not empty, restore into a new directory", dir)
	}
	return nil
}

// checkSnapshotVersion reports whether this build can read the snapshot's
// database image.
func checkSnapshotVersion(snap sia.RemoteSnapshot) error {
	if v := sqlite.SchemaVersion(); snap.Metadata.DBVersion > v {
		return fmt.Errorf("snapshot database version %d is newer than the %d this build supports", snap.Metadata.DBVersion, v)
	}
	return nil
}
