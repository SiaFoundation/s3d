package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"text/tabwriter"
	"time"

	"github.com/SiaFoundation/s3d/s3"
	"go.sia.tech/core/types"
)

const (
	snapshotsUsage = `Usage: s3d snapshots [command]

Manage database snapshots backed up to Sia.

Commands:
	create		Back up the database and upload it to Sia
	list		List snapshots
	delete		Delete a snapshot and unpin its Sia object
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
