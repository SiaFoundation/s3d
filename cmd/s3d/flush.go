package main

import (
	"context"
	"flag"
	"fmt"
	"os"
)

const flushUsage = `Usage: s3d flush

Upload all pending objects to Sia immediately, regardless of padding. Blocks
until the uploads complete.

Reads the admin address and password from the loaded config file or
S3D_CONFIG_FILE.`

func runFlush(ctx context.Context, cmd *flag.FlagSet) {
	if len(cmd.Args()) != 0 {
		cmd.Usage()
		os.Exit(1)
	}
	requireAdminConfig()

	fmt.Println("Flushing pending objects to Sia. This may take a while...")
	checkFatalError("failed to flush objects", postAdmin(ctx, cfg.AdminAddress, cfg.AdminPassword, "/objects/flush"))
	fmt.Println("Flushed all pending objects.")
}
