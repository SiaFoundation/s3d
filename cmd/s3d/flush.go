package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
)

const flushUsage = `Usage: s3d flush

Upload all pending objects to Sia immediately, regardless of padding. Blocks
until the uploads complete.`

func runFlush(ctx context.Context, cmd *flag.FlagSet) {
	if len(cmd.Args()) != 0 {
		cmd.Usage()
		os.Exit(1)
	}
	client := newAdminClient()

	fmt.Println("Flushing pending objects to Sia. This may take a while...")
	checkFatalError("failed to flush objects", client.request(ctx, http.MethodPost, "/objects/flush", nil, nil))
	fmt.Println("Flushed all pending objects.")
}
