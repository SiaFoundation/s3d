package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"

	"github.com/SiaFoundation/s3d/s3"
)

const (
	keysUsage = `Usage: s3d keys <command>

Manage S3 access keys through the admin API of the running s3d instance.

Commands:
  create    Create a new access key
  delete    Delete an access key
  list      List access keys`

	keysCreateUsage = `Usage: s3d keys create [--access-key <id> --secret-key <secret>] <username>

Create a new access key pair for a user. The pair is auto-generated when both
flags are omitted.`

	keysDeleteUsage = `Usage: s3d keys delete <access-key-id>

Delete an access key.`

	keysListUsage = `Usage: s3d keys list [username]

List access keys, optionally filtered by username.`
)

func runKeysCreate(ctx context.Context, cmd *flag.FlagSet, accessKey, secretKey string) {
	args := cmd.Args()
	if len(args) != 1 {
		cmd.Usage()
		os.Exit(1)
	}
	userName := args[0]

	if (accessKey == "") != (secretKey == "") {
		fmt.Fprintln(os.Stderr, "Both --access-key and --secret-key must be provided together, or omit both to auto-generate.")
		os.Exit(1)
	}
	client := newAdminClient()

	req := s3.CreateAccessKeyRequest{
		UserName:    userName,
		AccessKeyID: accessKey,
		SecretKey:   secretKey,
	}
	var key s3.AccessKey
	err := client.request(ctx, http.MethodPost, "/keys", req, &key)
	checkFatalError("failed to create access key", err)

	fmt.Println("Created access key for user", userName)
	fmt.Println("")
	fmt.Printf("  Access Key: %s\n", key.AccessKeyID)
	fmt.Printf("  Secret Key: %s\n", key.SecretKey)
}

func runKeysDelete(ctx context.Context, cmd *flag.FlagSet) {
	args := cmd.Args()
	if len(args) != 1 {
		cmd.Usage()
		os.Exit(1)
	}
	client := newAdminClient()

	err := client.request(ctx, http.MethodDelete, "/keys/"+url.PathEscape(args[0]), nil, nil)
	checkFatalError("failed to delete access key", err)
	fmt.Printf("Deleted access key %q\n", args[0])
}

func runKeysList(ctx context.Context, cmd *flag.FlagSet) {
	args := cmd.Args()
	if len(args) > 1 {
		cmd.Usage()
		os.Exit(1)
	}
	client := newAdminClient()

	route := "/keys"
	if len(args) == 1 {
		route += "?username=" + url.QueryEscape(args[0])
	}
	var keys []s3.AccessKeyMetadata
	err := client.request(ctx, http.MethodGet, route, nil, &keys)
	checkFatalError("failed to list access keys", err)

	if len(keys) == 0 {
		fmt.Println("No access keys found.")
		return
	}
	for _, k := range keys {
		fmt.Printf("%s\t%s\n", k.AccessKeyID, k.UserName)
	}
}
