package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/SiaFoundation/s3d/s3/auth"
	"go.uber.org/zap"
)

const (
	keysUsage = `Usage: s3d keys <command>

Manage S3 access keys.

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

func runKeysCreate(cmd *flag.FlagSet, accessKey, secretKey string) {
	args := cmd.Args()
	if len(args) != 1 {
		cmd.Usage()
		os.Exit(1)
	}
	userName := args[0]

	// validate or generate
	if accessKey == "" && secretKey == "" {
		accessKey, secretKey = auth.GenerateAccessKey()
	} else if accessKey == "" || secretKey == "" {
		fmt.Fprintln(os.Stderr, "Both --access-key and --secret-key must be provided together, or omit both to auto-generate.")
		os.Exit(1)
	} else {
		checkFatalError("invalid key pair", auth.ValidateAccessKey(accessKey, secretKey))
	}

	store, err := openStore(zap.NewNop())
	checkFatalError("failed to open database", err)
	defer store.Close()

	checkFatalError("failed to create access key", store.CreateAccessKey(userName, accessKey, secretKey))

	fmt.Println("Created access key for user", userName)
	fmt.Println("")
	fmt.Printf("  Access Key: %s\n", accessKey)
	fmt.Printf("  Secret Key: %s\n", secretKey)
}

func runKeysDelete(cmd *flag.FlagSet) {
	args := cmd.Args()
	if len(args) != 1 {
		cmd.Usage()
		os.Exit(1)
	}

	store, err := openStore(zap.NewNop())
	checkFatalError("failed to open database", err)
	defer store.Close()

	checkFatalError("failed to delete access key", store.DeleteAccessKey(args[0]))
	fmt.Printf("Deleted access key %q\n", args[0])
}

func runKeysList(cmd *flag.FlagSet) {
	args := cmd.Args()
	if len(args) > 1 {
		cmd.Usage()
		os.Exit(1)
	}

	var userName *string
	if len(args) == 1 {
		userName = &args[0]
	}

	store, err := openStore(zap.NewNop())
	checkFatalError("failed to open database", err)
	defer store.Close()

	keys, err := store.ListAccessKeys(userName)
	checkFatalError("failed to list access keys", err)

	if len(keys) == 0 {
		fmt.Println("No access keys found.")
		return
	}
	for _, k := range keys {
		fmt.Printf("%s\t%s\t%s\n", k.AccessKeyID, k.SecretKey, k.UserName)
	}
}
