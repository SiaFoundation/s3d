package main

import (
	"encoding/base32"
	"encoding/base64"
	"flag"
	"fmt"
	"os"

	"go.uber.org/zap"
	"lukechampine.com/frand"
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

func generateAccessKey() (accessKey, secretKey string) {
	// 12 random bytes encode to exactly 20 base32 characters
	akBytes := make([]byte, 12)
	frand.Read(akBytes)
	accessKey = base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(akBytes)

	// 30 random bytes encode to exactly 40 base64 characters
	skBytes := make([]byte, 30)
	frand.Read(skBytes)
	secretKey = base64.StdEncoding.EncodeToString(skBytes)

	return
}

func runKeysCreate(cmd *flag.FlagSet, accessKey, secretKey string) {
	args := cmd.Args()
	if len(args) != 1 {
		cmd.Usage()
		os.Exit(1)
	}
	userName := args[0]

	// validate or generate
	if accessKey == "" && secretKey == "" {
		accessKey, secretKey = generateAccessKey()
	} else if accessKey == "" || secretKey == "" {
		fmt.Fprintln(os.Stderr, "Both --access-key and --secret-key must be provided together, or omit both to auto-generate.")
		os.Exit(1)
	} else {
		if len(accessKey) < 16 || len(accessKey) > 128 {
			fmt.Fprintln(os.Stderr, "Access key must be between 16 and 128 characters.")
			os.Exit(1)
		}
		if len(secretKey) < 32 || len(secretKey) > 128 {
			fmt.Fprintln(os.Stderr, "Secret key must be between 32 and 128 characters.")
			os.Exit(1)
		}
	}

	store, err := openStore(zap.NewNop())
	checkFatalError("failed to open database", err)
	defer store.Close()

	checkFatalError("failed to create access key", store.CreateAccessKey(userName, accessKey, secretKey))

	fmt.Println("Created access key for user", userName)
	fmt.Println("")
	fmt.Printf("  Access Key: %s\n", accessKey)
	fmt.Printf("  Secret Key: %s\n", secretKey)
	fmt.Println("")
	fmt.Println("Save these credentials. The secret key will not be shown again.")
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
		fmt.Printf("%s\t%s\n", k.AccessKeyID, k.UserName)
	}
}
