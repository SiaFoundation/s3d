package main

import (
	"flag"
	"fmt"
	"os"

	"go.uber.org/zap"
)

const (
	usersUsage = `Usage: s3d users <command>

Manage S3 users.

Commands:
  create    Create a new user
  delete    Delete a user
  list      List all users`

	usersCreateUsage = `Usage: s3d users create <username>

Create a new user.`

	usersDeleteUsage = `Usage: s3d users delete <username>

Delete an existing user.`

	usersListUsage = `Usage: s3d users list

List all users.`
)

func runUsersCreate(cmd *flag.FlagSet) {
	args := cmd.Args()
	if len(args) != 1 {
		cmd.Usage()
		os.Exit(1)
	}

	store, err := openStore(zap.NewNop())
	checkFatalError("failed to open database", err)
	defer store.Close()

	checkFatalError("failed to create user", store.CreateUser(args[0]))
	fmt.Printf("Created user %q\n", args[0])
}

func runUsersDelete(cmd *flag.FlagSet) {
	args := cmd.Args()
	if len(args) != 1 {
		cmd.Usage()
		os.Exit(1)
	}

	store, err := openStore(zap.NewNop())
	checkFatalError("failed to open database", err)
	defer store.Close()

	checkFatalError("failed to delete user", store.DeleteUser(args[0]))
	fmt.Printf("Deleted user %q\n", args[0])
}

func runUsersList(cmd *flag.FlagSet) {
	if len(cmd.Args()) != 0 {
		cmd.Usage()
		os.Exit(1)
	}

	store, err := openStore(zap.NewNop())
	checkFatalError("failed to open database", err)
	defer store.Close()

	users, err := store.ListUsers()
	checkFatalError("failed to list users", err)

	if len(users) == 0 {
		fmt.Println("No users found.")
		return
	}
	for _, name := range users {
		fmt.Println(name)
	}
}
