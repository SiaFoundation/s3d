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
	usersUsage = `Usage: s3d users <command>

Manage S3 users through the admin API of the running s3d instance.

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

func runUsersCreate(ctx context.Context, cmd *flag.FlagSet) {
	args := cmd.Args()
	if len(args) != 1 {
		cmd.Usage()
		os.Exit(1)
	}
	requireAdminConfig()

	err := adminRequest(ctx, http.MethodPost, cfg.AdminAddress, cfg.AdminPassword, "/users", s3.CreateUserRequest{Name: args[0]}, nil)
	checkFatalError("failed to create user", err)
	fmt.Printf("Created user %q\n", args[0])
}

func runUsersDelete(ctx context.Context, cmd *flag.FlagSet) {
	args := cmd.Args()
	if len(args) != 1 {
		cmd.Usage()
		os.Exit(1)
	}
	requireAdminConfig()

	err := adminRequest(ctx, http.MethodDelete, cfg.AdminAddress, cfg.AdminPassword, "/users/"+url.PathEscape(args[0]), nil, nil)
	checkFatalError("failed to delete user", err)
	fmt.Printf("Deleted user %q\n", args[0])
}

func runUsersList(ctx context.Context, cmd *flag.FlagSet) {
	if len(cmd.Args()) != 0 {
		cmd.Usage()
		os.Exit(1)
	}
	requireAdminConfig()

	var users []string
	err := adminRequest(ctx, http.MethodGet, cfg.AdminAddress, cfg.AdminPassword, "/users", nil, &users)
	checkFatalError("failed to list users", err)

	if len(users) == 0 {
		fmt.Println("No users found.")
		return
	}
	for _, name := range users {
		fmt.Println(name)
	}
}
