package main

import (
	"fmt"
	"os"

	"go.uber.org/zap"
)

const usersUsage = `Usage: s3d users <command>

Commands:
  create <username>    Create a new user
  delete <username>    Delete a user
  list                 List all users`

func runUsersCmd(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, usersUsage)
		os.Exit(1)
	}

	switch args[0] {
	case "create":
		if len(args) != 2 {
			fmt.Fprintln(os.Stderr, usersUsage)
			os.Exit(1)
		}

		store, err := openStore(zap.NewNop())
		checkFatalError("failed to open database", err)
		defer store.Close()

		checkFatalError("failed to create user", store.CreateUser(args[1]))
		fmt.Printf("Created user %q\n", args[1])

	case "delete":
		if len(args) != 2 {
			fmt.Fprintln(os.Stderr, usersUsage)
			os.Exit(1)
		}

		store, err := openStore(zap.NewNop())
		checkFatalError("failed to open database", err)
		defer store.Close()

		checkFatalError("failed to delete user", store.DeleteUser(args[1]))
		fmt.Printf("Deleted user %q\n", args[1])

	case "list":
		if len(args) != 1 {
			fmt.Fprintln(os.Stderr, usersUsage)
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

	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand %q\n\n", args[0])
		fmt.Fprintln(os.Stderr, usersUsage)
		os.Exit(1)
	}
}
