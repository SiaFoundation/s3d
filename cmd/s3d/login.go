package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
)

func openStore(log *zap.Logger) (*sqlite.Store, error) {
	if err := os.MkdirAll(cfg.Directory, 0700); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}
	return sqlite.OpenDatabase(filepath.Join(cfg.Directory, "s3d.db"), log)
}

func newSDKBuilder(indexerURL string) *sdk.Builder {
	return sdk.NewBuilder(indexerURL, sdk.AppMetadata{
		ID:          types.HashBytes([]byte("s3d")),
		Name:        "S3d",
		Description: "A S3-compatible storage service backed by Sia",
		LogoURL:     "https://example.com/logo.png",
		ServiceURL:  "https://github.com/SiaFoundation/s3d",
	})
}

func runLoginCmd(ctx context.Context, configPath string) {
	// if no config exists yet, run the config wizard first
	if configPath == "" {
		fmt.Println("No existing config found. Launching configuration wizard.")
		fmt.Println("")
		runConfigCmd("")
		fmt.Println("")
	}

	fmt.Println("This command will register s3d with the Sia indexer.")
	fmt.Println("You will be prompted for a recovery phrase and asked to visit a URL to approve the app connection.")
	fmt.Println("")

	store, err := openStore(zap.NewNop())
	checkFatalError("failed to open database", err)
	defer store.Close()

	if _, existingURL, err := store.AppKey(); err == nil {
		fmt.Println(ansiStyle("33", fmt.Sprintf("This app is already registered with %s.", existingURL)))
		return
	} else if !errors.Is(err, sqlite.ErrNoAppKey) {
		checkFatalError("failed to check app key", err)
	}

	indexerURL := readInput("Indexer URL (default: https://sia.storage)")
	if indexerURL == "" {
		indexerURL = "https://sia.storage"
	}

	phrase := promptRecoveryPhrase()

	builder := newSDKBuilder(indexerURL)

	respURL, err := builder.RequestConnection(ctx)
	checkFatalError("failed to request app connection", err)
	fmt.Println("")
	fmt.Println("Please approve the app connection by visiting the following URL:", ansiStyle("34;1", respURL))
	fmt.Println("")

	err = builder.WaitForApproval(ctx)
	if err != nil && !errors.Is(err, sdk.ErrUserRejected) {
		checkFatalError("failed to wait for app approval", err)
	} else if errors.Is(err, sdk.ErrUserRejected) {
		fmt.Println(ansiStyle("31", "app connection was declined"))
		os.Exit(1)
	}

	sdkClient, err := builder.Register(ctx, phrase)
	checkFatalError("failed to register app", err)

	checkFatalError("failed to store app key", store.SetAppKey(sdkClient.AppKey(), indexerURL))

	fmt.Println(ansiStyle("32", "Login successful. You can now start s3d."))
}

func promptRecoveryPhrase() string {
	fmt.Println("Enter your 12-word recovery phrase.")
	fmt.Println("(Leave blank to generate a new one.)")

	for {
		input := readPasswordInput("Enter recovery phrase")
		if input == "" {
			phrase := wallet.NewSeedPhrase()

			fmt.Println("")
			fmt.Println("A new recovery phrase has been generated below. " + ansiStyle("1", "Write it down and keep it safe."))
			fmt.Println("Your recovery phrase is used to register your app with the indexer.")
			fmt.Println("")
			fmt.Println("  Recovery Phrase: " + ansiStyle("34;1", phrase))
			fmt.Println("")

			for {
				confirm := readPasswordInput("Confirm recovery phrase")
				if confirm == phrase {
					return phrase
				}
				fmt.Println(ansiStyle("31", "Recovery phrases do not match!"))
				fmt.Println(ansiStyle("31", fmt.Sprintf("Expected: %q", phrase)))
				fmt.Println(ansiStyle("31", fmt.Sprintf("Entered:  %q", confirm)))
				fmt.Println("")
			}
		}

		var seed [32]byte
		if err := wallet.SeedFromPhrase(&seed, input); err != nil {
			fmt.Println(ansiStyle("31", fmt.Sprintf("Invalid recovery phrase: %s", err.Error())))
			fmt.Println("")
			continue
		}
		return input
	}
}
