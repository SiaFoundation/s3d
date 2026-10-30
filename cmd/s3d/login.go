package main

import (
	"context"
	"encoding/hex"
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

func newSDKBuilder(indexerURL string) (*sdk.Builder, error) {
	return sdk.NewBuilder(indexerURL, sdk.AppMetadata{
		AppID:       types.HashBytes([]byte("s3d")),
		Name:        "S3d",
		Description: "A S3-compatible storage service backed by Sia",
		LogoURL:     "https://example.com/logo.png",
		ServiceURL:  "https://github.com/SiaFoundation/s3d",
	})
}

func runLoginCmd(ctx context.Context, configPath string) {
	preAuthorizedKey, err := preAuthorizedKeyFromEnv()
	checkFatalError("failed to read pre-authorized key", err)

	// if no config exists yet, run the config wizard first
	if configPath == "" {
		fmt.Println("No existing config found. Launching configuration wizard.")
		fmt.Println("")
		runConfigCmd("")
		fmt.Println("")
	}

	fmt.Println("This command will register s3d with the Sia indexer.")
	if preAuthorizedKey != nil {
		fmt.Println("You will be prompted for a recovery phrase. The app connection is approved with the pre-authorized key from " + preAuthorizedKeyEnvVar + ".")
	} else {
		fmt.Println("You will be prompted for a recovery phrase and asked to visit a URL to approve the app connection.")
	}
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

	indexerURL := promptIndexerURL()
	phrase := promptRecoveryPhrase()

	var sdkClient *sdk.SDK
	if preAuthorizedKey != nil {
		sdkClient, err = connectPreAuthorized(ctx, indexerURL, phrase, preAuthorizedKey)
	} else {
		sdkClient, err = registerSDK(ctx, indexerURL, phrase)
	}
	checkFatalError("failed to register app", err)
	defer sdkClient.Close()

	checkFatalError("failed to store app key", store.SetAppKey(sdkClient.AppKey(), indexerURL))

	fmt.Println(ansiStyle("32", "Login successful. You can now start s3d."))
}

// preAuthorizedKeyFromEnv returns the pre-authorized key set in the
// environment, or nil when the variable is unset.
func preAuthorizedKeyFromEnv() (types.PrivateKey, error) {
	v := os.Getenv(preAuthorizedKeyEnvVar)
	if v == "" {
		return nil, nil
	}

	key, err := hex.DecodeString(v)
	if err != nil {
		return nil, fmt.Errorf("%s is not valid hex: %w", preAuthorizedKeyEnvVar, err)
	} else if len(key) < 32 {
		return nil, fmt.Errorf("%s must be at least 32 bytes", preAuthorizedKeyEnvVar)
	}
	return key, nil
}

// connectPreAuthorized registers the app with a key the indexer has already
// authorized, so no approval in the browser is needed.
func connectPreAuthorized(ctx context.Context, indexerURL, phrase string, key types.PrivateKey) (*sdk.SDK, error) {
	builder, err := newSDKBuilder(indexerURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create SDK builder: %w", err)
	}
	defer builder.Close()

	return builder.ConnectPreAuthorized(ctx, key, phrase)
}

// existingAppKey returns the app key recorded in the configured data directory
// and whether one was found. A directory holding no database is left
// untouched.
func existingAppKey() (types.PrivateKey, string, bool, error) {
	dbPath := filepath.Join(cfg.Directory, "s3d.db")
	if _, err := os.Stat(dbPath); errors.Is(err, os.ErrNotExist) {
		return nil, "", false, nil
	} else if err != nil {
		return nil, "", false, fmt.Errorf("failed to check for a database: %w", err)
	}

	appKey, indexerURL, err := sqlite.ReadAppKey(dbPath)
	if errors.Is(err, sqlite.ErrNoAppKey) {
		return nil, "", false, nil
	} else if err != nil {
		return nil, "", false, fmt.Errorf("failed to read app key: %w", err)
	}
	return appKey, indexerURL, true, nil
}

// registerSDK walks the user through approving an app connection and returns
// the registered client. The app key it derives is not persisted.
func registerSDK(ctx context.Context, indexerURL, phrase string) (*sdk.SDK, error) {
	builder, err := newSDKBuilder(indexerURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create SDK builder: %w", err)
	}
	defer builder.Close()

	respURL, err := builder.RequestConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to request app connection: %w", err)
	}
	fmt.Println("")
	fmt.Println("Please approve the app connection by visiting the following URL:", ansiStyle("34;1", respURL))
	fmt.Println("")

	if err := builder.WaitForApproval(ctx); errors.Is(err, sdk.ErrUserRejected) {
		return nil, errors.New("the app connection was declined")
	} else if err != nil {
		return nil, fmt.Errorf("failed to wait for app approval: %w", err)
	}

	sdkClient, err := builder.Register(ctx, phrase)
	if err != nil {
		return nil, err
	}
	return sdkClient, nil
}

// promptIndexerURL reads the indexer URL, defaulting to the production one.
func promptIndexerURL() string {
	if indexerURL := readInput("Indexer URL (default: https://sia.storage)"); indexerURL != "" {
		return indexerURL
	}
	return "https://sia.storage"
}

// promptExistingRecoveryPhrase reads the recovery phrase of an account that
// already exists. A blank line is rejected rather than generating a phrase.
func promptExistingRecoveryPhrase() string {
	fmt.Println("Enter the 12-word recovery phrase of the account holding the snapshots.")

	for {
		input := readPasswordInput("Enter recovery phrase")

		var seed [32]byte
		if err := wallet.SeedFromPhrase(&seed, input); err != nil {
			fmt.Println(ansiStyle("31", fmt.Sprintf("Invalid recovery phrase: %s", err.Error())))
			fmt.Println("")
			continue
		}
		return input
	}
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
