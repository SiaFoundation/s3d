package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/SiaFoundation/s3d/build"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.sia.tech/core/types"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"lukechampine.com/flagg"
)

const (
	recoveryPhraseEnvVar = "S3D_RECOVERY_PHRASE"
	configFileEnvVar     = "S3D_CONFIG_FILE"
	dataDirEnvVar        = "S3D_DATA_DIR"
)

var cfg = Config{
	ApiAddress:     "127.0.0.1:8000",
	RecoveryPhrase: os.Getenv(recoveryPhraseEnvVar),
	Directory:      os.Getenv(dataDirEnvVar),
	Log: Log{
		File: FileLog{
			Level:   zap.NewAtomicLevelAt(zapcore.InfoLevel),
			Enabled: true,
			Format:  "json",
		},
		StdOut: StdOutLog{
			Level:      zap.NewAtomicLevelAt(zapcore.InfoLevel),
			Enabled:    true,
			Format:     "human",
			EnableANSI: runtime.GOOS != "windows",
		},
	},
	Sia: Sia{
		IndexerURL:     "https://sia.storage",
		DiskUsageLimit: 10 * (1 << 30), // 10 GiB
	},
	S3: S3{},
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	rootCmd := flagg.Root
	rootCmd.StringVar(&cfg.ApiAddress, "api.s3", cfg.ApiAddress, "address to serve S3 API on")
	versionCmd := flagg.New("version", ``)
	configCmd := flagg.New("config", ``)

	// attempt to load the config, command line flags will override any values
	// set in the config file
	configPath := tryLoadConfig()

	// determine the data directory
	cfg.Directory = dataDirectory(cfg.Directory)

	cmd := flagg.Parse(flagg.Tree{
		Cmd: rootCmd,
		Sub: []flagg.Tree{
			{Cmd: versionCmd},
			{Cmd: configCmd},
		},
	})

	switch cmd {
	case versionCmd:
		if len(cmd.Args()) != 0 {
			cmd.Usage()
			return
		}

		fmt.Println("s3d", build.Version())
		fmt.Println("Commit:", build.Commit())
		fmt.Println("Build Date:", build.Time())
		return
	case configCmd:
		if len(cmd.Args()) != 0 {
			cmd.Usage()
			return
		}

		runConfigCmd(configPath)
		return
	case rootCmd:
	}

	var logCores []zapcore.Core

	if cfg.Log.StdOut.Enabled {
		var encoder zapcore.Encoder
		switch cfg.Log.StdOut.Format {
		case "json":
			encoder = jsonEncoder()
		default:
			encoder = humanEncoder(cfg.Log.StdOut.EnableANSI)
		}

		// create the stdout logger
		logCores = append(logCores, zapcore.NewCore(encoder, zapcore.Lock(os.Stdout), cfg.Log.StdOut.Level))
	}

	if cfg.Log.File.Enabled {
		// normalize log path
		if cfg.Log.File.Path == "" {
			cfg.Log.File.Path = filepath.Join(cfg.Directory, "s3d.log")
		}

		// configure file logging
		var encoder zapcore.Encoder
		switch cfg.Log.File.Format {
		case "json":
			encoder = jsonEncoder()
		default:
			encoder = humanEncoder(false) // disable colors in file log
		}

		fileWriter, closeFn, err := zap.Open(cfg.Log.File.Path)
		checkFatalError("failed to open log file", err)
		defer closeFn()

		// create the file logger
		logCores = append(logCores, zapcore.NewCore(encoder, zapcore.Lock(fileWriter), cfg.Log.File.Level))
	}

	var log *zap.Logger
	if len(logCores) == 1 {
		log = zap.New(logCores[0], zap.AddCaller())
	} else {
		log = zap.New(zapcore.NewTee(logCores...), zap.AddCaller())
	}
	defer log.Sync()
	zap.RedirectStdLog(log.Named("stdlib"))

	adminAPIListener, err := startLocalhostListener(cfg.ApiAddress, log.Named("api.listener"))
	if err != nil {
		checkFatalError("failed to start S3 API listener", err)
	}
	defer adminAPIListener.Close()

	store, err := sqlite.OpenDatabase(filepath.Join(cfg.Directory, "s3d.db"), log)
	if err != nil {
		checkFatalError("failed to open database", err)
	}
	defer store.Close()

	// before initializing the SDK, check whether we have at least one key pair configured
	if len(cfg.Sia.KeyPairs) == 0 {
		checkFatalError("Please provide at least one valid key pair. You can do so by updating the config file or running the 'config' command", sia.ErrNoAccessKey)
	}

	builder := sdk.NewBuilder(cfg.Sia.IndexerURL, sdk.AppMetadata{
		ID:          types.HashBytes([]byte("s3d")),
		Name:        "S3d",
		Description: "A S3-compatible storage service backed by Sia",
		LogoURL:     "https://example.com/logo.png",
		ServiceURL:  "https://github.com/Siafoundation/s3d",
	})

	var sdkClient *sdk.SDK
	appKey, err := store.AppKey()
	if err == nil {
		sdkClient, err = builder.SDK(appKey, sdk.WithLogger(log.Named("sdk")))
		if err != nil {
			checkFatalError("failed to create SDK client", err)
		}
	} else if errors.Is(err, sqlite.ErrNoAppKey) {
		// register app
		if cfg.RecoveryPhrase == "" {
			cfg.RecoveryPhrase = sdk.NewSeedPhrase()
			fmt.Println("No recovery phrase found. Generated new recovery phrase...")
			fmt.Println("IMPORTANT: Store this recovery phrase in a safe place. It is required to recover your S3d account and data:")
			fmt.Println(cfg.RecoveryPhrase)
		}
		respURL, err := builder.RequestConnection(ctx)
		if err != nil {
			log.Fatal("failed to request app connection", zap.Error(err))
		}
		fmt.Println("Please approve the app connection by visiting the following URL:", respURL)
		approved, err := builder.WaitForApproval(ctx)
		if err != nil {
			log.Fatal("failed to wait for app approval", zap.Error(err))
		} else if !approved {
			log.Info("app connection was declined")
			os.Exit(0)
		}
		sdkClient, err = builder.Register(ctx, cfg.RecoveryPhrase)
		if err != nil {
			log.Fatal("failed to register app", zap.Error(err))
		}
		if err := store.SetAppKey(sdkClient.AppKey()); err != nil {
			log.Fatal("failed to store app key in database", zap.Error(err))
		}
	} else {
		checkFatalError("failed to get app key from database", err)
	}

	var siaOpts []sia.Option
	for _, kp := range cfg.Sia.KeyPairs {
		siaOpts = append(siaOpts, sia.WithKeyPair(kp.AccessKey, kp.SecretKey))
	}
	siaOpts = append(siaOpts, sia.WithLogger(log.Named("backend")))
	if cfg.Sia.DiskUsageLimit > 0 {
		siaOpts = append(siaOpts, sia.WithDiskUsageLimit(cfg.Sia.DiskUsageLimit))
	}

	backend, err := sia.New(ctx, sia.NewSDK(sdkClient), store, cfg.Directory, siaOpts...)
	if errors.Is(err, sia.ErrNoAccessKey) {
		checkFatalError("Please provide at least one valid key pair. You can do so by updating the config file or running the 'config' command", err)
	} else if err != nil {
		checkFatalError("failed to create Sia backend", err)
	}
	defer backend.Close()

	s3Handler := s3.New(backend, s3.WithHostBucketBases(cfg.S3.HostBases),
		s3.WithLogger(log))

	server := http.Server{
		Handler: s3Handler,
	}

	go func() {
		log.Debug("starting S3 API", zap.String("address", cfg.ApiAddress))
		if err := server.Serve(adminAPIListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("failed to serve S3 API", zap.Error(err))
		}
	}()

	log.Info("server started", zap.Stringer("admin", adminAPIListener.Addr()), zap.Stringer("application", adminAPIListener.Addr()))
	<-ctx.Done()
	log.Info("shutdown signal received...attempting graceful shutdown...")

	// shutdown signal received - shut down gracefully
	shutdownCtx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("failed to shutdown S3 API", zap.Error(err))
	}
	select {
	case <-shutdownCtx.Done():
		log.Info("graceful shutdown was interrupted")
	default:
	}
	log.Info("...shutdown complete")
}

// checkFatalError prints an error message to stderr and exits with a 1 exit code. If err is nil, this is a no-op.
func checkFatalError(context string, err error) {
	if err == nil {
		return
	}
	os.Stderr.WriteString(fmt.Sprintf("%s: %s\n", context, err))
	os.Exit(1)
}

func dataDirectory(fp string) string {
	// use the provided path if it's not empty
	if fp != "" {
		return fp
	}

	// default to the operating system's application directory
	switch runtime.GOOS {
	case "windows":
		return filepath.Join(os.Getenv("APPDATA"), "s3d")
	case "darwin":
		return filepath.Join(os.Getenv("HOME"), "Library", "Application Support", "s3d")
	case "linux", "freebsd", "openbsd":
		return filepath.Join(string(filepath.Separator), "var", "lib", "s3d")
	default:
		return "."
	}
}

func startLocalhostListener(listenAddr string, log *zap.Logger) (l net.Listener, err error) {
	addr, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse API address: %w", err)
	}

	// if the address is not localhost, listen on the address as-is
	if addr != "localhost" {
		return net.Listen("tcp", listenAddr)
	}

	// localhost fails on some new installs of Windows 11, so try a few
	// different addresses
	tryAddresses := []string{
		net.JoinHostPort("localhost", port), // original address
		net.JoinHostPort("127.0.0.1", port), // IPv4 loopback
		net.JoinHostPort("::1", port),       // IPv6 loopback
	}

	for _, addr := range tryAddresses {
		l, err = net.Listen("tcp", addr)
		if err == nil {
			return
		}
		log.Debug("failed to listen on fallback address", zap.String("address", addr), zap.Error(err))
	}
	return
}

// tryLoadConfig tries to load the config file. It will try multiple locations
// based on GOOS starting with PWD/s3d.yml. If the file does not exist, it will
// try the next location. If an error occurs while loading the file, it will
// print the error and exit. If the config is successfully loaded, the path to
// the config file is returned.
func tryLoadConfig() string {
	for _, fp := range tryConfigPaths() {
		if err := LoadFile(fp, &cfg); err == nil {
			return fp
		} else if !errors.Is(err, os.ErrNotExist) {
			checkFatalError("failed to load config file", err)
		}
	}
	return ""
}

func tryConfigPaths() []string {
	if str := os.Getenv(configFileEnvVar); str != "" {
		return []string{str}
	}

	paths := []string{
		"s3d.yml",
	}
	if str := os.Getenv(dataDirEnvVar); str != "" {
		paths = append(paths, filepath.Join(str, "s3d.yml"))
	}

	switch runtime.GOOS {
	case "windows":
		paths = append(paths, filepath.Join(os.Getenv("APPDATA"), "s3d", "s3d.yml"))
	case "darwin":
		paths = append(paths, filepath.Join(os.Getenv("HOME"), "Library", "Application Support", "s3d", "s3d.yml"))
	case "linux", "freebsd", "openbsd":
		paths = append(paths,
			filepath.Join(string(filepath.Separator), "etc", "s3d", "s3d.yml"),
			filepath.Join(string(filepath.Separator), "var", "lib", "s3d", "s3d.yml"), // old default for the Linux service
		)
	}
	return paths
}

// jsonEncoder returns a zapcore.Encoder that encodes logs as JSON intended for
// parsing.
func jsonEncoder() zapcore.Encoder {
	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.RFC3339TimeEncoder
	return zapcore.NewJSONEncoder(cfg)
}

// humanEncoder returns a zapcore.Encoder that encodes logs as human-readable
// text.
func humanEncoder(showColors bool) zapcore.Encoder {
	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.RFC3339TimeEncoder
	cfg.EncodeDuration = zapcore.StringDurationEncoder

	if showColors {
		cfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		cfg.EncodeLevel = zapcore.CapitalLevelEncoder
	}

	cfg.StacktraceKey = ""
	cfg.CallerKey = ""
	return zapcore.NewConsoleEncoder(cfg)
}
