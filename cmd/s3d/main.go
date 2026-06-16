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
	"time"

	"github.com/SiaFoundation/s3d/build"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.sia.tech/jape"
	sdk "go.sia.tech/siastorage"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"lukechampine.com/flagg"
)

const (
	configFileEnvVar = "S3D_CONFIG_FILE"
	dataDirEnvVar    = "S3D_DATA_DIR"

	rootUsage = `Usage:
s3d [flags] [command]

Run 's3d' with no command to start the S3 gateway daemon.

Commands:
	version		Print the s3d version
	config		Interactively configure s3d
	login		Register this s3d instance with the indexer
	status		Print a basic overview of the running s3d instance
	users		Manage users
	keys		Manage S3 access keys
`

	versionUsage = `Usage: s3d version

Print the s3d version.`

	configUsage = `Usage: s3d config

Interactively configure s3d. The resulting config will be saved to s3d.yml or
the file specified by the S3D_CONFIG_FILE environment variable.`

	loginUsage = `Usage: s3d login

Register this s3d instance with the indexer and obtain an app key.`
)

var cfg = Config{
	ApiAddress:   "127.0.0.1:8000",
	AdminAddress: "127.0.0.1:8001",
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
		DiskUsageLimit: 10 * (1 << 30), // 10 GiB
	},
	S3: S3{},
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	rootCmd := flagg.Root
	rootCmd.Usage = flagg.SimpleUsage(rootCmd, rootUsage)
	rootCmd.StringVar(&cfg.ApiAddress, "api.s3", cfg.ApiAddress, "address to serve S3 API on")
	versionCmd := flagg.New("version", versionUsage)
	configCmd := flagg.New("config", configUsage)
	loginCmd := flagg.New("login", loginUsage)
	statusCmd := flagg.New("status", statusUsage)

	usersCmd := flagg.New("users", usersUsage)
	usersCreateCmd := flagg.New("create", usersCreateUsage)
	usersDeleteCmd := flagg.New("delete", usersDeleteUsage)
	usersListCmd := flagg.New("list", usersListUsage)

	keysCmd := flagg.New("keys", keysUsage)
	keysCreateCmd := flagg.New("create", keysCreateUsage)
	keysDeleteCmd := flagg.New("delete", keysDeleteUsage)
	keysListCmd := flagg.New("list", keysListUsage)

	var keysCreateAccessKey, keysCreateSecretKey string
	keysCreateCmd.StringVar(&keysCreateAccessKey, "access-key", "", "access key ID (auto-generated if empty)")
	keysCreateCmd.StringVar(&keysCreateSecretKey, "secret-key", "", "secret key (auto-generated if empty)")

	// attempt to load the config file
	configPath := tryLoadConfig()

	// apply environment variable overrides
	applyEnvVars(&cfg)

	// fall back to default directory if not set by now
	if cfg.Directory == "" {
		cfg.Directory = applicationDirectoryOS()
	}

	cmd := flagg.Parse(flagg.Tree{
		Cmd: rootCmd,
		Sub: []flagg.Tree{
			{Cmd: versionCmd},
			{Cmd: configCmd},
			{Cmd: loginCmd},
			{Cmd: statusCmd},
			{
				Cmd: usersCmd,
				Sub: []flagg.Tree{
					{Cmd: usersCreateCmd},
					{Cmd: usersDeleteCmd},
					{Cmd: usersListCmd},
				},
			},
			{
				Cmd: keysCmd,
				Sub: []flagg.Tree{
					{Cmd: keysCreateCmd},
					{Cmd: keysDeleteCmd},
					{Cmd: keysListCmd},
				},
			},
		},
	})

	switch cmd {
	case versionCmd:
		if len(cmd.Args()) != 0 {
			cmd.Usage()
			os.Exit(1)
		}

		fmt.Println("s3d", build.Version())
		fmt.Println("Commit:", build.Commit())
		fmt.Println("Build Date:", build.Time())
		return
	case configCmd:
		if len(cmd.Args()) != 0 {
			cmd.Usage()
			os.Exit(1)
		}

		runConfigCmd(configPath)
		return
	case loginCmd:
		if len(cmd.Args()) != 0 {
			cmd.Usage()
			os.Exit(1)
		}

		runLoginCmd(ctx, configPath)
		return
	case statusCmd:
		runStatus(ctx, statusCmd)
		return
	case usersCmd:
		cmd.Usage()
		if len(cmd.Args()) != 0 {
			os.Exit(1)
		}
		return
	case usersCreateCmd:
		runUsersCreate(usersCreateCmd)
		return
	case usersDeleteCmd:
		runUsersDelete(usersDeleteCmd)
		return
	case usersListCmd:
		runUsersList(usersListCmd)
		return
	case keysCmd:
		cmd.Usage()
		if len(cmd.Args()) != 0 {
			os.Exit(1)
		}
		return
	case keysCreateCmd:
		runKeysCreate(keysCreateCmd, keysCreateAccessKey, keysCreateSecretKey)
		return
	case keysDeleteCmd:
		runKeysDelete(keysDeleteCmd)
		return
	case keysListCmd:
		runKeysList(keysListCmd)
		return
	case rootCmd:
		if len(cmd.Args()) != 0 {
			cmd.Usage()
			os.Exit(1)
		}
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

	log.Info("s3d", zap.String("version", build.Version()), zap.String("commit", build.Commit()), zap.Time("buildDate", build.Time()))

	adminAPIListener, err := startLocalhostListener(cfg.ApiAddress, log.Named("api.listener"))
	if err != nil {
		checkFatalError("failed to start S3 API listener", err)
	}
	defer adminAPIListener.Close()

	store, err := openStore(log)
	if err != nil {
		checkFatalError("failed to open database", err)
	}
	defer store.Close()

	appKey, indexerURL, err := store.AppKey()
	if errors.Is(err, sqlite.ErrNoAppKey) {
		os.Stderr.WriteString("No app key found. Please run 's3d login' to register the app.\n")
		os.Exit(1)
	} else if err != nil {
		checkFatalError("failed to get app key from database", err)
	}
	builder := newSDKBuilder(indexerURL)
	sdkClient, err := builder.SDK(appKey, sdk.WithLogger(log.Named("sdk")))
	if err != nil {
		checkFatalError("failed to create SDK client", err)
	}

	backend, err := sia.New(ctx, sia.NewSDK(sdkClient), store, cfg.Directory, sia.WithDiskUsageLimit(cfg.Sia.DiskUsageLimit), sia.WithLogger(log.Named("backend")))
	if err != nil {
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

	// serve the admin API on a separate listener
	if cfg.AdminAddress == "" {
		checkFatalError("invalid admin configuration", errors.New("admin address must be set"))
	} else if cfg.AdminAddress == cfg.ApiAddress {
		checkFatalError("invalid admin configuration", errors.New("admin address must differ from the S3 API address"))
	} else if cfg.AdminPassword == "" {
		checkFatalError("invalid admin configuration", errors.New("admin password must be set"))
	}

	adminListener, err := startLocalhostListener(cfg.AdminAddress, log.Named("admin.listener"))
	if err != nil {
		checkFatalError("failed to start admin listener", err)
	}
	defer adminListener.Close()

	adminHandler := jape.BasicAuth(cfg.AdminPassword)(s3.NewAdmin(backend, s3.WithLogger(log)))
	adminServer := &http.Server{
		Handler:     adminHandler,
		ReadTimeout: 30 * time.Second,
		// no WriteTimeout: /objects/flush blocks until all pending objects are
		// uploaded, which can take longer than any fixed deadline.
	}
	defer adminServer.Close()

	go func() {
		log.Debug("starting admin server", zap.String("address", cfg.AdminAddress))
		if err := adminServer.Serve(adminListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("failed to serve admin server", zap.Error(err))
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
	if err := adminServer.Shutdown(shutdownCtx); err != nil {
		log.Error("failed to shutdown admin server", zap.Error(err))
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
	fmt.Fprintf(os.Stderr, "%s: %s\n", context, err)
	os.Exit(1)
}

func applicationDirectoryOS() string {
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

// applyEnvVars overrides config values with environment variables. This is
// called after the config file and CLI flags have been applied so that
// environment variables take the highest precedence.
func applyEnvVars(cfg *Config) {
	if v := os.Getenv(dataDirEnvVar); v != "" {
		cfg.Directory = v
	}
}
