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

	"github.com/SiaFoundation/s3d/s3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	appSecretEnvVar = "S3D_APP_SECRET"
	accessKeyEnv    = "S3D_ACCESS_KEY"
	secretKeyEnv    = "S3D_SECRET_KEY"

	configFileEnvVar = "S3D_CONFIG_FILE"
	dataDirEnvVar    = "S3D_DATA_DIR"
)

var cfg = Config{
	ApiAddress: "127.0.0.1:8000",
	AppSecret:  os.Getenv(appSecretEnvVar),
	Directory:  os.Getenv(dataDirEnvVar),
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
	S3: S3{
		AccessKey: os.Getenv(accessKeyEnv),
		SecretKey: os.Getenv(secretKeyEnv),
	},
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// determine the data directory
	cfg.Directory = dataDirectory(cfg.Directory)

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
			cfg.Log.File.Path = filepath.Join(cfg.Directory, "indexd.log")
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

	s3Handler := s3.New(nil, s3.WithHostBucketBases(cfg.S3.HostBases),
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
		return filepath.Join(os.Getenv("APPDATA"), "indexd")
	case "darwin":
		return filepath.Join(os.Getenv("HOME"), "Library", "Application Support", "indexd")
	case "linux", "freebsd", "openbsd":
		return filepath.Join(string(filepath.Separator), "var", "lib", "indexd")
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
// based on GOOS starting with PWD/indexd.yml. If the file does not exist, it will
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
		"indexd.yml",
	}
	if str := os.Getenv(dataDirEnvVar); str != "" {
		paths = append(paths, filepath.Join(str, "indexd.yml"))
	}

	switch runtime.GOOS {
	case "windows":
		paths = append(paths, filepath.Join(os.Getenv("APPDATA"), "indexd", "indexd.yml"))
	case "darwin":
		paths = append(paths, filepath.Join(os.Getenv("HOME"), "Library", "Application Support", "indexd", "indexd.yml"))
	case "linux", "freebsd", "openbsd":
		paths = append(paths,
			filepath.Join(string(filepath.Separator), "etc", "indexd", "indexd.yml"),
			filepath.Join(string(filepath.Separator), "var", "lib", "indexd", "indexd.yml"), // old default for the Linux service
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
