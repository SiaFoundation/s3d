package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/sia"
	"github.com/SiaFoundation/s3d/sia/persist/sqlite"
	"go.uber.org/zap"
)

const (
	toxAccessKey = "0555b35654ad1656d804"
	toxSecretKey = "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="

	altAccessKey = "NOPQRSTUVWXYZABCDEFG"
	altSecretKey = "nopqrstuvwxyzabcdefghijklmnabcdefghijklm"

	tenantAccessKey = "HIJKLMNOPQRSTUVWXYZA"
	tenantSecretKey = "opqrstuvwxyzabcdefghijklmnopqrstuvwxyzab"
)

var (
	toxKeyPairs = []struct {
		AccessKey string
		SecretKey string
	}{
		{toxAccessKey, toxSecretKey},
		{altAccessKey, altSecretKey},
		{tenantAccessKey, tenantSecretKey},
	}
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("failed to create logger: %v", err)
	}

	dir, err := os.MkdirTemp("", "s3d-tox-")
	if err != nil {
		log.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	store, err := sqlite.OpenDatabase(filepath.Join(dir, "s3d.sqlite"), logger)
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer store.Close()

	opts := []sia.Option{sia.WithLogger(logger)}
	for _, pair := range toxKeyPairs {
		opts = append(opts, sia.WithKeyPair(pair.AccessKey, pair.SecretKey))
	}

	backend, err := sia.New(context.Background(), testutil.NewMemorySDK(), store, dir, opts...)
	if err != nil {
		log.Fatalf("failed to create Sia backend: %v", err)
	}

	s3 := s3.New(backend,
		s3.WithHostBucketBases([]string{"localhost"}),
		s3.WithLogger(logger))
	if err := http.ListenAndServe("localhost:8000", s3); err != nil {
		log.Printf("failed to start server: %v", err)
	}
}
