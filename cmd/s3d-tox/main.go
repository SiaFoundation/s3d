package main

import (
	"log"
	"net/http"

	"github.com/SiaFoundation/s3d/internal/testutil"
	"github.com/SiaFoundation/s3d/s3"
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

	var opts []testutil.MemoryBackendOption
	for _, pair := range toxKeyPairs {
		opts = append(opts, testutil.WithKeyPair(pair.AccessKey, pair.SecretKey))
	}
	backend := testutil.NewMemoryBackend(opts...)

	s3 := s3.New(backend,
		s3.WithHostBucketBases([]string{"localhost"}),
		s3.WithLogger(logger))
	if err := http.ListenAndServe("localhost:8000", s3); err != nil {
		log.Printf("failed to start server: %v", err)
	}
}
