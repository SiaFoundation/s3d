package main

import (
	"context"
	"log"
	"net/http"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/testutils"
	"go.uber.org/zap"
)

const (
	toxAccessKey = "0555b35654ad1656d804"
	toxSecretKey = "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("failed to create logger: %v", err)
	}
	backend := testutils.NewMemoryBackend()
	err = backend.AddAccessKey(context.Background(), toxAccessKey, toxSecretKey)
	if err != nil {
		log.Fatalf("failed to add access key: %v", err)
	}
	s3 := s3.New(backend,
		s3.WithHostBucketBases([]string{"localhost"}),
		s3.WithLogger(logger))
	http.ListenAndServe("localhost:8000", s3)
}
