package main

import (
	"log"
	"net/http"

	"github.com/SiaFoundation/s3d/s3"
	"github.com/SiaFoundation/s3d/testutils"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("failed to create logger: %v", err)
	}
	s3 := s3.New(testutils.NewMemoryBackend(),
		s3.WithHostBucketBases([]string{"localhost"}),
		s3.WithLogger(logger))
	http.ListenAndServe("localhost:7777", s3)
}
