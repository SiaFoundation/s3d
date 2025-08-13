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
	s3 := s3.New(testutils.NewMemoryBackend(), logger)
	http.ListenAndServe("127.0.0.1:7777", s3)
}
