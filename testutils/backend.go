package testutils

// MemoryBackend is an in-memory implementation of the s3 backend for testing.
type MemoryBackend struct {
}

// NewMemoryBackend creates a new MemoryBackend.
func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{}
}
