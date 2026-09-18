package auth

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"hash"
	"hash/crc32"

	"github.com/minio/crc64nvme"
)

// checksumAlgorithms maps each X-Amz-Checksum-* suffix the backend can compute,
// canonically formatted, to its hash constructor.
var checksumAlgorithms = map[string]func() hash.Hash{
	"Crc32":     func() hash.Hash { return crc32.NewIEEE() },
	"Crc32c":    func() hash.Hash { return crc32.New(crc32.MakeTable(crc32.Castagnoli)) },
	"Crc64nvme": func() hash.Hash { return crc64nvme.New() },
	"Md5":       md5.New,
	"Sha1":      sha1.New,
	"Sha256":    sha256.New,
	"Sha512":    sha512.New,
}

// NewChecksumHash returns a hash for an X-Amz-Checksum-* algorithm, named by
// its canonically formatted header suffix such as "Crc32", or nil when the
// backend cannot compute it.
func NewChecksumHash(algorithm string) hash.Hash {
	if newHash, ok := checksumAlgorithms[algorithm]; ok {
		return newHash()
	}
	return nil
}
