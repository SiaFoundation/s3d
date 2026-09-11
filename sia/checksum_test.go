package sia

import (
	"encoding/base64"
	"testing"
)

func TestChecksumHashers(t *testing.T) {
	// pin every algorithm against a value computed elsewhere, because a hasher
	// on the wrong polynomial would reject correct uploads rather than fail
	// visibly
	const body = "hello growth verification\n"

	tests := map[string]string{
		// produced by the AWS CLI itself against a live s3d, so these check the
		// polynomial against AWS and not against another copy of the same
		// assumption. Neither is in python's stdlib
		"Crc64nvme": "7TQAehZyeq8=",
		"Crc32c":    "RJH9UA==",

		// the rest from python's zlib and hashlib
		"Crc32":  "/7DfVA==",
		"Sha1":   "1y2Y/xz0bv/90p+udC6eiQv0T9o=",
		"Sha256": "eTzjBj+IGti9vVCUPoKKS3FRE3Icm86NkTYwAemfvK4=",
	}

	for algorithm, want := range tests {
		t.Run(algorithm, func(t *testing.T) {
			h := newChecksumHasher(algorithm)
			if h == nil {
				t.Fatal("no hasher")
			} else if _, err := h.Write([]byte(body)); err != nil {
				t.Fatal(err)
			}

			if got := base64.StdEncoding.EncodeToString(h.Sum(nil)); got != want {
				t.Errorf("expected %s, got %s", want, got)
			}
		})
	}

	if newChecksumHasher("MadeUp") != nil {
		t.Error("unexpected hasher for an unknown algorithm")
	}
}
