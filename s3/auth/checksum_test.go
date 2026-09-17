package auth

import (
	"encoding/base64"
	"testing"
)

func TestNewChecksumHash(t *testing.T) {
	// "123456789" is the check string the CRC catalogue publishes a value for,
	// so a hasher built on the wrong polynomial fails here rather than
	// silently rejecting correct uploads
	const body = "123456789"

	tests := map[string]string{
		"Crc32":     "y/Q5Jg==",     // 0xcbf43926, the ISO HDLC check value
		"Crc32c":    "4waSgw==",     // 0xe3069283, the Castagnoli check value
		"Crc64nvme": "rosUhgp5mIg=", // 0xae8b14860a799888, the NVME check value
		"Md5":       "JfnnlDI7RTiF9RgfG2JNCw==",
		"Sha1":      "98O8HYCOBHMq32eZZczDTKeuNEE=",
		"Sha256":    "FeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiU=",
		"Sha512":    "2eZ2LdHI6vbWGzxhkvxAjU1tXxF20MKRabwk5xw/J0rSf81YEbMT1oH35V7ALXPUmclUVba1u1A6z1dPuo/+hQ==",
	}

	for algorithm, want := range tests {
		t.Run(algorithm, func(t *testing.T) {
			h := NewChecksumHash(algorithm)
			if h == nil {
				t.Fatal("no hasher")
			} else if _, err := h.Write([]byte(body)); err != nil {
				t.Fatal(err)
			}

			if got := base64.StdEncoding.EncodeToString(h.Sum(nil)); got != want {
				t.Fatal("checksum mismatch", got)
			}
		})
	}

	if NewChecksumHash("MadeUp") != nil {
		t.Fatal("unexpected hasher")
	}
}
