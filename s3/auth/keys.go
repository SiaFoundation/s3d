package auth

import (
	"encoding/base32"
	"encoding/base64"
	"fmt"
	"strings"
	"unicode"

	"lukechampine.com/frand"
)

const (
	minAccessKeyIDLength = 16
	maxAccessKeyIDLength = 128
	minSecretKeyLength   = 32
	maxSecretKeyLength   = 128
)

// GenerateAccessKey returns a random access key pair.
func GenerateAccessKey() (accessKeyID, secretKey string) {
	// 12 random bytes encode to exactly 20 base32 characters and 30 random
	// bytes to exactly 40 base64 characters
	accessKeyID = base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(frand.Bytes(12))
	secretKey = base64.StdEncoding.EncodeToString(frand.Bytes(30))
	return
}

// ValidateAccessKey checks that an access key ID and secret key have acceptable
// lengths and that the ID can be carried in a SigV4 credential.
func ValidateAccessKey(accessKeyID, secretKey string) error {
	if len(accessKeyID) < minAccessKeyIDLength || len(accessKeyID) > maxAccessKeyIDLength {
		return fmt.Errorf("access key must be between %d and %d characters", minAccessKeyIDLength, maxAccessKeyIDLength)
	} else if strings.ContainsAny(accessKeyID, "/,") || strings.ContainsFunc(accessKeyID, unicode.IsSpace) {
		return fmt.Errorf("access key must not contain slashes, commas or whitespace")
	} else if len(secretKey) < minSecretKeyLength || len(secretKey) > maxSecretKeyLength {
		return fmt.Errorf("secret key must be between %d and %d characters", minSecretKeyLength, maxSecretKeyLength)
	}
	return nil
}
