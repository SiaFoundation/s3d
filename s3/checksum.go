package s3

import (
	"fmt"
	"hash"
	"net/textproto"
	"slices"
	"strings"

	"github.com/SiaFoundation/s3d/s3/auth"
	"github.com/SiaFoundation/s3d/s3/s3errs"
)

// ChecksumAlgorithm is one of the additional checksum algorithms AWS accepts
// alongside Content-MD5.
type ChecksumAlgorithm string

// the additional checksum algorithms a client may ask for
const (
	ChecksumCRC32     ChecksumAlgorithm = "CRC32"
	ChecksumCRC32C    ChecksumAlgorithm = "CRC32C"
	ChecksumCRC64NVME ChecksumAlgorithm = "CRC64NVME"
	ChecksumSHA1      ChecksumAlgorithm = "SHA1"
	ChecksumSHA256    ChecksumAlgorithm = "SHA256"
)

// ChecksumType is how a multipart upload's checksum covers the object.
type ChecksumType string

const (
	// ChecksumTypeComposite is a checksum over the part checksums, reported
	// with a part count suffix such as "-3".
	ChecksumTypeComposite ChecksumType = "COMPOSITE"
	// ChecksumTypeFullObject is a checksum over the object's bytes, the same
	// value a single PUT of those bytes reports.
	ChecksumTypeFullObject ChecksumType = "FULL_OBJECT"
)

// the X-Amz-Checksum-* headers whose suffix names something other than an
// algorithm
const (
	HeaderXAMZChecksumAlgorithm = "X-Amz-Checksum-Algorithm"
	HeaderXAMZChecksumType      = "X-Amz-Checksum-Type"
	HeaderXAMZChecksumMode      = "X-Amz-Checksum-Mode"
)

// HeaderXAMZMpObjectSize is the size a client expects a completed multipart
// object to have.
const HeaderXAMZMpObjectSize = "X-Amz-Mp-Object-Size"

// reservedChecksumHeaders cannot carry a checksum value, so a scan over the
// X-Amz-Checksum-* family skips them.
var reservedChecksumHeaders = []string{HeaderXAMZChecksumAlgorithm, HeaderXAMZChecksumType, HeaderXAMZChecksumMode}

// checksumTypes lists the types each algorithm supports, the default first.
var checksumTypes = map[ChecksumAlgorithm][]ChecksumType{
	ChecksumCRC32:     {ChecksumTypeComposite, ChecksumTypeFullObject},
	ChecksumCRC32C:    {ChecksumTypeComposite, ChecksumTypeFullObject},
	ChecksumCRC64NVME: {ChecksumTypeFullObject},
	ChecksumSHA1:      {ChecksumTypeComposite},
	ChecksumSHA256:    {ChecksumTypeComposite},
}

// ChecksumAlgorithmFromSuffix returns the algorithm an X-Amz-Checksum-* header
// suffix such as "Crc32" names, ignoring case.
func ChecksumAlgorithmFromSuffix(suffix string) (ChecksumAlgorithm, bool) {
	algorithm := ChecksumAlgorithm(strings.ToUpper(suffix))
	if _, ok := checksumTypes[algorithm]; !ok {
		return "", false
	}
	return algorithm, true
}

// ParseChecksumAlgorithm parses an algorithm name, ignoring case. An empty name
// parses to the empty algorithm and an unknown one to s3errs.ErrInvalidRequest.
func ParseChecksumAlgorithm(s string) (ChecksumAlgorithm, error) {
	if s == "" {
		return "", nil
	}
	algorithm, ok := ChecksumAlgorithmFromSuffix(s)
	if !ok {
		return "", fmt.Errorf("unknown checksum algorithm %q: %w", s, s3errs.ErrInvalidRequest)
	}
	return algorithm, nil
}

// ParseChecksumType parses a checksum type, ignoring case. An empty value
// parses to the empty type and an unknown one to s3errs.ErrInvalidRequest.
func ParseChecksumType(s string) (ChecksumType, error) {
	switch checksumType := ChecksumType(strings.ToUpper(s)); checksumType {
	case "", ChecksumTypeComposite, ChecksumTypeFullObject:
		return checksumType, nil
	default:
		return "", fmt.Errorf("unknown checksum type %q: %w", s, s3errs.ErrInvalidRequest)
	}
}

// HeaderSuffix returns the algorithm's canonically formatted X-Amz-Checksum-*
// header suffix such as "Crc32".
func (a ChecksumAlgorithm) HeaderSuffix() string {
	return textproto.CanonicalMIMEHeaderKey(strings.ToLower(string(a)))
}

// Header returns the algorithm's X-Amz-Checksum-* header name.
func (a ChecksumAlgorithm) Header() string {
	return checksumPrefix + a.HeaderSuffix()
}

// NewHash returns a hash for the algorithm, or nil when the backend cannot
// compute it.
func (a ChecksumAlgorithm) NewHash() hash.Hash {
	return auth.NewChecksumHash(a.HeaderSuffix())
}

// SupportsType reports whether the algorithm can cover a multipart object in
// the given way.
func (a ChecksumAlgorithm) SupportsType(t ChecksumType) bool {
	return slices.Contains(checksumTypes[a], t)
}

// DefaultType returns the type a multipart upload takes when the client names
// the algorithm without naming a type, or "" for an unknown algorithm.
func (a ChecksumAlgorithm) DefaultType() ChecksumType {
	types := checksumTypes[a]
	if len(types) == 0 {
		return ""
	}
	return types[0]
}

// ObjectChecksum is the additional checksum stored on a completed object.
type ObjectChecksum struct {
	Algorithm ChecksumAlgorithm
	Type      ChecksumType

	// Value is the base64 checksum, carrying a part count suffix such as "-3"
	// when Type is COMPOSITE.
	Value string
}

// ObjectChecksumFromMeta reads the additional checksum stored with an object,
// returning nil when it carries none. The algorithm is whatever the stored
// header named, which need not be one of the constants.
func ObjectChecksumFromMeta(meta map[string]string) *ObjectChecksum {
	var found *ObjectChecksum
	var checksumType ChecksumType

	for name, value := range meta {
		name = textproto.CanonicalMIMEHeaderKey(name)
		suffix, ok := strings.CutPrefix(name, checksumPrefix)
		if !ok {
			continue
		}
		if slices.Contains(reservedChecksumHeaders, name) {
			if name == HeaderXAMZChecksumType {
				checksumType = ChecksumType(value)
			}
			continue
		}
		found = &ObjectChecksum{Algorithm: ChecksumAlgorithm(strings.ToUpper(suffix)), Value: value}
	}

	if found == nil {
		return nil
	}
	found.Type = checksumType
	return found
}

// StoreInMeta writes the checksum to an object's metadata.
func (c ObjectChecksum) StoreInMeta(meta map[string]string) {
	if c.Algorithm == "" {
		return
	}
	meta[c.Algorithm.Header()] = c.Value
	meta[HeaderXAMZChecksumType] = string(c.Type)
}
