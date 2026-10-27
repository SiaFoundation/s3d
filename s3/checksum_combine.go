package s3

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

// the reflected polynomial and bit width of each CRC algorithm
var crcPolynomials = map[ChecksumAlgorithm]struct {
	poly  uint64
	width uint
}{
	ChecksumCRC32:     {0xEDB88320, 32},
	ChecksumCRC32C:    {0x82F63B78, 32},
	ChecksumCRC64NVME: {0x9A6C9329AC4BC9B5, 64},
}

// PartChecksum is one part's raw digest and the number of bytes it covers.
type PartChecksum struct {
	Sum    []byte
	Length int64
}

// CompositeChecksum returns the base64 digest of the concatenated part digests
// followed by a part count suffix such as "-3", the value AWS reports for a
// multipart object whose checksum type is COMPOSITE. It returns an empty string
// for an algorithm the backend cannot hash and for a part that does not carry a
// digest of the algorithm's width.
func CompositeChecksum(a ChecksumAlgorithm, parts []PartChecksum) string {
	h := a.NewHash()
	if h == nil {
		return ""
	}
	for _, part := range parts {
		if len(part.Sum) != h.Size() {
			return ""
		}
		h.Write(part.Sum)
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil)) + "-" + strconv.Itoa(len(parts))
}

// CombineChecksums returns the big endian digest the algorithm would produce
// over the parts' bytes concatenated in order. The algorithm must support
// ChecksumTypeFullObject, there must be at least one part, and every part must
// carry a digest of the algorithm's width, otherwise the error wraps
// s3errs.ErrInvalidRequest.
func CombineChecksums(a ChecksumAlgorithm, parts []PartChecksum) ([]byte, error) {
	crc, ok := crcPolynomials[a]
	if !ok || !a.SupportsType(ChecksumTypeFullObject) {
		return nil, fmt.Errorf("checksum algorithm %q cannot cover a full object: %w", a, s3errs.ErrInvalidRequest)
	} else if len(parts) == 0 {
		return nil, fmt.Errorf("cannot combine the checksums of zero parts: %w", s3errs.ErrInvalidRequest)
	}

	size := int(crc.width) / 8

	var combined uint64
	for i, part := range parts {
		if len(part.Sum) != size {
			return nil, fmt.Errorf("part %d carries a %d byte %s checksum, expected %d bytes: %w", i+1, len(part.Sum), a, size, s3errs.ErrInvalidRequest)
		}

		var sum uint64
		if size == 4 {
			sum = uint64(binary.BigEndian.Uint32(part.Sum))
		} else {
			sum = binary.BigEndian.Uint64(part.Sum)
		}
		combined = crcCombine(combined, sum, part.Length, crc.poly, crc.width)
	}

	if size == 4 {
		return binary.BigEndian.AppendUint32(nil, uint32(combined)), nil
	}
	return binary.BigEndian.AppendUint64(nil, combined), nil
}

func crcCombine(crc1, crc2 uint64, len2 int64, poly uint64, width uint) uint64 {
	odd := make([]uint64, width)
	even := make([]uint64, width)

	odd[0] = poly
	for n := uint(1); n < width; n++ {
		odd[n] = 1 << (n - 1)
	}
	gf2MatrixSquare(even, odd)
	gf2MatrixSquare(odd, even)

	for len2 > 0 {
		gf2MatrixSquare(even, odd)
		if len2&1 != 0 {
			crc1 = gf2MatrixTimes(even, crc1)
		}

		len2 >>= 1
		if len2 == 0 {
			break
		}

		gf2MatrixSquare(odd, even)
		if len2&1 != 0 {
			crc1 = gf2MatrixTimes(odd, crc1)
		}
		len2 >>= 1
	}

	return crc1 ^ crc2
}

// the GF(2) matrix routines of zlib's crc32_combine
func gf2MatrixTimes(mat []uint64, vec uint64) uint64 {
	var sum uint64
	for i := 0; vec != 0; i++ {
		if vec&1 != 0 {
			sum ^= mat[i]
		}
		vec >>= 1
	}
	return sum
}

func gf2MatrixSquare(square, mat []uint64) {
	for n := range mat {
		square[n] = gf2MatrixTimes(mat, mat[n])
	}
}
