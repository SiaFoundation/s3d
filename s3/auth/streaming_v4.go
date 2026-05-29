package auth

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/SiaFoundation/s3d/s3/s3errs"
)

// hex-encoded SHA-256 of the empty input, used as a fixed placeholder in
// every chunk's string-to-sign.
const emptySha256Hex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

type chunkSigVerifier struct {
	mac        hash.Hash
	signingKey []byte
	scope      string
	timestamp  string
	prevSig    string
}

func newChunkSigVerifier(r *v4SignResult) *chunkSigVerifier {
	return &chunkSigVerifier{
		mac:        hmac.New(sha256.New, r.SigningKey),
		signingKey: r.SigningKey,
		scope:      r.Scope,
		timestamp:  r.Timestamp,
		prevSig:    r.SeedSig,
	}
}

func (v *chunkSigVerifier) computeSig(stringToSign string, out []byte) {
	v.mac.Reset()
	v.mac.Write([]byte(stringToSign))
	var sumBuf [32]byte
	hex.Encode(out, v.mac.Sum(sumBuf[:0]))
}

func (v *chunkSigVerifier) verifyChunk(declaredSig string, dataHash [32]byte) error {
	var dataHex [64]byte
	hex.Encode(dataHex[:], dataHash[:])

	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256-PAYLOAD",
		v.timestamp,
		v.scope,
		v.prevSig,
		emptySha256Hex,
		string(dataHex[:]),
	}, "\n")

	var computed [64]byte
	v.computeSig(stringToSign, computed[:])

	if subtle.ConstantTimeCompare(computed[:], []byte(declaredSig)) != 1 {
		return s3errs.ErrInvalidSignature
	}
	v.prevSig = string(computed[:])
	return nil
}

func (v *chunkSigVerifier) verifyTrailer(canonicalTrailer, declaredSig string) error {
	trailerHash := sha256.Sum256([]byte(canonicalTrailer))
	var trailerHex [64]byte
	hex.Encode(trailerHex[:], trailerHash[:])

	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256-TRAILER",
		v.timestamp,
		v.scope,
		v.prevSig,
		string(trailerHex[:]),
	}, "\n")

	var computed [64]byte
	v.computeSig(stringToSign, computed[:])

	if subtle.ConstantTimeCompare(computed[:], []byte(declaredSig)) != 1 {
		return s3errs.ErrInvalidSignature
	}
	return nil
}

func handleAuthV4Streaming(req *http.Request, result *v4SignResult) error {
	// parse x-amz-decoded-content-length
	sizeStr, ok := req.Header["X-Amz-Decoded-Content-Length"]
	if !ok || len(sizeStr) == 0 || sizeStr[0] == "" {
		return s3errs.ErrInvalidArgument
	}
	size, err := strconv.ParseInt(sizeStr[0], 10, 64)
	if err != nil || size < 0 {
		return s3errs.ErrInvalidArgument
	}

	sha256Hdr := req.Header.Get(HeaderXAMZContentSHA256)

	// x-amz-trailer is only sent with the *-TRAILER variants
	var expectedHeaders map[string]struct{}
	if sha256Hdr == ContentStreamingUnsignedPayloadTrailer ||
		sha256Hdr == ContentStreamingAWS4HMACSHA256PayloadTrailer {
		xAmzTrailer := req.Header.Get(HeaderXAMZTrailer)
		if xAmzTrailer == "" {
			return s3errs.ErrInvalidArgument
		}
		expectedHeaders = make(map[string]struct{})
		for h := range strings.SplitSeq(xAmzTrailer, ",") {
			expectedHeaders[http.CanonicalHeaderKey(h)] = struct{}{}
		}
	}

	var verifier *chunkSigVerifier
	if sha256Hdr == ContentStreamingAWS4HMACSHA256Payload ||
		sha256Hdr == ContentStreamingAWS4HMACSHA256PayloadTrailer {
		verifier = newChunkSigVerifier(result)
	}

	switch sha256Hdr {
	case ContentStreamingUnsignedPayloadTrailer,
		ContentStreamingAWS4HMACSHA256Payload,
		ContentStreamingAWS4HMACSHA256PayloadTrailer:
		req.Body = newChunkedPayloadTrailerReader(req.Body, expectedHeaders, verifier)
	default:
		// should not reach here
		return s3errs.ErrInternalError
	}

	req.ContentLength = size
	return nil
}

// chunkedPayloadTrailerReader reads an aws-chunked body and exposes only
// the payload bytes via io.Reader. When verifier is non-nil it also
// enforces per-chunk and trailer signatures.
type chunkedPayloadTrailerReader struct {
	br              *bufio.Reader
	r               io.Closer // io.Closer to avoid reading from it rather than br
	chunkRemain     int64
	done            bool
	expectedHeaders map[string]struct{}

	crc32Hasher  hash.Hash32
	crc32CHasher hash.Hash32
	sha1Hasher   hash.Hash
	sha256Hasher hash.Hash

	verifier   *chunkSigVerifier
	chunkSig   string
	chunkHash  hash.Hash
	trailerSig string
}

// newChunkedPayloadTrailerReader wraps r. r should be the raw HTTP message body
// with Content-Encoding: aws-chunked.
func newChunkedPayloadTrailerReader(r io.ReadCloser, expectedHeaders map[string]struct{}, verifier *chunkSigVerifier) *chunkedPayloadTrailerReader {
	var crc32Hasher hash.Hash32
	if _, exists := expectedHeaders[xAmzChecksumCrc32]; exists {
		crc32Hasher = crc32.New(crc32.MakeTable(crc32.IEEE))
	}
	var crc32CHasher hash.Hash32
	if _, exists := expectedHeaders[xAmzChecksumCrc32C]; exists {
		crc32CHasher = crc32.New(crc32.MakeTable(crc32.Castagnoli))
	}
	var sha1Hasher hash.Hash
	if _, exists := expectedHeaders[xAmzChecksumSha1]; exists {
		sha1Hasher = sha1.New()
	}
	var sha256Hasher hash.Hash
	if _, exists := expectedHeaders[xAmzChecksumSha256]; exists {
		sha256Hasher = sha256.New()
	}

	rdr := &chunkedPayloadTrailerReader{
		r:               r,
		br:              bufio.NewReader(r),
		chunkRemain:     0,
		expectedHeaders: expectedHeaders,

		crc32Hasher:  crc32Hasher,
		crc32CHasher: crc32CHasher,
		sha1Hasher:   sha1Hasher,
		sha256Hasher: sha256Hasher,

		verifier: verifier,
	}
	if verifier != nil {
		rdr.chunkHash = sha256.New()
	}
	return rdr
}

// Close clears any derived signing key and closes the underlying reader.
func (r *chunkedPayloadTrailerReader) Close() error {
	if r.verifier != nil {
		clear(r.verifier.signingKey)
	}
	return r.r.Close()
}

// Read streams only the payload bytes. Once the payload is exhausted,
// Read returns io.EOF and trailers will have been parsed and verified.
func (r *chunkedPayloadTrailerReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}

	// ensure we have an active chunk to read from.
	for r.chunkRemain == 0 {
		line, err := readCRLFLine(r.br)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return 0, io.ErrUnexpectedEOF
			}
			return 0, err
		}
		size, sig, err := parseChunkHeader(line)
		if err != nil {
			return 0, err
		}
		if size == 0 {
			if r.verifier != nil {
				if err := r.verifier.verifyChunk(sig, sha256.Sum256(nil)); err != nil {
					return 0, err
				}
			}
			parsed, err := r.assertTrailerHeaders(r.expectedHeaders)
			if err != nil {
				return 0, err
			}
			if r.verifier != nil && r.expectedHeaders != nil {
				if err := r.verifyTrailerSignature(parsed); err != nil {
					return 0, err
				}
			}
			r.done = true
			return 0, io.EOF
		}
		r.chunkRemain = size
		r.chunkSig = sig
		if r.chunkHash != nil {
			r.chunkHash.Reset()
		}
	}

	// read up to min(len(p), chunkRemain)
	nwant := int64(len(p))
	if nwant == 0 {
		return 0, nil
	}
	if nwant > r.chunkRemain {
		nwant = r.chunkRemain
	}

	n, err := io.ReadFull(r.br, p[:nwant])
	r.chunkRemain -= int64(n)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return n, io.ErrUnexpectedEOF
		}
		return n, err
	}

	// update hashers with read data
	r.updateHashers(p[:n])
	if r.chunkHash != nil {
		r.chunkHash.Write(p[:n])
	}

	// if we just finished this chunk, expect a trailing CRLF.
	if r.chunkRemain == 0 {
		if err := expectCRLF(r.br); err != nil {
			if errors.Is(err, io.EOF) {
				return n, io.ErrUnexpectedEOF
			}
			return n, err
		}
		if r.verifier != nil {
			var sumBuf [32]byte
			var sum [32]byte
			copy(sum[:], r.chunkHash.Sum(sumBuf[:0]))
			if err := r.verifier.verifyChunk(r.chunkSig, sum); err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

// assertTrailerHeaders reads and asserts trailer headers from br match
// expectedHeaders. x-amz-trailer-signature is captured into r.trailerSig
// when the request is a signed *-TRAILER variant.
func (r *chunkedPayloadTrailerReader) assertTrailerHeaders(expectedHeaders map[string]struct{}) (http.Header, error) {
	parsedHeaders := make(http.Header)

	for {
		// read a line from the buffer
		line, err := readCRLFLine(r.br)
		if err != nil {
			return nil, s3errs.ErrInvalidArgument
		}

		// an empty line indicates the end of the headers
		if line == "" {
			break
		}

		// split the line into key and value
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			return nil, s3errs.ErrInvalidArgument
		}

		key := http.CanonicalHeaderKey(strings.TrimSpace(parts[0]))
		value := strings.TrimSpace(parts[1])

		// capture the trailer signature when this variant is expected to
		// carry one. on other variants it falls through and gets rejected
		// as an unknown trailer.
		if key == HeaderXAMZTrailerSignature && r.verifier != nil && r.expectedHeaders != nil {
			r.trailerSig = value
			continue
		}

		// check if the header is expected
		if _, ok := expectedHeaders[key]; !ok {
			return nil, s3errs.ErrInvalidArgument
		}

		// reject duplicates rather than guess at canonicalization for
		// multi-value trailers
		if _, dup := parsedHeaders[key]; dup {
			return nil, s3errs.ErrInvalidArgument
		}

		// add the header to the parsed headers
		parsedHeaders.Add(key, value)
	}

	// ensure all expected headers are present
	for expected := range expectedHeaders {
		h, ok := parsedHeaders[expected]
		if !ok {
			return nil, fmt.Errorf("missing expected trailer header: %q", expected)
		}

		// verify checksum headers
		if expected == xAmzChecksumCrc32 {
			if chksum := base64.StdEncoding.EncodeToString(r.crc32Hasher.Sum(nil)); h[0] != chksum {
				return nil, s3errs.ErrBadDigest
			}
		}
		if expected == xAmzChecksumCrc32C {
			if chksum := base64.StdEncoding.EncodeToString(r.crc32CHasher.Sum(nil)); h[0] != chksum {
				return nil, s3errs.ErrBadDigest
			}
		}
		if expected == xAmzChecksumSha1 {
			if chksum := base64.StdEncoding.EncodeToString(r.sha1Hasher.Sum(nil)); h[0] != chksum {
				return nil, s3errs.ErrBadDigest
			}
		}
		if expected == xAmzChecksumSha256 {
			if chksum := base64.StdEncoding.EncodeToString(r.sha256Hasher.Sum(nil)); h[0] != chksum {
				return nil, s3errs.ErrBadDigest
			}
		}
	}
	return parsedHeaders, nil
}

func (r *chunkedPayloadTrailerReader) verifyTrailerSignature(parsedHeaders http.Header) error {
	if r.trailerSig == "" {
		return s3errs.ErrInvalidSignature
	}

	keys := make([]string, 0, len(parsedHeaders))
	for k := range parsedHeaders {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf strings.Builder
	for _, k := range keys {
		for _, v := range parsedHeaders[k] {
			buf.WriteString(strings.ToLower(k))
			buf.WriteByte(':')
			buf.WriteString(signV4TrimAll(v))
			buf.WriteByte('\n')
		}
	}
	return r.verifier.verifyTrailer(buf.String(), r.trailerSig)
}

// updateHashers updates all active trailer-checksum hashers with data so
// they can be compared against the declared trailer values later.
func (r *chunkedPayloadTrailerReader) updateHashers(data []byte) {
	if r.crc32Hasher != nil {
		_, _ = r.crc32Hasher.Write(data)
	}
	if r.crc32CHasher != nil {
		_, _ = r.crc32CHasher.Write(data)
	}
	if r.sha1Hasher != nil {
		_, _ = r.sha1Hasher.Write(data)
	}
	if r.sha256Hasher != nil {
		_, _ = r.sha256Hasher.Write(data)
	}
}

func expectCRLF(br *bufio.Reader) error {
	b, err := br.Peek(2)
	if err != nil {
		return err
	}
	if len(b) < 2 || b[0] != '\r' || b[1] != '\n' {
		return fmt.Errorf("malformed chunk: missing CRLF after data")
	}
	_, _ = br.Discard(2)
	return nil
}

// parseChunkHeader parses a chunk-header line and returns the size plus
// the chunk-signature extension value if present.
func parseChunkHeader(line string) (int64, string, error) {
	sz, exts, _ := strings.Cut(line, ";")
	var sig string
	if exts != "" {
		const prefix = "chunk-signature="
		for ext := range strings.SplitSeq(exts, ";") {
			if strings.HasPrefix(ext, prefix) {
				sig = ext[len(prefix):]
				break
			}
		}
	}
	sz = strings.TrimSpace(sz)
	if sz == "" {
		return 0, "", fmt.Errorf("empty chunk size")
	}
	n, err := strconv.ParseInt(sz, 16, 64)
	if err != nil || n < 0 {
		return 0, "", fmt.Errorf("invalid chunk size %q: %w", sz, err)
	}
	return n, sig, nil
}

func readCRLFLine(br *bufio.Reader) (string, error) {
	// read until '\n' and ensure it ends with "\r\n".
	b, err := br.ReadBytes('\n')
	if err != nil {
		return "", err
	}
	if len(b) < 2 || b[len(b)-2] != '\r' {
		return "", fmt.Errorf("malformed chunk header: missing CRLF")
	}
	return string(b[:len(b)-2]), nil // strip CRLF
}
