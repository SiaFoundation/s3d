package s3

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"go.uber.org/zap"
)

// the headers a GetObjectAttributes request carries
const (
	// HeaderXAMZObjectAttributes names the attributes to report.
	HeaderXAMZObjectAttributes = "X-Amz-Object-Attributes"

	// HeaderXAMZMaxParts caps the number of parts to report.
	HeaderXAMZMaxParts = "X-Amz-Max-Parts"

	// HeaderXAMZPartNumberMarker is the part number to resume after.
	HeaderXAMZPartNumberMarker = "X-Amz-Part-Number-Marker"
)

// the attributes a GetObjectAttributes request may ask for
const (
	attributeETag         = "ETag"
	attributeChecksum     = "Checksum"
	attributeObjectParts  = "ObjectParts"
	attributeStorageClass = "StorageClass"
	attributeObjectSize   = "ObjectSize"
)

var objectAttributeNames = []string{
	attributeETag,
	attributeChecksum,
	attributeObjectParts,
	attributeStorageClass,
	attributeObjectSize,
}

// ObjectAttributesPage bounds the parts a GetObjectAttributes request reports.
type ObjectAttributesPage struct {
	// PartNumberMarker excludes every part up to and including it.
	PartNumberMarker int

	// MaxParts caps how many parts follow the marker.
	MaxParts int
}

// ObjectPart is one part of a multipart object.
type ObjectPart struct {
	PartNumber int
	Size       int64

	// Checksum is the part's additional digest, nil when the part carries none.
	Checksum []byte
}

// ObjectAttributes are the attributes of a stored object.
type ObjectAttributes struct {
	ContentMD5   [16]byte
	PartsCount   int32
	Size         int64
	LastModified time.Time

	// VersionID is the version ID of the object, or "" for the null version.
	VersionID string

	// Versioned reports whether the object's bucket has versioning configured,
	// either Enabled or Suspended. When false, no version header is emitted.
	Versioned bool

	// IsDeleteMarker is true when the requested version is a delete marker.
	IsDeleteMarker bool

	// Checksum is the additional checksum stored with the object, nil when it
	// carries none.
	Checksum *ObjectChecksum

	// Parts is the page of parts the request asked for, empty for an object
	// that was not uploaded in parts.
	Parts []ObjectPart

	// IsTruncated reports whether parts beyond the page remain.
	IsTruncated bool
}

// routeObjectAttributes routes requests that contain '?attributes' in the
// query string.
func (s *s3) routeObjectAttributes(r *http.Request, object string, version VersionRequest) (operation, error) {
	if r.Method != http.MethodGet {
		return operation{}, s3errs.ErrMethodNotAllowed
	}
	return operation{action: readAction(version), serve: func(w http.ResponseWriter, r *http.Request, access *bucketAccess) error {
		return s.getObjectAttributes(w, r, access, object, version)
	}}, nil
}

// getObjectAttributes handles GET Object attributes requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html
func (s *s3) getObjectAttributes(w http.ResponseWriter, r *http.Request, access *bucketAccess, object string, version VersionRequest) error {
	s.logger.Debug("getting object attributes",
		zap.String("bucket", access.bucket),
		zap.String("object", object),
		zap.String("version", version.LogValue()))

	requested, err := parseObjectAttributes(r.Header)
	if err != nil {
		return err
	}
	page, err := objectAttributesPage(r.Header)
	if err != nil {
		return err
	}

	attrs, err := s.backend.GetObjectAttributes(r.Context(), access.bucketID, object, version, page)
	if err != nil {
		return access.hideMissing(err, object)
	}

	// a delete marker and a versioned read report the headers a HEAD reports
	obj := &Object{
		LastModified:   attrs.LastModified,
		VersionID:      attrs.VersionID,
		Versioned:      attrs.Versioned,
		IsDeleteMarker: attrs.IsDeleteMarker,
	}
	if attrs.IsDeleteMarker {
		// a current delete marker proves the key once existed, so it is
		// hidden like a missing key
		if !version.Specified {
			if err := access.assertListAllowed(object); err != nil {
				return err
			}
		}
		return deleteMarkerError(w, obj, version)
	}
	setVersionHeaders(w, obj)
	w.Header().Set("Last-Modified", attrs.LastModified.UTC().Format(http.TimeFormat))

	resp := GetObjectAttributesResponse{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"}
	if requested[attributeETag] {
		// the element carries the ETag without the quotes a header spells it with
		resp.ETag = strings.Trim(FormatETag(attrs.ContentMD5[:], int(attrs.PartsCount)), `"`)
	}
	if requested[attributeObjectSize] {
		resp.ObjectSize = aws.Int64(attrs.Size)
	}
	if requested[attributeStorageClass] {
		resp.StorageClass = "STANDARD"
	}
	if requested[attributeChecksum] && attrs.Checksum != nil {
		resp.Checksum = &ChecksumResponse{
			ChecksumElements: NewChecksumElements(attrs.Checksum.Algorithm, attrs.Checksum.Value),
			ChecksumType:     attrs.Checksum.Type,
		}
	}
	if requested[attributeObjectParts] && attrs.PartsCount > 0 {
		resp.ObjectParts = objectPartsResponse(attrs, page)
	}

	return writeXMLResponse(w, http.StatusOK, resp)
}

// parseObjectAttributes reads the attributes a request asks for. The header
// carries them comma separated and may repeat, which is how the AWS SDKs send
// more than one. A request naming no attribute or an unknown one returns
// [s3errs.ErrInvalidArgument].
func parseObjectAttributes(header http.Header) (map[string]bool, error) {
	values := header.Values(HeaderXAMZObjectAttributes)
	if len(values) == 0 {
		return nil, fmt.Errorf("missing %s header: %w", HeaderXAMZObjectAttributes, s3errs.ErrInvalidArgument)
	}

	requested := make(map[string]bool, len(objectAttributeNames))
	for _, value := range values {
		for _, name := range strings.Split(value, ",") {
			name = strings.TrimSpace(name)
			if !slices.Contains(objectAttributeNames, name) {
				return nil, fmt.Errorf("unknown object attribute %q: %w", name, s3errs.ErrInvalidArgument)
			}
			requested[name] = true
		}
	}
	return requested, nil
}

// objectAttributesPage reads the page of parts a request asks for.
func objectAttributesPage(header http.Header) (ObjectAttributesPage, error) {
	maxParts, err := parseClampedInt(header.Get(HeaderXAMZMaxParts), DefaultMaxUploadListParts, 1, MaxUploadListParts)
	if err != nil {
		return ObjectAttributesPage{}, err
	}

	page := ObjectAttributesPage{MaxParts: int(maxParts)}
	if raw := header.Get(HeaderXAMZPartNumberMarker); raw != "" {
		marker, err := strconv.Atoi(raw)
		if err != nil {
			return ObjectAttributesPage{}, fmt.Errorf("invalid part number marker %q: %w", raw, s3errs.ErrInvalidArgument)
		}
		page.PartNumberMarker = marker
	}
	return page, nil
}

// objectPartsResponse builds the parts element of a GetObjectAttributes
// response. The next marker is the last part reported and is only set when
// parts remain.
func objectPartsResponse(attrs *ObjectAttributes, page ObjectAttributesPage) *ObjectPartsResponse {
	parts := &ObjectPartsResponse{
		IsTruncated:      attrs.IsTruncated,
		MaxParts:         page.MaxParts,
		PartNumberMarker: page.PartNumberMarker,
		PartsCount:       attrs.PartsCount,
	}

	for _, part := range attrs.Parts {
		reported := ObjectPartResponse{
			PartNumber: part.PartNumber,
			Size:       part.Size,
		}
		if attrs.Checksum != nil && len(part.Checksum) > 0 {
			reported.ChecksumElements = NewChecksumElements(attrs.Checksum.Algorithm, base64.StdEncoding.EncodeToString(part.Checksum))
		}
		parts.Parts = append(parts.Parts, reported)
	}

	if attrs.IsTruncated && len(parts.Parts) > 0 {
		parts.NextPartNumberMarker = parts.Parts[len(parts.Parts)-1].PartNumber
	}
	return parts
}
