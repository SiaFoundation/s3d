package s3

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/SiaFoundation/s3d/s3/s3errs"
	"go.uber.org/zap"
)

// ObjectTagging is the tag set of an object version, as returned by
// [Backend.GetObjectTagging].
type ObjectTagging struct {
	Tags TagSet
	// Version addresses the version explicitly, so a later call acts on the
	// same one.
	Version VersionRequest
	// VersionID is the version ID a response reports, or "" for the current
	// version of a bucket that never had versioning.
	VersionID string
}

// tagLimits are S3's limits on a tag set.
type tagLimits struct {
	count      int
	countUTF16 bool // count lengths in UTF-16 code units, not characters
}

var (
	objectTagLimits = tagLimits{count: 10, countUTF16: true}
	bucketTagLimits = tagLimits{count: 50}
)

// the maximum lengths of a tag key and value
const (
	maxTagKeyLength   = 128
	maxTagValueLength = 256
)

// validate checks S3's tag limits, character set, reserved prefix and unique
// keys.
func (tags TagSet) validate(limits tagLimits) error {
	if len(tags) > limits.count {
		return fmt.Errorf("at most %d tags are allowed: %w", limits.count, s3errs.ErrInvalidTag)
	}
	seen := make(map[string]bool)
	for _, tag := range tags {
		if seen[tag.Key] {
			return fmt.Errorf("duplicate tag key %q: %w", tag.Key, s3errs.ErrInvalidTag)
		}
		seen[tag.Key] = true
		if tag.Key == "" {
			return fmt.Errorf("a tag key must not be empty: %w", s3errs.ErrInvalidTag)
		} else if strings.HasPrefix(strings.ToLower(tag.Key), "aws:") {
			return fmt.Errorf("tag key %q uses the reserved aws: prefix: %w", tag.Key, s3errs.ErrInvalidTag)
		} else if err := validateTagString(tag.Key, maxTagKeyLength, limits); err != nil {
			return fmt.Errorf("tag key %q: %w", tag.Key, err)
		} else if err := validateTagString(tag.Value, maxTagValueLength, limits); err != nil {
			return fmt.Errorf("tag value %q: %w", tag.Value, err)
		}
	}
	return nil
}

// validateTagString checks the length and character set of a tag key or value.
func validateTagString(s string, maxLength int, limits tagLimits) error {
	if !utf8.ValidString(s) {
		return fmt.Errorf("invalid UTF-8: %w", s3errs.ErrInvalidTag)
	}
	length := utf8.RuneCountInString(s)
	if limits.countUTF16 {
		length = len(utf16.Encode([]rune(s)))
	}
	if length > maxLength {
		return fmt.Errorf("longer than %d characters: %w", maxLength, s3errs.ErrInvalidTag)
	}
	// S3 allows [\p{L}\p{Z}\p{N}_.:/=+\-@]; tabs and newlines are not \p{Z}
	for _, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsNumber(r) && !unicode.Is(unicode.Z, r) && !strings.ContainsRune("_.:/=+@-", r) {
			return fmt.Errorf("invalid character %q: %w", r, s3errs.ErrInvalidTag)
		}
	}
	return nil
}

// tagXMLNode decodes a Tagging document strictly. A tag set replaces the
// existing one, so decoding a document missing its TagSet leniently would clear
// the tags rather than fail with MalformedXML as S3 does.
type tagXMLNode struct {
	Attributes []xml.Attr `xml:",any,attr"`
	XMLName    xml.Name
	Text       string       `xml:",chardata"`
	Children   []tagXMLNode `xml:",any"`
}

// parseTaggingXML parses a Tagging document strictly and validates its tags
// against limits.
func parseTaggingXML(body []byte, limits tagLimits) (TagSet, error) {
	d := xml.NewDecoder(bytes.NewReader(body))
	var root tagXMLNode
	if err := d.Decode(&root); err != nil {
		return nil, s3errs.ErrMalformedXML
	}
	// only whitespace and comments may follow the document
	for {
		token, err := d.Token()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, s3errs.ErrMalformedXML
		}
		if data, ok := token.(xml.CharData); ok && strings.TrimSpace(string(data)) == "" {
			continue
		} else if _, ok := token.(xml.Comment); ok {
			continue
		}
		return nil, fmt.Errorf("content after the Tagging element: %w", s3errs.ErrMalformedXML)
	}

	if !validTagXMLNode(root, root.XMLName.Space) {
		return nil, fmt.Errorf("unexpected namespace or attribute: %w", s3errs.ErrMalformedXML)
	} else if root.XMLName.Local != "Tagging" || len(root.Children) != 1 || root.Children[0].XMLName.Local != "TagSet" || strings.TrimSpace(root.Text) != "" {
		return nil, fmt.Errorf("a Tagging element must contain exactly one TagSet: %w", s3errs.ErrMalformedXML)
	}
	set := root.Children[0]
	if strings.TrimSpace(set.Text) != "" {
		return nil, fmt.Errorf("unexpected text in TagSet: %w", s3errs.ErrMalformedXML)
	}

	tags := make(TagSet, 0, len(set.Children))
	for _, node := range set.Children {
		if node.XMLName.Local != "Tag" || len(node.Children) != 2 || strings.TrimSpace(node.Text) != "" {
			return nil, fmt.Errorf("a Tag must contain exactly one Key and one Value: %w", s3errs.ErrMalformedXML)
		}
		var tag Tag
		seen := make(map[string]bool)
		for _, child := range node.Children {
			if len(child.Children) != 0 || seen[child.XMLName.Local] {
				return nil, fmt.Errorf("a Tag must contain exactly one Key and one Value: %w", s3errs.ErrMalformedXML)
			}
			seen[child.XMLName.Local] = true
			switch child.XMLName.Local {
			case "Key":
				tag.Key = child.Text
			case "Value":
				tag.Value = child.Text
			default:
				return nil, fmt.Errorf("unknown Tag element %q: %w", child.XMLName.Local, s3errs.ErrMalformedXML)
			}
		}
		tags = append(tags, tag)
	}
	if err := tags.validate(limits); err != nil {
		return nil, err
	}
	return tags, nil
}

// validTagXMLNode reports whether node and its children are in namespace,
// which is empty or the S3 namespace, and carry no attributes other than
// namespace declarations.
func validTagXMLNode(node tagXMLNode, namespace string) bool {
	if namespace != "" && namespace != "http://s3.amazonaws.com/doc/2006-03-01/" || node.XMLName.Space != namespace {
		return false
	}
	for _, attr := range node.Attributes {
		if attr.Name.Local != "xmlns" && attr.Name.Space != "xmlns" {
			return false
		}
	}
	for _, child := range node.Children {
		if !validTagXMLNode(child, namespace) {
			return false
		}
	}
	return true
}

// readTagging reads a tag set from a request body, which S3 requires to carry
// Content-MD5 or a checksum.
func readTagging(r *http.Request, limits tagLimits) (TagSet, error) {
	body, err := readDigestedBody(r)
	if err != nil {
		return nil, err
	}
	return parseTaggingXML(body, limits)
}

// routeBucketTagging routes the ?tagging bucket subresource.
func (s *s3) routeBucketTagging(r *http.Request) (operation, error) {
	switch r.Method {
	case http.MethodPut:
		return operation{action: actionPutBucketTagging, serve: s.putBucketTagging}, nil
	case http.MethodGet:
		return operation{action: actionGetBucketTagging, serve: s.getBucketTagging}, nil
	case http.MethodDelete:
		// S3 has no separate delete action
		return operation{action: actionPutBucketTagging, serve: s.deleteBucketTagging}, nil
	default:
		return operation{}, s3errs.ErrMethodNotAllowed
	}
}

// putBucketTagging handles PUT Bucket tagging requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketTagging.html
func (s *s3) putBucketTagging(w http.ResponseWriter, r *http.Request, access *bucketAccess) error {
	s.logger.Debug("putting bucket tagging", zap.String("bucket", access.bucket))

	tags, err := readTagging(r, bucketTagLimits)
	if err != nil {
		return err
	} else if err := s.backend.PutBucketTagging(r.Context(), access.bucketID, tags); err != nil {
		return err
	}
	// S3 answers with 204, though its reference documents 200
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// getBucketTagging handles GET Bucket tagging requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html
func (s *s3) getBucketTagging(w http.ResponseWriter, r *http.Request, access *bucketAccess) error {
	s.logger.Debug("getting bucket tagging", zap.String("bucket", access.bucket))

	tags, err := s.backend.GetBucketTagging(r.Context(), access.bucketID)
	if err != nil {
		return err
	}
	return writeXMLResponse(w, http.StatusOK, Tagging{
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		TagSet: tags,
	})
}

// deleteBucketTagging handles DELETE Bucket tagging requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketTagging.html
func (s *s3) deleteBucketTagging(w http.ResponseWriter, r *http.Request, access *bucketAccess) error {
	s.logger.Debug("deleting bucket tagging", zap.String("bucket", access.bucket))

	if err := s.backend.DeleteBucketTagging(r.Context(), access.bucketID); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// routeObjectTagging routes the ?tagging object subresource.
func (s *s3) routeObjectTagging(r *http.Request, object string, version VersionRequest) (operation, error) {
	var action string
	var handler func(http.ResponseWriter, *http.Request, *bucketAccess, string, VersionRequest) error
	switch r.Method {
	case http.MethodPut:
		action, handler = versionedAction(version, actionPutObjectTagging, actionPutObjectVersionTagging), s.putObjectTagging
	case http.MethodGet:
		action, handler = versionedAction(version, actionGetObjectTagging, actionGetObjectVersionTagging), s.getObjectTagging
	case http.MethodDelete:
		action, handler = versionedAction(version, actionDeleteObjectTagging, actionDeleteObjectVersionTagging), s.deleteObjectTagging
	default:
		return operation{}, s3errs.ErrMethodNotAllowed
	}
	return operation{action: action, serve: func(w http.ResponseWriter, r *http.Request, access *bucketAccess) error {
		return handler(w, r, access, object, version)
	}}, nil
}

// putObjectTagging handles PUT Object tagging requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html
func (s *s3) putObjectTagging(w http.ResponseWriter, r *http.Request, access *bucketAccess, object string, version VersionRequest) error {
	s.logger.Debug("putting object tagging", zap.String("bucket", access.bucket), zap.String("object", object))

	tags, err := readTagging(r, objectTagLimits)
	if err != nil {
		return err
	}
	tagging, err := s.objectTagging(r, access, object, version)
	if err != nil {
		return err
	} else if err := s.backend.PutObjectTagging(r.Context(), access.bucketID, object, tagging.Version, tags); err != nil {
		return err
	}
	setTaggingVersionHeader(w, tagging)
	return nil
}

// getObjectTagging handles GET Object tagging requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html
func (s *s3) getObjectTagging(w http.ResponseWriter, r *http.Request, access *bucketAccess, object string, version VersionRequest) error {
	s.logger.Debug("getting object tagging", zap.String("bucket", access.bucket), zap.String("object", object))

	tagging, err := s.objectTagging(r, access, object, version)
	if err != nil {
		return err
	}
	setTaggingVersionHeader(w, tagging)
	return writeXMLResponse(w, http.StatusOK, Tagging{
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		TagSet: tagging.Tags,
	})
}

// deleteObjectTagging handles DELETE Object tagging requests.
//
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html
func (s *s3) deleteObjectTagging(w http.ResponseWriter, r *http.Request, access *bucketAccess, object string, version VersionRequest) error {
	s.logger.Debug("deleting object tagging", zap.String("bucket", access.bucket), zap.String("object", object))

	tagging, err := s.objectTagging(r, access, object, version)
	if err != nil {
		return err
	}
	if err := s.backend.DeleteObjectTagging(r.Context(), access.bucketID, object, tagging.Version); err != nil {
		return err
	}
	setTaggingVersionHeader(w, tagging)
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// objectTagging looks up the addressed version of object, hiding a missing
// one from callers that may not list it.
func (s *s3) objectTagging(r *http.Request, access *bucketAccess, object string, version VersionRequest) (ObjectTagging, error) {
	tagging, err := s.backend.GetObjectTagging(r.Context(), access.bucketID, object, version)
	if isMissingOrDeleteMarker(err) {
		return ObjectTagging{}, access.hideMissing(err, object)
	}
	return tagging, err
}

// setTaggingVersionHeader reports the version a tagging request acted on.
func setTaggingVersionHeader(w http.ResponseWriter, tagging ObjectTagging) {
	if tagging.VersionID != "" {
		w.Header().Set("x-amz-version-id", tagging.VersionID)
	}
}

// isMissingOrDeleteMarker reports whether err is a backend's report that the
// addressed version does not exist or is a delete marker, which has no tags.
func isMissingOrDeleteMarker(err error) bool {
	return errors.Is(err, s3errs.ErrNoSuchKey) || errors.Is(err, s3errs.ErrNoSuchVersion) || errors.Is(err, s3errs.ErrMethodNotAllowed)
}
