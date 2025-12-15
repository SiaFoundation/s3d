package s3

import (
	"encoding/xml"
	"net/http"
	"time"
)

// Null is used by S3 to represent an explicit empty value in XML responses.
// Such as a VersionID or location.
const Null = "null"

// globalUserInfo is a static placeholder for all responses requiring user info.
// Once we add authentication, this will be passed tied to the authenticated
// user and persisted in the backend.
var globalUserInfo = &UserInfo{
	ID:          "bcaf1ffd86f41161ca5fb16fd081034f",
	DisplayName: "S3D",
}

// Common types
type (
	// UserInfo represents the owner of a resource
	UserInfo struct {
		ID          string `xml:"ID"`
		DisplayName string `xml:"DisplayName"`
	}
)

// Types related to bucket routes
type (
	// BucketInfo represents an S3 bucket
	BucketInfo struct {
		Name         string      `xml:"Name"`
		CreationDate ContentTime `xml:"CreationDate"`
	}

	// ListBucketsResponse is the response to a ListBuckets request
	ListBucketsResponse struct {
		XMLName xml.Name     `xml:"ListAllMyBucketsResult"`
		Xmlns   string       `xml:"xmlns,attr"`
		Owner   *UserInfo    `xml:"Owner,omitempty"`
		Buckets []BucketInfo `xml:"Buckets>Bucket"`
	}
)

// ContentTime is a wrapper around time.Time to provide custom XML marshalling.
type ContentTime struct {
	time.Time
}

// NewContentTime creates a new ContentTime instance.
func NewContentTime(t time.Time) ContentTime {
	return ContentTime{t}
}

// MarshalXML implements custom XML marshalling for ContentTime.
func (c ContentTime) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	// This is the format expected by the aws xml code, not the default.
	if !c.IsZero() {
		var s = c.UTC().Format("2006-01-02T15:04:05.999Z")
		return e.EncodeElement(s, start)
	}
	return nil
}

// HttpTime is a wrapper around time.Time to provide custom XML marshalling.
type HttpTime struct {
	time.Time
}

// NewHttpTime creates a new HttpTime instance.
func NewHttpTime(t time.Time) HttpTime {
	return HttpTime{t}
}

// MarshalXML implements custom XML marshalling for HttpTime.
func (c HttpTime) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if !c.IsZero() {
		var s = c.UTC().Format(http.TimeFormat)
		return e.EncodeElement(s, start)
	}
	return nil
}

// UnmarshalXML implements custom XML unmarshalling for HttpTime.
func (c *HttpTime) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var value string
	if err := d.DecodeElement(&value, &start); err != nil {
		return err
	}
	t, err := time.Parse(http.TimeFormat, value)
	if err != nil {
		return err
	}
	c.Time = t
	return nil
}

// StdTime returns the standard time.Time value.
func (c *HttpTime) StdTime() time.Time {
	return c.Time
}

// GetBucketLocation is the response to a GetBucketLocation request.
type GetBucketLocation struct {
	XMLName            xml.Name `xml:"LocationConstraint"`
	Xmlns              string   `xml:"xmlns,attr"`
	LocationConstraint string   `xml:",chardata"`
}

type (
	// CommonPrefix is used in Bucket.CommonPrefixes to list partial delimited keys
	// that represent pseudo-directories.
	CommonPrefix struct {
		Prefix string `xml:"Prefix"`
	}

	// Content represents an S3 object in a bucket listing.
	Content struct {
		Key          string       `xml:"Key"`
		LastModified ContentTime  `xml:"LastModified"`
		ETag         string       `xml:"ETag"`
		Size         int64        `xml:"Size"`
		StorageClass StorageClass `xml:"StorageClass,omitempty"`
		Owner        *UserInfo    `xml:"Owner,omitempty"`
	}

	// ListObjectsV2Result is the response to a ListObjectsV2 request.
	ListObjectsV2Result struct {
		ListObjectsResultBase

		// If ContinuationToken was sent with the request, it is included in the
		// response.
		ContinuationToken *string `xml:"ContinuationToken,omitempty"`

		// Returns the number of keys included in the response. The value is always
		// less than or equal to the MaxKeys value.
		KeyCount int64 `xml:"KeyCount,omitempty"`

		// If the response is truncated, Amazon S3 returns this parameter with a
		// continuation token. You can specify the token as the continuation-token
		// in your next request to retrieve the next set of keys.
		NextContinuationToken string `xml:"NextContinuationToken,omitempty"`

		// If StartAfter was sent with the request, it is included in the response.
		StartAfter string `xml:"StartAfter,omitempty"`
	}

	// Version represents a version of an S3 object.
	Version struct {
		XMLName      xml.Name    `xml:"Version"`
		Key          string      `xml:"Key"`
		VersionID    string      `xml:"VersionId"`
		IsLatest     bool        `xml:"IsLatest"`
		LastModified ContentTime `xml:"LastModified,omitempty"`
		Size         int64       `xml:"Size"`

		// According to the S3 docs, this is always STANDARD for a Version:
		StorageClass StorageClass `xml:"StorageClass"`

		ETag  string    `xml:"ETag"`
		Owner *UserInfo `xml:"Owner,omitempty"`
	}

	// ListObjectVersionsResult is the response to a ListObjectVersions request.
	ListObjectVersionsResult struct {
		XMLName        xml.Name       `xml:"ListObjectVersionsResult"`
		Xmlns          string         `xml:"xmlns,attr"`
		Name           string         `xml:"Name"`
		Delimiter      string         `xml:"Delimiter,omitempty"`
		Prefix         string         `xml:"Prefix,omitempty"`
		CommonPrefixes []CommonPrefix `xml:"CommonPrefixes,omitempty"`
		IsTruncated    bool           `xml:"IsTruncated"`
		MaxKeys        int64          `xml:"MaxKeys"`

		// When the number of responses exceeds the value of MaxKeys, NextKeyMarker
		// specifies the first key not returned that satisfies the search criteria.
		// Use this value for the key-marker request parameter in a subsequent
		// request.
		NextKeyMarker string `xml:"NextKeyMarker,omitempty"`

		// Marks the last version of the Key returned in a truncated response.
		VersionIDMarker string `xml:"VersionIdMarker,omitempty"`

		// When the number of responses exceeds the value of MaxKeys,
		// NextVersionIdMarker specifies the first object version not returned that
		// satisfies the search criteria. Use this value for the version-id-marker
		// request parameter in a subsequent request.
		NextVersionIDMarker string `xml:"NextVersionIdMarker,omitempty"`

		// AWS responds with a list of either <Version> or <DeleteMarker> objects. The order
		// needs to be preserved:
		//	<ListBucketVersionsResult>
		//		<DeleteMarker ... />
		//		<Version ... />
		//		<DeleteMarker ... />
		//		<Version ... />
		//	</ListBucketVersionsResult>
		Versions []Version

		// prefixes maintains an index of prefixes that have already been seen.
		// This is a convenience for backend implementers like s3bolt and s3mem,
		// which operate on a full, flat list of keys.
		prefixes map[string]bool
	}

	// ListObjectsResultBase is the common part of a listing response.
	ListObjectsResultBase struct {
		XMLName xml.Name `xml:"ListBucketResult"`
		Xmlns   string   `xml:"xmlns,attr"`

		// Name of the bucket.
		Name string `xml:"Name"`

		// Specifies whether (true) or not (false) all of the results were
		// returned. If the number of results exceeds that specified by MaxKeys,
		// all of the results might not be returned.
		IsTruncated bool `xml:"IsTruncated"`

		// Causes keys that contain the same string between the prefix and the
		// first occurrence of the delimiter to be rolled up into a single result
		// element in the CommonPrefixes collection. These rolled-up keys are not
		// returned elsewhere in the response.
		//
		// NOTE: Each rolled-up result in CommonPrefixes counts as only one return
		// against the MaxKeys value.
		Delimiter string `xml:"Delimiter,omitempty"`

		Prefix string `xml:"Prefix"`

		MaxKeys int64 `xml:"MaxKeys,omitempty"`

		CommonPrefixes []CommonPrefix `xml:"CommonPrefixes,omitempty"`
		Contents       []*Content     `xml:"Contents"`
	}

	// StorageClass represents the storage class of an S3 object. If not specified,
	// it defaults to "STANDARD".
	StorageClass string
)

// MarshalXML implements custom XML marshalling for StorageClass to override the
// empty value.
func (s StorageClass) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if s == "" {
		s = "STANDARD"
	}
	return e.EncodeElement(string(s), start)
}

type (
	// ObjectID represents an object to be deleted in a multi-delete request.
	ObjectID struct {
		Key string `xml:"Key"`

		ETag             *string   `xml:"ETag,omitempty"`
		Size             *int64    `xml:"Size,omitempty"`
		LastModifiedTime *HttpTime `xml:"LastModifiedTime,omitempty"`

		// nolint:tagliatelle
		VersionID string `xml:"VersionId,omitempty"`
	}

	// DeleteRequest represents a multi delete request.
	DeleteRequest struct {
		Objects []ObjectID `xml:"Object"`

		// Quiet is used to enable quiet mode for the request.
		//
		// By default, the operation uses verbose mode in which the response
		// includes the result of deletion of each key in your request. In quiet
		// mode the response includes only keys where the delete operation
		// encountered an error. For a successful deletion, the operation does not
		// return any information about the delete in the response body.
		Quiet bool `xml:"Quiet"`
	}

	// ObjectsDeleteResult contains the response from a multi delete operation.
	ObjectsDeleteResult struct {
		XMLName xml.Name      `xml:"DeleteResult"`
		Deleted []ObjectID    `xml:"Deleted"`
		Error   []ErrorResult `xml:",omitempty"`
	}

	// ErrorResult represents an error encountered while deleting an object
	// during a multi delete operation.
	ErrorResult struct {
		XMLName   xml.Name `xml:"Error"`
		Key       string   `xml:"Key,omitempty"`
		Code      string   `xml:"Code,omitempty"`
		Message   string   `xml:"Message,omitempty"`
		Resource  string   `xml:"Resource,omitempty"`
		RequestID string   `xml:"RequestId,omitempty"`
	}
)

type (
	// ObjectCopyResult contains the response from a CopyObject operation.
	ObjectCopyResult struct {
		XMLName      xml.Name    `xml:"CopyObjectResult"`
		ETag         string      `xml:"ETag"`
		LastModified ContentTime `xml:"LastModified,omitempty"`
	}

	// PartCopyResult contains the response from an UploadPartCopy operation.
	PartCopyResult struct {
		XMLName      xml.Name    `xml:"CopyPartResult"`
		ETag         string      `xml:"ETag"`
		LastModified ContentTime `xml:"LastModified,omitempty"`
	}
)

type (
	// InitiateMultipartUploadResponse matches the XML response returned by AWS
	// when creating a multipart upload.
	InitiateMultipartUploadResponse struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Xmlns    string   `xml:"xmlns,attr"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		UploadID string   `xml:"UploadId"`
	}

	// ListPartsResponse matches the XML response returned by AWS when listing
	// uploaded parts for an in-progress multipart upload.
	ListPartsResponse struct {
		XMLName              xml.Name             `xml:"ListPartsResult"`
		Xmlns                string               `xml:"xmlns,attr"`
		Bucket               string               `xml:"Bucket"`
		Key                  string               `xml:"Key"`
		UploadID             string               `xml:"UploadId"`
		PartNumberMarker     int                  `xml:"PartNumberMarker"`
		NextPartNumberMarker int                  `xml:"NextPartNumberMarker,omitempty"`
		MaxParts             int64                `xml:"MaxParts"`
		IsTruncated          bool                 `xml:"IsTruncated"`
		StorageClass         StorageClass         `xml:"StorageClass,omitempty"`
		Initiator            *UserInfo            `xml:"Initiator,omitempty"`
		Owner                *UserInfo            `xml:"Owner,omitempty"`
		Parts                []ListedPartResponse `xml:"Part"`
	}

	// ListedPartResponse represents a single part entry in a ListParts response.
	ListedPartResponse struct {
		PartNumber   int         `xml:"PartNumber"`
		LastModified ContentTime `xml:"LastModified,omitempty"`
		ETag         string      `xml:"ETag"`
		Size         int64       `xml:"Size"`
	}

	// CompleteMultipartUploadRequest matches the XML request body sent when
	// completing a multipart upload.
	CompleteMultipartUploadRequest struct {
		XMLName xml.Name                `xml:"CompleteMultipartUpload"`
		Parts   []CompleteMultipartPart `xml:"Part"`
	}

	// CompleteMultipartPart represents a single part in a
	// CompleteMultipartUploadRequest.
	CompleteMultipartPart struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	}

	// CompleteMultipartUploadResponse matches the XML response returned after a
	// successful CompleteMultipartUpload operation.
	CompleteMultipartUploadResponse struct {
		XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
		Xmlns    string   `xml:"xmlns,attr"`
		Location string   `xml:"Location"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		ETag     string   `xml:"ETag"`
	}

	// ListMultipartUploadsResponse is the response to a ListMultipartUploads
	// request.
	ListMultipartUploadsResponse struct {
		XMLName            xml.Name                `xml:"ListMultipartUploadsResult"`
		Xmlns              string                  `xml:"xmlns,attr"`
		Bucket             string                  `xml:"Bucket"`
		KeyMarker          string                  `xml:"KeyMarker,omitempty"`
		UploadIDMarker     string                  `xml:"UploadIdMarker,omitempty"`
		NextKeyMarker      string                  `xml:"NextKeyMarker,omitempty"`
		NextUploadIDMarker string                  `xml:"NextUploadIdMarker,omitempty"`
		MaxUploads         int64                   `xml:"MaxUploads"`
		IsTruncated        bool                    `xml:"IsTruncated"`
		Prefix             string                  `xml:"Prefix,omitempty"`
		Delimiter          string                  `xml:"Delimiter,omitempty"`
		CommonPrefixes     []CommonPrefix          `xml:"CommonPrefixes,omitempty"`
		Uploads            []ListedMultipartUpload `xml:"Upload"`
	}

	// ListedMultipartUpload represents a single multipart upload in a listing.
	ListedMultipartUpload struct {
		Key          string       `xml:"Key"`
		UploadID     string       `xml:"UploadId"`
		Initiator    *UserInfo    `xml:"Initiator,omitempty"`
		Owner        *UserInfo    `xml:"Owner,omitempty"`
		StorageClass StorageClass `xml:"StorageClass"`
		Initiated    ContentTime  `xml:"Initiated"`
	}
)
