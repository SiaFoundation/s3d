package s3

import (
	"encoding/xml"
	"time"
)

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

	Content struct {
		Key          string       `xml:"Key"`
		LastModified ContentTime  `xml:"LastModified"`
		ETag         string       `xml:"ETag"`
		Size         int64        `xml:"Size"`
		StorageClass StorageClass `xml:"StorageClass,omitempty"`
		Owner        *UserInfo    `xml:"Owner,omitempty"`
	}

	ListObjectsV2Result struct {
		ListObjectsResultBase

		// If ContinuationToken was sent with the request, it is included in the
		// response.
		ContinuationToken string `xml:"ContinuationToken,omitempty"`

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
		// against the MaxKeys value. (BW: been waiting to find some confirmation of
		// that for a while!)
		Delimiter string `xml:"Delimiter,omitempty"`

		Prefix string `xml:"Prefix"`

		MaxKeys int64 `xml:"MaxKeys,omitempty"`

		CommonPrefixes []CommonPrefix `xml:"CommonPrefixes,omitempty"`
		Contents       []*Content     `xml:"Contents"`
	}

	StorageClass string
)

func (s StorageClass) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if s == "" {
		s = "STANDARD"
	}
	return e.EncodeElement(string(s), start)
}
