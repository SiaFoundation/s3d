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
