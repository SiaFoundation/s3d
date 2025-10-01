package s3

import (
	"encoding/xml"
	"time"
)

// Common types
type (
	// UserInfo represents the owner of a resource
	UserInfo struct {
		ID          string `xml:"ID"`
		DisplayName string `xml:"DisplayName"`
	}
)

// globalUserInfo is a static placeholder for all responses requiring user info.
// Once we add authentication, this will be passed tied to the authenticated
// user and persisted in the backend.
var globalUserInfo = &UserInfo{
	ID:          "bcaf1ffd86f41161ca5fb16fd081034f",
	DisplayName: "S3D",
}

// Types related to bucket routes
type (
	// BucketInfo represents an S3 bucket
	BucketInfo struct {
		Name         string    `xml:"Name"`
		CreationDate time.Time `xml:"CreationDate"`
	}

	// ListBucketsResponse is the response to a ListBuckets request
	ListBucketsResponse struct {
		XMLName xml.Name     `xml:"ListAllMyBucketsResult"`
		Xmlns   string       `xml:"xmlns,attr"`
		Owner   *UserInfo    `xml:"Owner,omitempty"`
		Buckets []BucketInfo `xml:"Buckets>Bucket"`
	}
)
