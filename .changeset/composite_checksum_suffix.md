---
default: patch
---

# Fixed multipart uploads from mc and other minio-go clients

A composite checksum sent on CompleteMultipartUpload without its part count suffix was answered with BadDigest, which failed every multipart upload from a client built on minio-go.
