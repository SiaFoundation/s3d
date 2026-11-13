---
default: minor
---

# Tag objects on upload and copy

PutObject, CreateMultipartUpload and POST Object forms now accept tags, which
require `s3:PutObjectTagging`. CopyObject keeps the source's tags, or replaces
them when `x-amz-tagging-directive` is `REPLACE`, and GET and HEAD report
`x-amz-tagging-count` to callers allowed to read the tags.
