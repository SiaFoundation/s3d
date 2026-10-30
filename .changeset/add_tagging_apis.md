---
default: minor
---

# Add bucket and object tagging APIs

GetBucketTagging, PutBucketTagging and DeleteBucketTagging, and
GetObjectTagging, PutObjectTagging and DeleteObjectTagging for current and
explicit versions, are now supported. A PUT replaces the whole tag set, up to 50
tags on a bucket and 10 on an object version, and must carry Content-MD5 or a
checksum. Existing databases gain empty tag sets through a schema migration.
