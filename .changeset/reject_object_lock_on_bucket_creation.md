---
default: patch
---

# Reject Object Lock on bucket creation

`CreateBucket` now refuses a request to enable Object Lock through
`x-amz-bucket-object-lock-enabled` rather than ignoring it, matching the
`?object-lock` subresource. A client can no longer be told a bucket was created
with Object Lock when nothing enforces it. Setting the header to `false` asks
for a bucket without Object Lock and still succeeds.
