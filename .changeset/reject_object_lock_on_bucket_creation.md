---
default: patch
---

# Reject Object Lock on bucket creation

`CreateBucket` now refuses `x-amz-bucket-object-lock-enabled` rather than
ignoring it, matching the `?object-lock` subresource. A client can no longer be
told a bucket was created with Object Lock when nothing enforces it.
