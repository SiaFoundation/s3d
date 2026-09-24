---
default: patch
---

# Report an empty bucket location instead of "null"

GetBucketLocation answered with the literal string `null`, which some clients
take at face value and then use as the signing region. An empty
LocationConstraint is how S3 reports us-east-1.
