---
default: patch
---

Report an empty bucket location instead of "null"

GetBucketLocation returned the literal string "null" for us-east-1. Amazon S3 represents the absence of a location constraint as an empty LocationConstraint element; SDKs expose this as null. Some clients were treating the literal "null" as the region name and using it for request signing.
