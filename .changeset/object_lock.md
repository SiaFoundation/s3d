---
default: minor
---

# Added support for S3 Object Lock

Buckets can now be created with object lock enabled, or have it turned on once
versioning is enabled. Object versions can be protected by a retention period in
`GOVERNANCE` or `COMPLIANCE` mode, by a legal hold, or by the bucket's default
retention rule, and a locked version cannot be deleted until its protection
ends. `x-amz-bypass-governance-retention` lets the bucket owner shorten, clear
or delete through a `GOVERNANCE` retention. Event holds are not supported and
are rejected with `NotImplemented`.
