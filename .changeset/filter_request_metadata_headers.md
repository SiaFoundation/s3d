---
default: patch
---

# Filter request headers from object metadata

Object writes now store only object metadata headers. Request control headers
such as signing, copy, tagging, ACL and encryption headers are no longer saved
as metadata or replayed on later reads, and metadata stored by earlier versions
is filtered when an object is read.
