---
default: patch
---

# Fix a panic on a malformed Content-MD5

A `Content-MD5` that did not decode to a 16 byte digest panicked the request
handler and dropped the connection. It is now refused with `InvalidDigest`.
