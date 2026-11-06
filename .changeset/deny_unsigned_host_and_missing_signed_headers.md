---
default: patch
---

# Deny requests that do not sign host or omit a signed header

A SigV4 request whose signed headers leave out `host` is now denied with
`AccessDenied` (403), and one missing a header it signed with
`SignatureDoesNotMatch` (403), instead of `AuthorizationHeaderMalformed` or
`AuthorizationQueryParametersError` (400).
