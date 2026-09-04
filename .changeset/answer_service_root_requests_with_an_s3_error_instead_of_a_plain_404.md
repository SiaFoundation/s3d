---
default: patch
---

# Answer service root requests with an S3 error instead of a plain 404

Non-GET requests to the service root returned Go's plain-text `404 page not
found`. AWS dispatches on the method before authenticating there, answering 405
with an `Allow: GET` header, and clients that probe the root before logging in,
such as Cyberduck sending an unsigned `HEAD /`, cannot connect otherwise.
