---
default: minor
---

# Implement POST Object

A bucket now accepts a browser form upload. The credential, policy and
signature are read from the form fields, the policy's conditions are enforced,
and `success_action_status` and `success_action_redirect` decide the response.
Such a request was previously refused with `AccessDenied`.
