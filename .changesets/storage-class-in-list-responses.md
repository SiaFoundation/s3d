---
default: patch
---

Fixed StorageClass XML element missing from list object and list parts responses, restoring compatibility with S3 clients like Arq Backup that require it.
