---
default: minor
---

# Added `s3d snapshots` commands to manage database snapshots

`s3d snapshots create`, `list`, `delete` and `restore` manage the database
snapshots stored on Sia, backed by new `GET /snapshots` and
`DELETE /snapshots/{objectID}` admin endpoints. Creating a snapshot now flushes
pending objects first so the snapshot can reference them. Snapshots are
addressed by their Sia object ID and listed newest first. `list --remote` and
`restore` find snapshots on the network using only the app key from the local
database, which is how a database that has been lost is recovered.
