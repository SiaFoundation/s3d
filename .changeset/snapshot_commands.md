---
default: minor
---

# Added `s3d snapshots` commands and admin endpoints to create, list, delete, and restore database snapshots

`s3d snapshots` manages database backups stored on Sia. `create`, `list`, and
`delete` wrap the admin API, `list --remote` enumerates the snapshots on the
network, and `restore` downloads a backup and writes it to the data directory so
a lost database can be recovered from Sia alone. Snapshots are addressed by their
Sia object ID, the only identifier that survives losing the database. Deleting a
snapshot unpins its backup object and releases the orphaned objects it was
withholding, which is what keeps deleted data from being retained forever.
