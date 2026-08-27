---
default: minor
---

# Added `s3d snapshots` commands and admin endpoints to manage database snapshots

`s3d snapshots` manages database snapshots stored on Sia. `create`, `list`, and
`delete` wrap the admin API, `list --remote` enumerates the snapshots on the
network, and `restore` recovers a lost database from Sia alone. Snapshots are
addressed by their Sia object ID and listed newest first. Deleting a snapshot
unpins its Sia object and releases the orphaned objects it was withholding.
