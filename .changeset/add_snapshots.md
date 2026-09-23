---
default: minor
---

# Add snapshots

A POST /snapshots admin endpoint uploads a backup of the metadata database to Sia
and prevents unpinning objects a snapshot still references. It replaces the
POST /system/sqlite3/backup endpoint.
