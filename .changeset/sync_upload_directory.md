---
default: patch
---

# Sync the upload directory after staging an object

Staged uploads were written and synced, but the directory holding them was
not, so an unclean shutdown could leave the database referencing a file that
no longer had a directory entry.
