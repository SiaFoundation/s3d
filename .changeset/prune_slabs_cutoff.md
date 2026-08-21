---
default: patch
---

# Stop pruning slabs of uploads that are still pinning

The orphan loop now prunes slabs that have been unreferenced for longer than the pin
deadline. It used an hour, but an upload gets 24 hours to pin and slabs are pinned
before the object that references them, so they looked unreferenced and could be
pruned while the upload was still retrying.
