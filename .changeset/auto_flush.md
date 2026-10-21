---
default: minor
---

# Auto flush objects after configurable interval

`sia.uploadDeadline` sets how long after an object was last modified its group
is uploaded to Sia regardless of wasted space, so small objects no longer wait
indefinitely for enough data to fill a slab. It defaults to `24h`, and `0s`
disables the deadline.
