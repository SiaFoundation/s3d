---
default: minor
---

# Expose snapshot metrics on the admin API's Prometheus endpoint

`/prometheus` now also reports the number of database snapshots, whether the listing is synced with the Sia network, and the time and object count of the latest snapshot.
