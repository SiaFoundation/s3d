---
default: patch
---

# Fail puts with SlowDown when waiting for disk space times out

Requests that wait for disk usage to drop below the limit now give up after
two minutes and return a 503 SlowDown error instead of blocking forever.
Previously a blocked put could pin its handler goroutine indefinitely, even
after the client disconnected, and prevent graceful shutdown from completing.
