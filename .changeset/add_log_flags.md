---
default: minor
---

# Add log flags

`-log.file.enabled` and `-log.stdout.enableANSI` set the two log options from the
command line, so a service manager that already captures stdout can turn off the
log file and the color codes without editing the config file. Neither default
changed.
