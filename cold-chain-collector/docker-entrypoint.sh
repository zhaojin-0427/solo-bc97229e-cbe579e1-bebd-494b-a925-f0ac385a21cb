#!/bin/sh
# Named Docker volumes are created root-owned; fix ownership once, then drop
# privileges to the unprivileged appuser for the actual process.
set -e

if [ "$(id -u)" = "0" ]; then
    chown -R appuser:appuser /data
    exec runuser -u appuser -- "$@"
fi

exec "$@"
