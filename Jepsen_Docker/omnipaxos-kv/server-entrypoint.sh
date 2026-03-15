#!/bin/bash
# server-entrypoint.sh
set -e
/usr/sbin/sshd
exec /usr/local/bin/server
