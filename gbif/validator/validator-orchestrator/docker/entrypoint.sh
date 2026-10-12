#!/bin/sh
set -eu

# Word splitting is intentional for the JVM option list.
# shellcheck disable=SC2086
exec java ${JVM_OPTIONS:-} -jar /app/service.jar "$@"
