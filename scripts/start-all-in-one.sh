#!/usr/bin/env bash
#
# Launches every Flightdeck JVM service inside a single container.
# Used by Dockerfile-all-in-one. Each JVM runs in the background with a
# tagged log prefix; if any one exits the whole container exits so Docker
# can restart the bundle.
#
set -euo pipefail

# Per-JVM heap cap so 5 JVMs don't balloon the container. Override with JAVA_OPTS.
JAVA_OPTS="${JAVA_OPTS:--XX:MaxRAMPercentage=15.0}"

run() {
  local name="$1" jar="$2"
  # stdbuf -oL forces sed to line-buffer; without it sed block-buffers when its
  # stdout is a pipe (as under Docker), so low-volume services' logs — including
  # startup banners and errors — would never surface.
  # shellcheck disable=SC2086
  java $JAVA_OPTS -jar "/app/${jar}" 2>&1 | stdbuf -oL sed "s/^/[${name}] /" &
}

run processing  processing.jar
run think        think.jar
run memoir       memoir.jar
run monitoring   monitoring.jar
run api          api.jar

# If ANY child process exits, stop the container with its exit code.
wait -n
exit $?
