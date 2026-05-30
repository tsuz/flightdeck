#!/bin/sh
set -e

# Runs via nginx's /docker-entrypoint.d/ mechanism before nginx starts.
# Renders runtime config from the WS_URL env var into the served env.js.
# Only ${WS_URL} is substituted; empty value = auto-detect from the page.
: "${WS_URL:=}"
export WS_URL
envsubst '${WS_URL}' < /etc/flightdeck/env.template.js > /usr/share/nginx/html/env.js
