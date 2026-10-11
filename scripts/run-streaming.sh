#!/usr/bin/env bash
source "$(dirname "$0")/compose.sh"
# The streaming driver is a managed Compose service with its own checkpoint.
"${COMPOSE[@]}" up -d streaming
"${COMPOSE[@]}" logs -f streaming
