#!/usr/bin/env bash
source "$(dirname "$0")/compose.sh"
"${COMPOSE[@]}" down
