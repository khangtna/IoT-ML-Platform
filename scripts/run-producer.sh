#!/usr/bin/env bash
source "$(dirname "$0")/compose.sh"
"${COMPOSE[@]}" run --rm producer "$@"
