#!/usr/bin/env bash
source "$(dirname "$0")/compose.sh"
if [[ ! -f .env ]]; then cp .env.example .env; fi
"${COMPOSE[@]}" up -d --build
