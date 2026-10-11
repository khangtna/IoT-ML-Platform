#!/usr/bin/env bash
source "$(dirname "$0")/compose.sh"
# The integration test creates and removes its own smartcity_test_<pid> database.
"${COMPOSE[@]}" run --rm tests
