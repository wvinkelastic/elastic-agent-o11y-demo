#!/usr/bin/env bash
# Local dev helper — loads .env and runs the chat agent. Not used in Docker.
cd "$(dirname "$0")"
set -a
source .env
set +a
exec python3 server.py
