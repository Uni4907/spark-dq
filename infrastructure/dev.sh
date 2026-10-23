#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose --env-file .env -f infrastructure/compose.yaml)
case "${1:-up}" in
  build)
    docker run --rm -v "$PWD:/src" -v spark-dq-maven-cache:/root/.m2 -w /src maven:3.9.12-eclipse-temurin-17@sha256:a0603aab698040d9c94259f379ec0487da1678560748d6c7508483034033c53d mvn -B package
    "${compose[@]}" build
    ;;
  up) "${compose[@]}" up -d ;;
  stop) "${compose[@]}" stop ;;
  down) "${compose[@]}" down ;;
  logs) "${compose[@]}" logs --tail=100 ;;
  status) "${compose[@]}" ps ;;
  *) echo "Usage: bash infrastructure/dev.sh {build|up|stop|down|logs|status}";exit 2 ;;
esac
