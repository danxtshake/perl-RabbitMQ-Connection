#!/usr/bin/env bash
# Build the module and run its live-broker tests against a throwaway RabbitMQ.
#   xt/docker-test.sh            # build image, start broker, make test, tear down
#   KEEP=1 xt/docker-test.sh     # leave the broker running afterwards
# The broker-driving tests (policies, memory alarm, forced close) run rabbitmqctl
# through `docker exec`, so the runner gets the docker socket and CLI mounted.
set -euo pipefail
cd "$(dirname "$0")/.."
NET=rmqc-test-net; BROKER=rmqc-test-broker; RUNNER_IMG=rmqc-test-runner
DOCKER_CLI="${DOCKER_CLI:-$(command -v docker)}"

cleanup() { [ -n "${KEEP:-}" ] || { docker rm -f "$BROKER" >/dev/null 2>&1 || true; docker network rm "$NET" >/dev/null 2>&1 || true; }; }
trap cleanup EXIT

docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET" >/dev/null
docker rm -f "$BROKER" >/dev/null 2>&1 || true
docker run -d --name "$BROKER" --network "$NET" \
  -e RABBITMQ_DEFAULT_USER=test -e RABBITMQ_DEFAULT_PASS=test \
  rabbitmq:3-management-alpine >/dev/null
docker build -q -t "$RUNNER_IMG" -f xt/Dockerfile.test xt >/dev/null

echo "waiting for the broker"
for i in $(seq 1 60); do
  docker exec "$BROKER" rabbitmq-diagnostics -q check_running >/dev/null 2>&1 && break; sleep 2
done

docker run --rm --network "$NET" \
  -v "$PWD:/src" -w /src \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$DOCKER_CLI:/usr/local/bin/docker:ro" \
  -e TEST_HOST="$BROKER" -e TEST_PORT=5672 -e TEST_USER=test -e TEST_PASS=test \
  -e TEST_CTL="docker exec $BROKER rabbitmqctl" \
  "$RUNNER_IMG" sh -c 'perl Makefile.PL >/dev/null && make >/dev/null && make test TEST_VERBOSE=1'
