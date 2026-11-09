#!/usr/bin/env bash
# Health-gated blue-green cut-over.
#   ./switch.sh green   -> verify api-green is ready, point the gateway at it, verify through the gateway
#   ./switch.sh blue    -> same in reverse (rollback)
set -euo pipefail

TARGET="${1:?usage: switch.sh blue|green}"
[[ "$TARGET" == "blue" || "$TARGET" == "green" ]] || { echo "target must be blue or green" >&2; exit 2; }

COMPOSE=(docker compose -f "$(dirname "$0")/docker-compose.bluegreen.yml")
UPSTREAM_FILE="$(dirname "$0")/upstreams/active.conf"

echo "==> 1/4 readiness check on api-${TARGET}"
for attempt in $(seq 1 30); do
  if "${COMPOSE[@]}" exec -T gateway wget -qO- "http://api-${TARGET}:8000/health/ready" | grep -q '"ready":true'; then
    break
  fi
  [[ "$attempt" == 30 ]] && { echo "api-${TARGET} never became ready; aborting (traffic unchanged)" >&2; exit 1; }
  sleep 2
done

echo "==> 2/4 smoke test api-${TARGET} directly"
"${COMPOSE[@]}" exec -T gateway wget -qO- --header "X-Tenant-ID: smoke-test" \
  "http://api-${TARGET}:8000/search?q=smoke" >/dev/null

echo "==> 3/4 switching gateway upstream to api-${TARGET}"
cp "$UPSTREAM_FILE" "${UPSTREAM_FILE}.previous"
cat > "$UPSTREAM_FILE" <<EOF
upstream active_api {
  server api-${TARGET}:8000;
  keepalive 32;
}
EOF
if ! "${COMPOSE[@]}" exec -T gateway nginx -t; then
  mv "${UPSTREAM_FILE}.previous" "$UPSTREAM_FILE"
  echo "nginx config invalid; restored previous upstream" >&2
  exit 1
fi
"${COMPOSE[@]}" exec -T gateway nginx -s reload

echo "==> 4/4 verifying through the gateway (root endpoint reports the live APP_VERSION)"
sleep 1
curl -fsS http://localhost:8000/
echo
echo "live colour: ${TARGET}. rollback: ./switch.sh $([[ "$TARGET" == blue ]] && echo green || echo blue)"
