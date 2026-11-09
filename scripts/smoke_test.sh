#!/usr/bin/env bash
# End-to-end smoke test of every required endpoint against a running instance.
#   bash scripts/smoke_test.sh [base-url]        (default http://localhost:8000)
# Works in sync and async indexing modes (polls search until the document is visible).
set -euo pipefail

BASE="${1:-http://localhost:8000}"
TENANT="smoke-$(date +%s)"
OTHER="smoke-other-$(date +%s)"

ok()   { printf '  ok    %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1" >&2; exit 1; }
code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
expect() { [[ "$2" == *"$3"* ]] && ok "$1 -> $3" || fail "$1: expected $2, got $3"; }

echo "Smoke testing $BASE (tenant $TENANT)"

expect "GET /health" "200" "$(code "$BASE/health")"

response=$(curl -s -w '\n%{http_code}' -X POST "$BASE/documents" \
  -H "X-Tenant-ID: $TENANT" -H 'Content-Type: application/json' \
  -d '{"title":"Smoke test document","content":"Distributed search smoke test.","tags":["smoke"],"metadata":{"suite":"ci"}}')
status="${response##*$'\n'}"
body="${response%$'\n'*}"
expect "POST /documents" "201 202" "$status"
DOC_ID=$(printf '%s' "$body" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
[[ -n "$DOC_ID" ]] || fail "no document id in response: $body"

found=""
for _ in $(seq 1 20); do
  if curl -s "$BASE/search?q=smoke&tenant=$TENANT" | grep -q '"total":1'; then found=1; break; fi
  sleep 0.5
done
[[ -n "$found" ]] && ok "GET /search?q=smoke&tenant=... -> 1 result" || fail "document never became searchable"

expect "GET /documents/{id}"                 "200" "$(code -H "X-Tenant-ID: $TENANT" "$BASE/documents/$DOC_ID")"
expect "GET /documents/{id} as other tenant" "404" "$(code -H "X-Tenant-ID: $OTHER" "$BASE/documents/$DOC_ID")"
expect "GET /search without tenant"          "400" "$(code "$BASE/search?q=smoke")"
expect "DELETE /documents/{id}"              "204" "$(code -X DELETE -H "X-Tenant-ID: $TENANT" "$BASE/documents/$DOC_ID")"
expect "GET /documents/{id} after delete"    "404" "$(code -H "X-Tenant-ID: $TENANT" "$BASE/documents/$DOC_ID")"

echo "All smoke checks passed."
