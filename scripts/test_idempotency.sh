#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
ENDPOINT="${BASE_URL}/v1/payments"
IDEM_KEY="test-$(date +%s)-$$"

PAYLOAD=$(cat <<EOF
{
  "merchant_id": "merchant-qa-001",
  "amount": "100.00",
  "currency": "USD",
  "idempotency_key": "${IDEM_KEY}"
}
EOF
)

echo "==> Idempotency integration check"
echo "    endpoint: ${ENDPOINT}"
echo "    idempotency_key: ${IDEM_KEY}"
echo

echo "[1/2] First POST — expect HTTP 201 Created"
STATUS1=$(curl -sS -o /tmp/payment_resp1.json -w "%{http_code}" \
  -X POST "${ENDPOINT}" \
  -H "Content-Type: application/json" \
  -d "${PAYLOAD}")

echo "    status: ${STATUS1}"
echo "    body:   $(cat /tmp/payment_resp1.json)"

if [[ "${STATUS1}" != "201" ]]; then
  echo "FAIL: expected 201, got ${STATUS1}"
  exit 1
fi
echo "PASS: payment created"
echo

echo "[2/2] Second POST with same idempotency_key — expect HTTP 409 Conflict"
STATUS2=$(curl -sS -o /tmp/payment_resp2.json -w "%{http_code}" \
  -X POST "${ENDPOINT}" \
  -H "Content-Type: application/json" \
  -d "${PAYLOAD}")

echo "    status: ${STATUS2}"
echo "    body:   $(cat /tmp/payment_resp2.json)"

if [[ "${STATUS2}" != "409" ]]; then
  echo "FAIL: expected 409 (unique violation / idempotency conflict), got ${STATUS2}"
  exit 1
fi
echo "PASS: duplicate rejected with 409"
echo
echo "OK: all checks passed"
