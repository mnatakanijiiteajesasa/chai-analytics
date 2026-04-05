#!/usr/bin/env bash
# ChaiMetrics API smoke tests
# Run from WSL host after `docker compose up -d`
#
#   bash scripts/test_api.sh
#   bash scripts/test_api.sh KTD-13033   # test a specific member

BASE="http://localhost:5000"
MEMBER=${1:-KTD-22354}

echo "=== ChaiMetrics API smoke tests ==="
echo "Base URL : $BASE"
echo "Member   : $MEMBER"
echo ""

#  Health check 
echo "--- GET /health"
curl -s "$BASE/health" | python3 -m json.tool
echo ""

#  Login 
echo "--- POST /auth/login"
TOKEN_RESP=$(curl -s -X POST "$BASE/auth/login" \
  -H "Content-Type: application/json" \
  -d "{\"ktda_member_no\": \"$MEMBER\", \"password\": \"$MEMBER\"}")
echo $TOKEN_RESP | python3 -m json.tool
TOKEN=$(echo $TOKEN_RESP | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])" 2>/dev/null)

if [ -z "$TOKEN" ]; then
  echo "[FAIL] Could not extract token. Stopping."
  exit 1
fi
echo "Token OK"
echo ""

#  List farms 
echo "--- GET /farms?factory=WRU-01&per_page=3"
curl -s "$BASE/farms?factory=WRU-01&per_page=3" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
echo ""

#  Single farm 
echo "--- GET /farms/$MEMBER"
curl -s "$BASE/farms/$MEMBER" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
echo ""

#  Insights (no narrative — faster) 
echo "--- GET /farms/$MEMBER/insights"
curl -s "$BASE/farms/$MEMBER/insights" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
echo ""

#  Insights with narrative (calls Ollama — ~5-10s) 
echo "--- GET /farms/$MEMBER/insights?narrative=true"
curl -s "$BASE/farms/$MEMBER/insights?narrative=true&refresh=true" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
echo ""

#  Pricing trends 
echo "--- GET /pricing/trends/WRU-01"
curl -s "$BASE/pricing/trends/WRU-01" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
echo ""

echo "--- GET /pricing/centres"
curl -s "$BASE/pricing/centres" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
echo ""

echo "=== Done ==="