#!/usr/bin/env bash
# Full wipe + redeploy using the 3-workflow pattern + health check.
set -euo pipefail

PROFILE="${DATABRICKS_PROFILE:-e2-demo-field-eng}"
TARGET="${BUNDLE_TARGET:-e2_demo}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ORG="1444828305810485"
HOST="https://e2-demo-field-eng.cloud.databricks.com"
APP="https://att-sdp-ops-ankur-${ORG}.aws.databricksapps.com"

echo "==> 1/4 Bundle deploy"
cd "$ROOT"
databricks bundle deploy -t "$TARGET" --profile "$PROFILE" --auto-approve

echo "==> 2/4 Cleanup (drop tables, views, MV, bronze volumes)"
databricks bundle run sdp_cleanup -t "$TARGET" --profile "$PROFILE"

echo "==> 3/4 Semantic setup (gold + seed + DLT full refresh + MV)"
databricks bundle run sdp_semantic_setup -t "$TARGET" --profile "$PROFILE"

echo "==> 4/4 Full pipeline (autoloader → SDP → Lakebase → App)"
databricks bundle run sdp_full_pipeline -t "$TARGET" --profile "$PROFILE"
sleep 30
TOKEN=$(databricks auth token --profile "$PROFILE" | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
curl -sS -H "Authorization: Bearer $TOKEN" "$APP/health" | python3 -m json.tool || true

cat <<EOF

================================================================================
ATT SDP — FULL REDEPLOY COMPLETE
================================================================================
App:        ${APP}/
Live Demo:  ${APP}/live
Health:     ${APP}/health
Catalog:    ${HOST}/explore/data/users/ankur_nayyar?o=${ORG}
Jobs:       ${HOST}/jobs?o=${ORG}
Lakebase:   ${HOST}/lakebase/projects/52d0022f-9c22-43bd-80fb-0971d8c46080?o=${ORG}

Daily refresh after append writes:
  databricks bundle run sdp_refresh -t ${TARGET} --profile ${PROFILE}
================================================================================
EOF
