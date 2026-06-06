#!/usr/bin/env bash
# Upload ATT SDP deck to Google Slides (requires one-time OAuth setup).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PRES_ID="${1:-1qj_BWxHYc7WIeIclSm7NhQ7driRx0MY0iYr_BEUrXXM}"
CREDS="$ROOT/config/google_credentials.json"

echo "==> ATT SDP → Google Slides upload"
echo "    Presentation: https://docs.google.com/presentation/d/${PRES_ID}/edit"

if [[ ! -f "$CREDS" ]]; then
  echo ""
  echo "Missing $CREDS"
  echo ""
  echo "One-time setup (5 min):"
  echo "  1. https://console.cloud.google.com/ → APIs: enable Slides + Drive"
  echo "  2. Credentials → OAuth client ID → Desktop app → Download JSON"
  echo "  3. Save as: config/google_credentials.json"
  echo "  4. Re-run: $0"
  echo ""
  echo "Manual import instead:"
  echo "  Open: $ROOT/docs/ATT_SDP_GoogleSlides_Import/ATT_SDP_Architecture_22slides.pdf"
  echo "  In Slides: File → Import slides → Upload → Replace all slides"
  open "$ROOT/docs/ATT_SDP_GoogleSlides_Import" 2>/dev/null || true
  open "https://docs.google.com/presentation/d/${PRES_ID}/edit" 2>/dev/null || true
  exit 1
fi

python3 -m pip install -q -r "$ROOT/scripts/requirements-google.txt"
if [[ ! -f "$ROOT/config/google_token.json" ]]; then
  echo "==> First login — browser will open…"
  python3 "$ROOT/scripts/google_slides_upload.py" --auth-only --presentation-id "$PRES_ID"
fi
python3 "$ROOT/scripts/google_slides_upload.py" --build --presentation-id "$PRES_ID"
echo ""
echo "Done: https://docs.google.com/presentation/d/${PRES_ID}/edit"
