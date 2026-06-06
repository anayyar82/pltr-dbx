#!/usr/bin/env python3
"""Upload ATT SDP component guide to Google Docs via OAuth.

Setup: docs/GOOGLE_OAUTH_SETUP.md (+ enable Google Docs API)

  python3 scripts/build_att_sdp_doc.py
  python3 scripts/google_docs_upload.py --auth-only
  python3 scripts/google_docs_upload.py
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOC_TXT = ROOT / "docs" / "ATT_SDP_GoogleDocs_Import" / "ATT_SDP_Component_Guide.txt"
CREDENTIALS = ROOT / "config" / "google_credentials.json"
TOKEN = ROOT / "config" / "google_token.json"
DEFAULT_DOC_ID = "1D8VVsgzxcIrxunHY56dUiqSmD5GMZGIN74RdwvNdNBs"

SCOPES = [
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/drive.file",
]


def _require_google_libs():
    try:
        from google.auth.transport.requests import Request  # noqa: F401
        from google.oauth2.credentials import Credentials  # noqa: F401
        from google_auth_oauthlib.flow import InstalledAppFlow  # noqa: F401
        from googleapiclient.discovery import build  # noqa: F401
    except ImportError as exc:
        sys.exit(
            "Missing Google API libraries. Run:\n"
            "  python3 -m pip install -r scripts/requirements-google.txt\n"
            f"Detail: {exc}"
        )


def authenticate(*, reauth: bool = False):
    _require_google_libs()
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow

    if not CREDENTIALS.exists():
        sys.exit(
            f"OAuth credentials not found at {CREDENTIALS}\n"
            "See docs/GOOGLE_OAUTH_SETUP.md"
        )

    creds = None
    if TOKEN.exists() and not reauth:
        creds = Credentials.from_authorized_user_file(str(TOKEN), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS), SCOPES)
            creds = flow.run_local_server(port=0)
        TOKEN.parent.mkdir(parents=True, exist_ok=True)
        TOKEN.write_text(creds.to_json(), encoding="utf-8")
        print(f"Saved token → {TOKEN}")

    return creds


def load_text() -> str:
    if not DOC_TXT.exists():
        import subprocess
        subprocess.run([sys.executable, str(ROOT / "scripts" / "build_att_sdp_doc.py")], check=True)
    return DOC_TXT.read_text(encoding="utf-8")


def replace_document_text(docs_service, document_id: str, text: str) -> None:
    doc = docs_service.documents().get(documentId=document_id).execute()
    body = doc.get("body", {})
    content = body.get("content", [])

    end_index = 1
    if content:
        last = content[-1]
        end_index = last.get("endIndex", 1)

    requests = []
    if end_index > 2:
        requests.append({
            "deleteContentRange": {
                "range": {"startIndex": 1, "endIndex": end_index - 1},
            }
        })

    requests.append({
        "insertText": {
            "location": {"index": 1},
            "text": text,
        }
    })

    docs_service.documents().batchUpdate(
        documentId=document_id,
        body={"requests": requests},
    ).execute()


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload ATT SDP guide to Google Docs")
    parser.add_argument("--document-id", default=DEFAULT_DOC_ID)
    parser.add_argument("--auth-only", action="store_true")
    parser.add_argument("--reauth", action="store_true")
    parser.add_argument("--build", action="store_true")
    args = parser.parse_args()

    if args.build:
        import subprocess
        subprocess.run([sys.executable, str(ROOT / "scripts" / "build_att_sdp_doc.py")], check=True)

    if args.auth_only:
        authenticate(reauth=args.reauth)
        print("Authentication OK.")
        return

    creds = authenticate(reauth=args.reauth)
    text = load_text()
    # Strip leading metadata lines for cleaner doc body optional - keep all for context

    _require_google_libs()
    from googleapiclient.discovery import build

    docs = build("docs", "v1", credentials=creds, cache_discovery=False)
    print(f"Uploading to document {args.document_id}…")
    replace_document_text(docs, args.document_id, text)
    print(
        "\nDone. Open:\n"
        f"  https://docs.google.com/document/d/{args.document_id}/edit"
    )


if __name__ == "__main__":
    main()
