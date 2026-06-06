#!/usr/bin/env python3
"""Upload ATT SDP architecture deck to Google Slides via OAuth.

Setup: docs/GOOGLE_OAUTH_SETUP.md

  python3 -m pip install -r scripts/requirements-google.txt
  python3 scripts/google_slides_upload.py --auth-only   # first login
  python3 scripts/google_slides_upload.py --build         # build HTML + upload
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HTML = ROOT / "docs" / "ATT_SDP_Architecture.html"
CACHE = ROOT / "docs" / ".slides_upload_cache"
CREDENTIALS = ROOT / "config" / "google_credentials.json"
TOKEN = ROOT / "config" / "google_token.json"
DEFAULT_PRESENTATION_ID = "1qj_BWxHYc7WIeIclSm7NhQ7driRx0MY0iYr_BEUrXXM"

SCOPES = [
    "https://www.googleapis.com/auth/presentations",
    "https://www.googleapis.com/auth/drive.file",
]

CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"


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
            "Follow docs/GOOGLE_OAUTH_SETUP.md to download google_credentials.json"
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


def slide_count(html_path: Path) -> int:
    text = html_path.read_text(encoding="utf-8")
    return text.count('class="slide"')


def render_slide_pngs(html_path: Path, out_dir: Path) -> list[Path]:
    if not Path(CHROME).exists():
        sys.exit(f"Chrome not found at {CHROME} — required for slide screenshots")

    out_dir.mkdir(parents=True, exist_ok=True)
    n = slide_count(html_path)
    paths: list[Path] = []

    for i in range(n):
        out = out_dir / f"slide_{i:02d}.png"
        url = f"file://{html_path.resolve()}?capture=1&slide={i}"
        r = subprocess.run(
            [
                CHROME,
                "--headless=new",
                "--disable-gpu",
                "--hide-scrollbars",
                "--window-size=1280,720",
                f"--screenshot={out}",
                url,
            ],
            capture_output=True,
            text=True,
        )
        if not out.exists() or out.stat().st_size < 500:
            sys.exit(f"Screenshot failed for slide {i + 1}: {r.stderr[:400]}")
        paths.append(out)
        print(f"  Rendered slide {i + 1}/{n} → {out.name}")

    return paths


def upload_image(drive_service, path: Path) -> str:
    from googleapiclient.http import MediaFileUpload

    meta = {"name": f"att-sdp-slide-{path.stem}", "mimeType": "image/png"}
    media = MediaFileUpload(str(path), mimetype="image/png", resumable=True)
    created = (
        drive_service.files()
        .create(body=meta, media_body=media, fields="id")
        .execute()
    )
    file_id = created["id"]
    drive_service.permissions().create(
        fileId=file_id,
        body={"type": "anyone", "role": "reader"},
    ).execute()
    return file_id


def replace_presentation_slides(
    slides_service,
    drive_service,
    presentation_id: str,
    image_paths: list[Path],
) -> None:
    pres = slides_service.presentations().get(presentationId=presentation_id).execute()
    page_size = pres.get("pageSize", {})
    width = page_size.get("width", {}).get("magnitude", 9144000)
    height = page_size.get("height", {}).get("magnitude", 5143500)

    delete_requests = [
        {"deleteObject": {"objectId": slide["objectId"]}}
        for slide in pres.get("slides", [])
    ]
    if delete_requests:
        slides_service.presentations().batchUpdate(
            presentationId=presentation_id,
            body={"requests": delete_requests},
        ).execute()
        print(f"Deleted {len(delete_requests)} existing slide(s)")

    for i, img in enumerate(image_paths):
        file_id = upload_image(drive_service, img)
        url = f"https://drive.google.com/uc?export=view&id={file_id}"
        slide_id = f"att_sdp_{i}_{uuid.uuid4().hex[:8]}"
        requests = [
            {
                "createSlide": {
                    "objectId": slide_id,
                    "slideLayoutReference": {"predefinedLayout": "BLANK"},
                }
            },
            {
                "createImage": {
                    "url": url,
                    "elementProperties": {
                        "pageObjectId": slide_id,
                        "size": {
                            "width": {"magnitude": width, "unit": "EMU"},
                            "height": {"magnitude": height, "unit": "EMU"},
                        },
                        "transform": {
                            "scaleX": 1,
                            "scaleY": 1,
                            "translateX": 0,
                            "translateY": 0,
                            "unit": "EMU",
                        },
                    },
                }
            },
        ]
        slides_service.presentations().batchUpdate(
            presentationId=presentation_id,
            body={"requests": requests},
        ).execute()
        print(f"  Uploaded slide {i + 1}/{len(image_paths)}")


def build_deck() -> None:
    script = ROOT / "scripts" / "build_att_sdp_slides.py"
    subprocess.run([sys.executable, str(script)], check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload ATT SDP deck to Google Slides")
    parser.add_argument(
        "--presentation-id",
        default=DEFAULT_PRESENTATION_ID,
        help="Google Slides presentation ID",
    )
    parser.add_argument("--auth-only", action="store_true", help="Run OAuth login only")
    parser.add_argument("--reauth", action="store_true", help="Force new OAuth consent")
    parser.add_argument("--build", action="store_true", help="Run build_att_sdp_slides.py first")
    parser.add_argument("--dry-run", action="store_true", help="Render PNGs only, no upload")
    args = parser.parse_args()

    if args.auth_only:
        creds = authenticate(reauth=args.reauth)
        print("Authentication OK. Token saved.")
        return

    if args.build:
        build_deck()

    if not HTML.exists():
        build_deck()

    print(f"Rendering slides from {HTML.name}…")
    images = render_slide_pngs(HTML, CACHE)
    print(f"Rendered {len(images)} slide image(s) in {CACHE}")

    if args.dry_run:
        print("Dry run — skipping Google Slides upload.")
        return

    creds = authenticate(reauth=args.reauth)
    if args.auth_only:
        print("Authentication OK. Token saved.")
        return

    _require_google_libs()
    from googleapiclient.discovery import build

    slides_service = build("slides", "v1", credentials=creds, cache_discovery=False)
    drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)

    print(f"Uploading to presentation {args.presentation_id}…")
    replace_presentation_slides(
        slides_service, drive_service, args.presentation_id, images
    )
    print(
        "\nDone. Open your deck:\n"
        f"  https://docs.google.com/presentation/d/{args.presentation_id}/edit\n"
        "Verify slide 1 shows: Deck v2026-06-06"
    )


if __name__ == "__main__":
    main()
