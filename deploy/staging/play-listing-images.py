#!/usr/bin/env python3
"""Upload the store listing's app icon and feature graphic to Google Play (Android Publisher API v3).

    play-listing-images.py --package ca.canifishthis.app [--icon PATH] [--feature PATH] [--language en-US]
                           [--key ~/.canifishthis/play-service-account.json] [--dry-run]

One edit: insert -> for each image type: images.deleteall -> images.upload -> commit. Each image REPLACES what the
listing holds for its type (Play keeps one icon and one feature graphic per language). Sizes are checked before anything
is sent — icon 512x512 PNG, feature graphic 1024x500 PNG without alpha — and a wrong one is refused by name, never
resized. --dry-run checks the files and opens + deletes an edit (proves the key reaches the app) without changing it.
The service-account JSON is a secret: it lives in ~/.canifishthis/, never in the repo. Needs google-auth, requests and
Pillow (the repo .venv has them).
"""
import argparse
import os
import sys
from pathlib import Path

from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account
from PIL import Image

API = "https://androidpublisher.googleapis.com/androidpublisher/v3/applications"
UPLOAD = "https://androidpublisher.googleapis.com/upload/androidpublisher/v3/applications"
SCOPE = ["https://www.googleapis.com/auth/androidpublisher"]
GRAPHICS = Path(__file__).resolve().parents[2] / "handoff/PLAN/app-integration/store/graphics"

#: Play's image type -> (size, alpha allowed)
RULES = {"icon": ((512, 512), True), "featureGraphic": ((1024, 500), False)}


def die(msg, r=None):
    detail = f" HTTP {r.status_code}: {r.text[:800]}" if r is not None else ""
    sys.exit(f"play-listing-images: {msg}{detail}")


def ok(r, what):
    if r.status_code >= 300:
        die(what, r)
    return r.json() if r.content else {}


def check(kind: str, path: Path) -> None:
    if not path.is_file():
        die(f"{kind}: no file at {path}")
    (size, alpha) = RULES[kind]
    with Image.open(path) as im:
        if im.format != "PNG":
            die(f"{kind}: {path.name} is {im.format}, not PNG")
        if im.size != size:
            die(f"{kind}: {path.name} is {im.size[0]}x{im.size[1]}, Play wants {size[0]}x{size[1]}")
        if not alpha and ("A" in im.getbands() or "transparency" in im.info):
            die(f"{kind}: {path.name} has an alpha channel, which Play refuses for this type")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--package", required=True)
    ap.add_argument("--icon", type=Path, default=GRAPHICS / "app-icon-512.png")
    ap.add_argument("--feature", type=Path, default=GRAPHICS / "feature-graphic-1024x500.png")
    ap.add_argument("--language", default="en-US")
    ap.add_argument("--key", default=os.path.expanduser("~/.canifishthis/play-service-account.json"))
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    images = {"icon": a.icon, "featureGraphic": a.feature}
    for kind, path in images.items():
        check(kind, path)
    if not os.path.isfile(a.key):
        die(f"no service-account key at {a.key} (owner step: create it in Google Cloud, grant it in Play Console)")
    s = AuthorizedSession(service_account.Credentials.from_service_account_file(a.key, scopes=SCOPE))
    base = f"{API}/{a.package}/edits"
    edit = ok(s.post(base, json={}), "edits.insert")["id"]
    if a.dry_run:
        ok(s.delete(f"{base}/{edit}"), "edits.delete")
        print(f"dry run: files fit, key reaches {a.package}; nothing changed")
        return
    for kind, path in images.items():
        ok(s.delete(f"{base}/{edit}/listings/{a.language}/{kind}"), f"images.deleteall {kind}")
        r = s.post(f"{UPLOAD}/{a.package}/edits/{edit}/listings/{a.language}/{kind}",
                   params={"uploadType": "media"}, data=path.read_bytes(), headers={"Content-Type": "image/png"})
        got = ok(r, f"images.upload {kind}")
        print(f"uploaded {kind}: {path.name} ({got.get('image', {}).get('sha256', '?')[:12]}) -> listings/{a.language}/{kind}")
    ok(s.post(f"{base}/{edit}:commit"), "edits.commit")
    print(f"committed: listing {a.language} icon + feature graphic on {a.package}")


if __name__ == "__main__":
    main()
