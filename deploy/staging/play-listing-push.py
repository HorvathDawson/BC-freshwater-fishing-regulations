#!/usr/bin/env python3
"""Push the store listing (text, phone screenshots, icon, feature graphic, promo video URL) to Google Play.

    play-listing-push.py [--package ca.canifishthis.app] [--language en-US ...] [--shots DIR] [--video-url URL]
                         [--key ~/.canifishthis/play-service-account.json] [--commit]

Default is a DRY RUN: open an edit, apply everything, ask Play to VALIDATE it, print what would change, then DELETE the
edit; nothing reaches the listing. --commit does the same and commits the edit instead of deleting it.

- Text: title "Can I Fish This?", the short and full descriptions from handoff/PLAN/play/store-description.md (the
  "## Short description" section, and "## Long description" to the end of the file). Over 30 / 80 / 4000 characters
  is refused before anything is sent.
- Phone screenshots: every PNG/JPEG in --shots (default ~/bcfish-session-backup-2026-11-12/store-auto), in file-name
  order, REPLACE the listing's phone screenshots (skipped when Play already holds the same files in the same order).
- Icon + feature graphic: handoff/PLAN/app-integration/store/graphics/{app-icon-512,feature-graphic-1024x500}.png,
  uploaded only when Play holds none or a different file (sha256).
- Promo video: only with --video-url (a YouTube URL); otherwise the listing's video is left as it is.
Languages: the listing languages Play already has among en-CA / en-US, or --language. Any asset that breaks Play's rules
is refused by name, never resized. Any API error exits non-zero. Shares the key and session with play-upload.py.
"""
import argparse
import hashlib
import os
import re
import sys
from pathlib import Path

from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account
from PIL import Image

API = "https://androidpublisher.googleapis.com/androidpublisher/v3/applications"
UPLOAD = "https://androidpublisher.googleapis.com/upload/androidpublisher/v3/applications"
SCOPE = ["https://www.googleapis.com/auth/androidpublisher"]
ROOT = Path(__file__).resolve().parents[2]
GRAPHICS = ROOT / "handoff/PLAN/app-integration/store/graphics"
DESCRIPTION = ROOT / "handoff/PLAN/play/store-description.md"
TITLE = "Can I Fish This?"
LIMITS = {"title": 30, "shortDescription": 80, "fullDescription": 4000}
CANDIDATE_LANGUAGES = ("en-CA", "en-US")
MIME = {"PNG": "image/png", "JPEG": "image/jpeg"}


def die(msg, r=None):
    detail = f" HTTP {r.status_code}: {r.text[:1500]}" if r is not None else ""
    sys.exit(f"play-listing-push: {msg}{detail}")


def ok(r, what):
    if r.status_code >= 300:
        die(what, r)
    return r.json() if r.content else {}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_text() -> dict:
    """The two sections of store-description.md; Play counts characters (code points), newlines included."""
    if not DESCRIPTION.is_file():
        die(f"no description file at {DESCRIPTION}")
    md = DESCRIPTION.read_text(encoding="utf-8")
    short = re.search(r"^## Short description\s*\n(.*?)(?=^## )", md, re.S | re.M)
    full = re.search(r"^## Long description\s*\n(.*)\Z", md, re.S | re.M)
    if not short or not full:
        die(f"{DESCRIPTION.name}: needs a '## Short description' section followed by '## Long description'")
    text = {"title": TITLE, "shortDescription": short.group(1).strip(), "fullDescription": full.group(1).strip()}
    for field, limit in LIMITS.items():
        if not text[field]:
            die(f"{field} is empty")
        if len(text[field]) > limit:
            die(f"{field} is {len(text[field])} characters; Play allows {limit}")
    return text


def check_image(kind: str, path: Path) -> str:
    """Play's rules for each image type; returns the MIME type. A wrong file is refused, never resized."""
    if not path.is_file():
        die(f"{kind}: no file at {path}")
    with Image.open(path) as im:
        fmt, (w, h), bands = im.format, im.size, im.getbands()
        alpha = "A" in bands or "transparency" in im.info
    if kind == "icon":
        if fmt != "PNG" or (w, h) != (512, 512):
            die(f"icon: {path.name} is {fmt} {w}x{h}; Play wants a 512x512 PNG")
    elif kind == "featureGraphic":
        if fmt not in MIME or (w, h) != (1024, 500) or alpha:
            die(f"featureGraphic: {path.name} is {fmt} {w}x{h}{' with alpha' if alpha else ''}; Play wants a "
                "1024x500 PNG/JPEG without alpha")
    else:  # phoneScreenshots
        if fmt not in MIME:
            die(f"screenshot {path.name} is {fmt}; Play takes PNG or JPEG")
        if not (320 <= min(w, h) and max(w, h) <= 3840):
            die(f"screenshot {path.name} is {w}x{h}; each side must be 320-3840 px")
        if max(w, h) > 2 * min(w, h):
            die(f"screenshot {path.name} is {w}x{h}; the long side may be at most twice the short side")
        if path.stat().st_size > 8 * 1024 * 1024:
            die(f"screenshot {path.name} is over 8 MB")
    return MIME[fmt]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--package", default="ca.canifishthis.app")
    ap.add_argument("--language", action="append", help="listing language(s); default: those Play has of en-CA/en-US")
    ap.add_argument("--shots", type=Path, default=Path.home() / "bcfish-session-backup-2026-11-12/store-auto")
    ap.add_argument("--icon", type=Path, default=GRAPHICS / "app-icon-512.png")
    ap.add_argument("--feature", type=Path, default=GRAPHICS / "feature-graphic-1024x500.png")
    ap.add_argument("--video-url")
    ap.add_argument("--key", default=os.path.expanduser("~/.canifishthis/play-service-account.json"))
    ap.add_argument("--commit", action="store_true", help="commit the edit (default: validate, then delete it)")
    a = ap.parse_args()

    if a.video_url and not re.match(r"^https://(www\.)?(youtube\.com/watch\?v=|youtu\.be/)[\w-]+", a.video_url):
        die(f"--video-url {a.video_url!r} is not a YouTube video URL")
    text = read_text()
    shots = sorted(p for p in a.shots.glob("*") if p.suffix.lower() in (".png", ".jpg", ".jpeg"))
    if not 2 <= len(shots) <= 8:
        die(f"{a.shots} holds {len(shots)} screenshots; Play takes 2-8 phone screenshots")
    images = {"icon": [a.icon], "featureGraphic": [a.feature], "phoneScreenshots": shots}
    mime = {p: check_image(kind, p) for kind, paths in images.items() for p in paths}
    print(f"assets fit Play's rules: {len(shots)} screenshots, icon, feature graphic; text "
          f"{len(text['shortDescription'])}/80 short, {len(text['fullDescription'])}/4000 full")

    if not os.path.isfile(a.key):
        die(f"no service-account key at {a.key} (owner step: create it in Google Cloud, grant it in Play Console)")
    s = AuthorizedSession(service_account.Credentials.from_service_account_file(a.key, scopes=SCOPE))
    base = f"{API}/{a.package}/edits"
    edit = ok(s.post(base, json={}), "edits.insert")["id"]
    e = f"{base}/{edit}"
    try:
        details = ok(s.get(f"{e}/details"), "details.get")
        listings = {l["language"]: l for l in ok(s.get(f"{e}/listings"), "listings.list").get("listings", [])}
        print(f"\nON PLAY NOW: default language {details.get('defaultLanguage')!r}; listings: "
              f"{', '.join(listings) or 'none'}")
        for lang, l in listings.items():
            print(f"  [{lang}] title={l.get('title', '')!r} short={len(l.get('shortDescription', ''))} chars "
                  f"full={len(l.get('fullDescription', ''))} chars video={l.get('video', '') or 'none'}")
            for kind in images:
                got = ok(s.get(f"{e}/listings/{lang}/{kind}"), f"images.list {lang} {kind}").get("images", [])
                listings[lang].setdefault("_images", {})[kind] = [i.get("sha256", "") for i in got]
                print(f"    {kind}: {len(got)}")

        langs = a.language or [l for l in CANDIDATE_LANGUAGES if l in listings]
        if not langs:
            die(f"Play has no en-CA/en-US listing (has: {', '.join(listings) or 'none'}; default language "
                f"{details.get('defaultLanguage')!r}); name one with --language")

        print(f"\nWOULD CHANGE{'' if a.commit else ' (dry run)'}:")
        for lang in langs:
            cur = listings.get(lang, {})
            body = dict(text)
            if a.video_url:
                body["video"] = a.video_url
            changed = [f for f in body if cur.get(f, "") != body[f]]
            ok(s.patch(f"{e}/listings/{lang}", json=body), f"listings.patch {lang}")
            print(f"  [{lang}] text: {', '.join(changed) if changed else 'unchanged'}")
            have = cur.get("_images", {})
            for kind, paths in images.items():
                want = [sha256(p) for p in paths]
                if have.get(kind, []) == want:
                    print(f"  [{lang}] {kind}: unchanged ({len(want)})")
                    continue
                ok(s.delete(f"{e}/listings/{lang}/{kind}"), f"images.deleteall {lang} {kind}")
                for p in paths:
                    r = s.post(f"{UPLOAD}/{a.package}/edits/{edit}/listings/{lang}/{kind}",
                               params={"uploadType": "media"}, data=p.read_bytes(),
                               headers={"Content-Type": mime[p]}, timeout=300)
                    ok(r, f"images.upload {lang} {kind} {p.name}")
                print(f"  [{lang}] {kind}: {len(have.get(kind, []))} -> {len(paths)} "
                      f"({', '.join(p.name for p in paths)})")

        ok(s.post(f"{e}:validate"), "edits.validate")
        print("\nvalidate: passed")
        if a.commit:
            ok(s.post(f"{e}:commit"), "edits.commit")
            print(f"committed: listing {', '.join(langs)} on {a.package}")
            edit = None
    finally:
        if edit is not None:
            r = s.delete(e)
            if r.status_code >= 300:
                die(f"edits.delete {edit} (the edit is uncommitted; Play expires it)", r)
            print(f"edit {edit} deleted: nothing changed on Play")


if __name__ == "__main__":
    main()
