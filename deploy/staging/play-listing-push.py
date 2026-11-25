#!/usr/bin/env python3
"""Push the store listing (text, phone screenshots, icon, feature graphic, promo video URL) to Google Play.

    play-listing-push.py [--package ca.canifishthis.app] [--language en-US ...] [--shots DIR] [--video-url URL]
                         [--tablet7 DIR] [--tablet10 DIR] [--key ~/.canifishthis/play-service-account.json]
                         [--plan | --commit]

--plan (T319) sends NOTHING: it checks every file and the text and prints what would go to which listing, in order.
Default is a DRY RUN: open an edit, apply everything (the images are uploaded INTO the edit), ask Play to VALIDATE it,
print what would change, then DELETE the edit; nothing reaches the listing. --commit does the same and commits the edit.

- Text (T1072): title, short and full descriptions from the one source, app/store-listing/listing.json (+ the
  full-description.txt it names). Over 30 / 80 / 4000 characters is refused before anything is sent.
--appstore (T1072) prints each App Store Connect field from the same source with its length and limit, to paste; no network.
- Phone screenshots: the pictures in --shots (default: the repo's one set, STORE/play-phone), in file-name
  order, REPLACE the listing's phone screenshots (skipped when Play already holds the same files in the same order).
- --no-shots (T319): text, icon and feature graphic only; every screenshot set on Play is left as it is.
- Tablet screenshots (T319, T1072): --tablet7 / --tablet10 folders, the same way (7-inch and 10-inch sets); the defaults are
  the repo's play-tablet7 / play-tablet10, the same committed set the App Store push reads (store-shots.mjs writes them);
  --no-shots leaves all three sets alone.
- (T1127) The pictures in each of the three folders must be EXACTLY the list app/tools/store-shots.json implies
  (deploy/staging/store_shots_list.py): a leftover picture or a missing one stops the run by name, in every mode, before
  any network call. Plan mode prints the count and names per size.
- Icon + feature graphic: handoff/PLAN/app-integration/store/graphics/{app-icon-512,feature-graphic-1024x500}.png,
  uploaded only when Play holds none or a different file (sha256).
- Promo video: only with --video-url (a YouTube URL); otherwise the listing's video is left as it is.
Languages: the listing languages Play already has among en-CA / en-US, or --language. Any asset that breaks Play's rules
is refused by name, never resized. Any API error exits non-zero. Shares the key and session with play-upload.py.
"""
import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path

from PIL import Image

from store_shots_list import pictures

API = "https://androidpublisher.googleapis.com/androidpublisher/v3/applications"
UPLOAD = "https://androidpublisher.googleapis.com/upload/androidpublisher/v3/applications"
SCOPE = ["https://www.googleapis.com/auth/androidpublisher"]
ROOT = Path(__file__).resolve().parents[2]
GRAPHICS = ROOT / "handoff/PLAN/app-integration/store/graphics"
SET = GRAPHICS / "store/light"   # the one committed store set (app/tools/store-shots.mjs writes it; the App Store push reads it too)
LISTING = ROOT / "app/store-listing/listing.json"
DESCRIPTION = LISTING
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


def read_listing() -> dict:
    """The one source of the store words (app/store-listing/listing.json); the full description is the file it names."""
    if not LISTING.is_file():
        die(f"no listing source at {LISTING}")
    src = json.loads(LISTING.read_text(encoding="utf-8"))
    src["fullDescription"] = (LISTING.parent / src["fullDescriptionFile"]).read_text(encoding="utf-8").strip()
    return src


def read_text() -> dict:
    """The Play text; Play counts characters (code points), newlines included."""
    src = read_listing()
    text = {"title": src["name"], "shortDescription": src["playShortDescription"], "fullDescription": src["fullDescription"]}
    for field, limit in LIMITS.items():
        if not text[field]:
            die(f"{field} is empty")
        if len(text[field]) > limit:
            die(f"{field} is {len(text[field])} characters; Play allows {limit}")
    return text


def print_appstore() -> None:
    """Every App Store Connect text field, its length and limit; nothing is sent (the owner pastes)."""
    src = read_listing()
    lim = src["limits"]
    rows = [("Name", src["name"], lim["name"]), ("Subtitle", src["appStoreSubtitle"], lim["appStoreSubtitle"]),
            ("Promotional text", src["appStorePromotionalText"], lim["appStorePromotionalText"]),
            ("Description", src["fullDescription"], lim["fullDescription"]),
            ("Keywords", src["appStoreKeywords"], lim["appStoreKeywords"]),
            ("What's New (first version, proposal)", src["whatsNew"]["appStoreFirstVersionProposal"], src["whatsNew"]["appStoreLimit"]),
            ("Support URL", src["urls"]["support"], None), ("Marketing URL", src["urls"]["marketing"], None),
            ("Privacy policy URL", src["urls"]["privacy"], None)]
    for field, value, limit in rows:
        n = len(value.encode("utf-16-le")) // 2
        print(f"=== {field}: {n}{'/' + str(limit) if limit else ''} characters {'(OVER)' if limit and n > limit else ''}\n{value}\n")
    c = src["categories"]["appStore"]
    print(f"=== Category: {c['primary']}, secondary {c['secondary']}")
    print("nothing sent: this prints the one source (app/store-listing/listing.json) for App Store Connect.")


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
    ap.add_argument("--shots", type=Path, default=SET / "play-phone", help="phone screenshots (default: the repo's committed set)")
    ap.add_argument("--icon", type=Path, default=GRAPHICS / "app-icon-512.png")
    ap.add_argument("--feature", type=Path, default=GRAPHICS / "feature-graphic-1024x500.png")
    ap.add_argument("--video-url")
    ap.add_argument("--key", default=os.path.expanduser("~/.canifishthis/play-service-account.json"))
    ap.add_argument("--tablet7", type=Path, default=SET / "play-tablet7", help="7-inch tablet screenshots (default: the repo's committed set)")
    ap.add_argument("--tablet10", type=Path, default=SET / "play-tablet10", help="10-inch tablet screenshots (default: the repo's committed set)")
    ap.add_argument("--no-shots", action="store_true", help="text, icon and feature graphic only; screenshots left as they are")
    ap.add_argument("--appstore", action="store_true", help="print the App Store Connect fields with length/limit; sends nothing")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--commit", action="store_true", help="commit the edit (default: validate, then delete it)")
    mode.add_argument("--plan", action="store_true", help="send nothing: check the files, print what would be sent")
    a = ap.parse_args()
    if a.appstore:
        return print_appstore()

    if a.video_url and not re.match(r"^https://(www\.)?(youtube\.com/watch\?v=|youtu\.be/)[\w-]+", a.video_url):
        die(f"--video-url {a.video_url!r} is not a YouTube video URL")
    text = read_text()
    try:   # (T1127) each size folder must hold exactly store-shots.json's list: a leftover or a gap stops the run, by name
        shots = [] if a.no_shots else pictures(a.shots, "play-phone")
        tablets = {} if a.no_shots else {k: pictures(d, t) for k, d, t in (
            ("sevenInchScreenshots", a.tablet7, "play-tablet7"), ("tenInchScreenshots", a.tablet10, "play-tablet10")) if d}
    except ValueError as e:
        die(str(e))
    if not a.no_shots and not 2 <= len(shots) <= 8:
        die(f"{a.shots} holds {len(shots)} screenshots; Play takes 2-8 phone screenshots")
    if a.no_shots:
        a.tablet7 = a.tablet10 = None   # --no-shots leaves every screenshot set alone, the tablet defaults included
    images = {"icon": [a.icon], "featureGraphic": [a.feature]}
    if not a.no_shots:
        images["phoneScreenshots"] = shots
    for kind, tab in tablets.items():
        if not 1 <= len(tab) <= 8:
            die(f"{kind}: {len(tab)} screenshots; Play takes up to 8 per tablet size")
        images[kind] = tab
    mime = {p: check_image(kind, p) for kind, paths in images.items() for p in paths}
    print(f"assets fit Play's rules: {len(shots)} screenshots, icon, feature graphic; text "
          f"{len(text['shortDescription'])}/80 short, {len(text['fullDescription'])}/4000 full")
    if a.plan:
        langs = a.language or ["en-CA and/or en-US: whichever listings Play has (read at run time)"]
        print(f"\nPLAN (nothing sent): package {a.package}, listing {', '.join(langs)}")
        print(f"  text: title {text['title']!r}, short + full description from {DESCRIPTION.relative_to(ROOT)}"
              " (sent only where Play's differ)")
        print(f"  promo video: {a.video_url} (the listing's video field)" if a.video_url else
              "  promo video: none given, the listing's is left as it is (Play takes a YouTube URL: --video-url)")
        for kind, paths in images.items():
            print(f"  {kind}: {len(paths)} (replaces Play's when the files or their order differ)")
            print(f"    names: {', '.join(p.name for p in paths)}")
            for i, p in enumerate(paths, 1):
                with Image.open(p) as im:
                    w, h = im.size
                print(f"    {i}. {p.relative_to(ROOT) if p.is_relative_to(ROOT) else p}  {w}x{h}  {p.stat().st_size // 1024} KB")
        print("plan only: nothing sent. Without --plan: a validated dry run (uploads into an edit, then deletes it); --commit pushes.")
        return

    if not os.path.isfile(a.key):
        die(f"no service-account key at {a.key} (owner step: create it in Google Cloud, grant it in Play Console)")
    # (T1127) the Google client is imported only here, on the network path: plan mode and the picture-list refusal
    # above run without it (CI has no google.auth).
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2 import service_account
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
