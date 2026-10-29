#!/usr/bin/env python3
"""(T267) Upload an AAB to a Google Play track (default: internal) through the Android Publisher API v3.

    play-upload.py --aab app.aab --package ca.canifishthis.app [--track internal] [--notes-file notes.txt]
                   [--key ~/.canifishthis/play-service-account.json] [--status completed|draft] [--max-version-code]

One edit: insert -> bundles.upload -> tracks.update (this versionCode only, release notes en-US, <= 500 chars) -> commit.
A draft app (store listing not yet complete) refuses "completed" releases; with --status completed (the default) a refusal
that says so is retried once as "draft", which the owner then rolls out in Play Console.
--max-version-code prints the highest versionCode Play holds on any track (0 when none) and exits; build-apk.sh uses it so
a bump always grows past what Play has seen. Needs google-auth + requests (the repo .venv has both).
The service-account JSON is a secret: it lives in ~/.canifishthis/, never in the repo.
"""
import argparse
import os
import sys

from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

API = "https://androidpublisher.googleapis.com/androidpublisher/v3/applications"
UPLOAD = "https://androidpublisher.googleapis.com/upload/androidpublisher/v3/applications"
SCOPE = ["https://www.googleapis.com/auth/androidpublisher"]


def die(msg, r=None):
    detail = f" HTTP {r.status_code}: {r.text[:800]}" if r is not None else ""
    sys.exit(f"play-upload: {msg}{detail}")


def ok(r, what):
    if r.status_code >= 300:
        die(what, r)
    return r.json() if r.content else {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--aab")
    ap.add_argument("--package", required=True)
    ap.add_argument("--track", default="internal")
    ap.add_argument("--notes-file")
    ap.add_argument("--status", default="completed", choices=["completed", "draft"])
    ap.add_argument("--key", default=os.path.expanduser("~/.canifishthis/play-service-account.json"))
    ap.add_argument("--max-version-code", action="store_true")
    a = ap.parse_args()
    if not os.path.isfile(a.key):
        die(f"no service-account key at {a.key} (owner step: create it in Google Cloud, grant it in Play Console)")
    creds = service_account.Credentials.from_service_account_file(a.key, scopes=SCOPE)
    s = AuthorizedSession(creds)
    base = f"{API}/{a.package}/edits"
    edit = ok(s.post(base, json={}), "edits.insert")["id"]

    if a.max_version_code:
        tracks = ok(s.get(f"{base}/{edit}/tracks"), "tracks.list").get("tracks", [])
        codes = [int(c) for t in tracks for rel in t.get("releases", []) for c in rel.get("versionCodes", [])]
        bundles = ok(s.get(f"{base}/{edit}/bundles"), "bundles.list").get("bundles", [])
        codes += [int(b["versionCode"]) for b in bundles]
        s.delete(f"{base}/{edit}")
        print(max(codes or [0]))
        return

    if not a.aab or not os.path.isfile(a.aab):
        die(f"no AAB at {a.aab}")
    with open(a.aab, "rb") as f:
        r = s.post(f"{UPLOAD}/{a.package}/edits/{edit}/bundles", params={"uploadType": "media"},
                   data=f, headers={"Content-Type": "application/octet-stream"}, timeout=900)
    code = ok(r, "bundles.upload")["versionCode"]
    print(f"uploaded versionCode {code}")
    notes = open(a.notes_file).read().strip()[:500] if a.notes_file else ""

    def release(status):
        rel = {"versionCodes": [str(code)], "status": status}
        if notes:
            rel["releaseNotes"] = [{"language": "en-US", "text": notes}]
        return s.put(f"{base}/{edit}/tracks/{a.track}", json={"track": a.track, "releases": [rel]})

    status = a.status
    r = release(status)
    if r.status_code >= 300 and status == "completed" and "draft" in r.text.lower():
        print("play-upload: Play accepts only draft releases for this app (listing not complete); retrying as draft")
        status = "draft"
        r = release(status)
    ok(r, f"tracks.update {a.track}")
    r = s.post(f"{base}/{edit}:commit")
    if r.status_code >= 300 and status == "completed" and "draft" in r.text.lower():
        die("commit refused a completed release on a draft app; rerun with --status draft", r)
    ok(r, "edits.commit")
    print(f"committed: versionCode {code} on track '{a.track}' as {status}")


if __name__ == "__main__":
    main()
