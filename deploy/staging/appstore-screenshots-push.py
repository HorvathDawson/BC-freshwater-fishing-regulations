#!/usr/bin/env python3
"""Put the store screenshots on the App Store listing (App Store Connect API: appScreenshotSets / appScreenshots). T319.

    appstore-screenshots-push.py [--shots ROOT] [--locale en-CA] [--bundle-id ca.canifishthis.app]
                                 [--key ~/.canifishthis/asc-api-key.json] [--check | --commit]

Three modes, the default sends nothing:
- (default) PLAN: no network. Checks every file against Apple's rules and prints what would be sent where, in order.
- --check: read-only. Signs in, finds the app, its editable iOS version and the locale's localization, and prints what
  each screenshot set holds now against what would replace it. No write.
- --commit: the real upload. For each display type whose files differ (MD5, in order): every screenshot in the set is
  deleted, the new ones are reserved, uploaded in the parts Apple names, committed with their MD5, waited on until Apple
  has processed them, and put in file-name order.

Sets (folders under ROOT, default handoff/PLAN/app-integration/store/graphics/store/light, as app/tools/store-shots.mjs
writes them): appstore-6.9 -> APP_IPHONE_67 (Apple's 6.9" slot, 1320x2868 / 1290x2796 / 1260x2736) and appstore-6.5 ->
APP_IPHONE_65 (1242x2688 / 1284x2778). No iPad set: app.json has supportsTablet false. Each set takes 1-10 PNG/JPEG files
without alpha; a wrong one is refused by name, never resized. The locale must already exist on the version (the app's
primary language is English (Canada), en-CA); it is never created here.
Key: asc-api-key.json {"key_id", "issuer_id", "key_path"} as build-ios-testflight.sh reads it (never printed). Needs
requests, cryptography and Pillow (the repo .venv has them).
"""
import argparse
import base64
import hashlib
import json
import os
import sys
import time
from pathlib import Path

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature
from PIL import Image

API = "https://api.appstoreconnect.apple.com/v1"
ROOT = Path(__file__).resolve().parents[2]
SHOTS = ROOT / "handoff/PLAN/app-integration/store/graphics/store/light"
#: folder -> (display type, the sizes Apple takes)
SETS = {
    "appstore-6.9": ("APP_IPHONE_67", {(1320, 2868), (1290, 2796), (1260, 2736)}),
    "appstore-6.5": ("APP_IPHONE_65", {(1242, 2688), (1284, 2778)}),
}
EDITABLE = "PREPARE_FOR_SUBMISSION,DEVELOPER_REJECTED,REJECTED,METADATA_REJECTED,INVALID_BINARY"


def die(msg, r=None):
    detail = f" HTTP {r.status_code}: {r.text[:1500]}" if r is not None else ""
    sys.exit(f"appstore-screenshots-push: {msg}{detail}")


def md5(p: Path) -> str:
    return hashlib.md5(p.read_bytes()).hexdigest()


def check(folder: str, files: list, sizes: set) -> None:
    if not 1 <= len(files) <= 10:
        die(f"{folder}: {len(files)} screenshots; Apple takes 1-10 per display type")
    for p in files:
        with Image.open(p) as im:
            fmt, size, alpha = im.format, im.size, "A" in im.getbands() or "transparency" in im.info
        if fmt not in ("PNG", "JPEG"):
            die(f"{folder}/{p.name} is {fmt}; Apple takes PNG or JPEG")
        if size not in sizes:
            die(f"{folder}/{p.name} is {size[0]}x{size[1]}; this slot takes {', '.join(f'{w}x{h}' for w, h in sorted(sizes))}")
        if alpha:
            die(f"{folder}/{p.name} has an alpha channel; Apple refuses screenshots with transparency")


def token(key_file: str) -> str:
    """An App Store Connect API token (ES256 JWT, 20 minutes)."""
    k = json.loads(Path(key_file).read_text())
    pk = serialization.load_pem_private_key(Path(k["key_path"]).read_bytes(), password=None)
    b64 = lambda b: base64.urlsafe_b64encode(b).rstrip(b"=").decode()
    head = b64(json.dumps({"alg": "ES256", "kid": k["key_id"], "typ": "JWT"}).encode())
    now = int(time.time())
    body = b64(json.dumps({"iss": k["issuer_id"], "iat": now, "exp": now + 1200, "aud": "appstoreconnect-v1"}).encode())
    r, s = decode_dss_signature(pk.sign(f"{head}.{body}".encode(), ec.ECDSA(hashes.SHA256())))
    return f"{head}.{body}.{b64(r.to_bytes(32, 'big') + s.to_bytes(32, 'big'))}"


class Asc:
    def __init__(self, key_file):
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"Bearer {token(key_file)}"

    def call(self, method, path, what, **kw):
        r = self.s.request(method, path if path.startswith("http") else f"{API}{path}", timeout=120, **kw)
        if r.status_code >= 300:
            die(what, r)
        return r.json() if r.content else {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--shots", type=Path, default=SHOTS)
    ap.add_argument("--locale", default="en-CA")
    ap.add_argument("--bundle-id", default="ca.canifishthis.app")
    ap.add_argument("--key", default=os.path.expanduser("~/.canifishthis/asc-api-key.json"))
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true", help="read-only: compare with what App Store Connect holds")
    mode.add_argument("--commit", action="store_true", help="upload (replaces each changed set)")
    a = ap.parse_args()

    want = {}
    for folder, (kind, sizes) in SETS.items():
        d = a.shots / folder
        files = sorted(p for p in d.glob("*") if p.suffix.lower() in (".png", ".jpg", ".jpeg")) if d.is_dir() else []
        check(folder, files, sizes)
        want[kind] = files
    mode_name = "COMMIT" if a.commit else "CHECK (read-only)" if a.check else "PLAN (no network)"
    print(f"appstore-screenshots-push: {mode_name}")
    print(f"  app {a.bundle_id}, its editable iOS version, localization {a.locale}")
    for kind, files in want.items():
        print(f"  {kind}: {len(files)} screenshots, replace the set in this order:")
        for i, p in enumerate(files, 1):
            with Image.open(p) as im:
                w, h = im.size
            print(f"    {i}. {p.relative_to(ROOT) if p.is_relative_to(ROOT) else p}  {w}x{h}  {p.stat().st_size // 1024} KB  md5 {md5(p)[:12]}")
    if not (a.check or a.commit):
        print("plan only: nothing sent. --check compares with App Store Connect (read-only); --commit uploads.")
        return

    if not os.path.isfile(a.key):
        die(f"no API key description at {a.key} (build-ios-testflight.sh's header says what it holds)")
    asc = Asc(a.key)
    apps = asc.call("GET", "/apps", "apps.list", params={"filter[bundleId]": a.bundle_id}).get("data", [])
    if not apps:
        die(f"no app with bundle id {a.bundle_id} on App Store Connect")
    app = apps[0]["id"]
    vers = asc.call("GET", f"/apps/{app}/appStoreVersions", "appStoreVersions.list",
                    params={"filter[platform]": "IOS", "filter[appStoreState]": EDITABLE}).get("data", [])
    if not vers:
        die("no editable iOS version (Prepare for Submission or rejected): add one in App Store Connect first")
    ver = vers[0]
    print(f"\nON APP STORE CONNECT: version {ver['attributes'].get('versionString')} ({ver['attributes'].get('appStoreState')})")
    locs = asc.call("GET", f"/appStoreVersions/{ver['id']}/appStoreVersionLocalizations", "localizations.list").get("data", [])
    loc = next((l for l in locs if l["attributes"]["locale"] == a.locale), None)
    if not loc:
        die(f"the version has no {a.locale} localization (has: {', '.join(l['attributes']['locale'] for l in locs)}); --locale")
    sets = {s["attributes"]["screenshotDisplayType"]: s for s in
            asc.call("GET", f"/appStoreVersionLocalizations/{loc['id']}/appScreenshotSets", "appScreenshotSets.list").get("data", [])}

    for kind, files in want.items():
        s = sets.get(kind)
        have = asc.call("GET", f"/appScreenshotSets/{s['id']}/appScreenshots", f"appScreenshots.list {kind}").get("data", []) if s else []
        same = [h["attributes"].get("sourceFileChecksum") for h in have] == [md5(p) for p in files]
        print(f"  [{a.locale}] {kind}: holds {len(have)}{' (no set yet)' if not s else ''} -> "
              f"{'unchanged' if same else f'replace with {len(files)}'}")
        if same or not a.commit:
            continue
        if not s:
            s = asc.call("POST", "/appScreenshotSets", f"appScreenshotSets.create {kind}", json={"data": {
                "type": "appScreenshotSets", "attributes": {"screenshotDisplayType": kind},
                "relationships": {"appStoreVersionLocalization": {"data": {"type": "appStoreVersionLocalizations", "id": loc["id"]}}}}})["data"]
        for h in have:
            asc.call("DELETE", f"/appScreenshots/{h['id']}", f"appScreenshots.delete {h['id']}")
        ids = []
        for p in files:
            data = p.read_bytes()
            res = asc.call("POST", "/appScreenshots", f"appScreenshots.create {p.name}", json={"data": {
                "type": "appScreenshots", "attributes": {"fileName": p.name, "fileSize": len(data)},
                "relationships": {"appScreenshotSet": {"data": {"type": "appScreenshotSets", "id": s["id"]}}}}})["data"]
            for op in res["attributes"]["uploadOperations"]:
                part = data[op["offset"]:op["offset"] + op["length"]]
                r = requests.request(op["method"], op["url"], data=part, timeout=300,
                                     headers={h["name"]: h["value"] for h in op.get("requestHeaders", [])})
                if r.status_code >= 300:
                    die(f"upload part {op['offset']} of {p.name}", r)
            asc.call("PATCH", f"/appScreenshots/{res['id']}", f"appScreenshots.commit {p.name}", json={"data": {
                "type": "appScreenshots", "id": res["id"], "attributes": {"uploaded": True, "sourceFileChecksum": md5(p)}}})
            ids.append(res["id"])
            print(f"    uploaded {p.name}")
        for sid in ids:   # Apple processes each upload; a FAILED one is said with Apple's reason
            for _ in range(60):
                st = asc.call("GET", f"/appScreenshots/{sid}", "appScreenshots.get")["data"]["attributes"].get("assetDeliveryState") or {}
                if st.get("state") in ("COMPLETE", "FAILED"):
                    break
                time.sleep(5)
            if st.get("state") != "COMPLETE":
                die(f"screenshot {sid} is {st.get('state')}: {st.get('errors')}")
        asc.call("PATCH", f"/appScreenshotSets/{s['id']}/relationships/appScreenshots", f"appScreenshotSets.order {kind}",
                 json={"data": [{"type": "appScreenshots", "id": i} for i in ids]})
        print(f"  [{a.locale}] {kind}: {len(ids)} screenshots up, in order")
    print("committed" if a.commit else "check only: nothing changed")


if __name__ == "__main__":
    main()
