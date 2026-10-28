"""Tell the OWNER, once, that an upstream update is due (owner 2026-10-22 late).

    FEED_NOTIFY_URL=https://ntfy.sh/<your-topic> python -m pipeline.gauges.feed.publish --out ... --clim ...

The feed writes its due owner tasks into index.json as `maintenance` notices (`publish.maintenance_notices`). This
sends each one to the owner the FIRST time it appears and never again: a notice is keyed `kind:latest`, and the keys
already sent live in `<feed dir>.notified.json` BESIDE the feed directory — never inside it, since the feed directory
is what gets synced to the public origin. Nothing here ever reaches an end user.

THE URL is the environment variable FEED_NOTIFY_URL. Unset, nothing is sent: one console line says notifications are
off, and the index notice and the publisher's console warning remain. Any URL takes a plain-text POST (ntfy.sh and a
self-hosted ntfy read the body as the message); a Discord webhook (`discord.com/api/webhooks/…`) gets `{"content"}`
and a Slack one (`hooks.slack.com/…`) `{"text"}`, the only shapes they accept.

A FAILED SEND is not recorded, so the next run (30 minutes later) tries again, and it never fails the feed: the
readings matter more than the owner's reminder.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.request
from pathlib import Path
from typing import Callable

ENV = "FEED_NOTIFY_URL"
TIMEOUT = 15

Sender = Callable[[str, str], None]


def key(n: dict) -> str:
    """What a notice is deduped by: its kind and the newest thing it names (a later HYDAT is a new message)."""
    return f"{n['kind']}:{n['latest']}"


def message(n: dict) -> str:
    """The notification's text: one line, what is due and the command that clears it."""
    if n["kind"] == "hydat":
        return f"HYDAT {n['latest']} is out; the gauge feed is on {n['current']}. Run: {n['action']}"
    if n["kind"] == "roster":
        return (f"ECCC's real-time station list differs from {n['current']} ({n['latest']}). "
                f"Run: {n['action']}")
    return f"{n['kind']}: {n['latest']} is due (on {n['current']}). Run: {n['action']}"


def state_path(out: Path) -> Path:
    """The sent-keys file: beside the feed directory, so it is never published with the feed.

    Resolved first: `--out .` would otherwise name `./.notified.json`, INSIDE the published directory."""
    out = Path(out).resolve()
    return out.parent / f"{out.name}.notified.json"


def body(url: str, text: str) -> tuple[bytes, str]:
    """`(payload, content type)` for the webhook at `url`."""
    if "discord.com/api/webhooks/" in url or "discordapp.com/api/webhooks/" in url:
        return json.dumps({"content": text}).encode(), "application/json"
    if "hooks.slack.com/" in url:
        return json.dumps({"text": text}).encode(), "application/json"
    return text.encode("utf-8"), "text/plain; charset=utf-8"


def post(url: str, text: str) -> None:
    """The real sender: one POST; raises on a network error or a non-2xx answer."""
    data, ctype = body(url, text)
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"Content-Type": ctype, "Title": "BC fishing feed: update due"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:     # noqa: S310 — the owner's own configured URL
        if not 200 <= r.status < 300:
            raise OSError(f"HTTP {r.status}")


def _sent(path: Path) -> list[str]:
    try:
        got = json.loads(path.read_text(encoding="utf-8")).get("sent")
        return [str(k) for k in got] if isinstance(got, list) else []
    except Exception:                                           # noqa: BLE001 — missing or unreadable: none sent
        return []


def send_due(notices: list[dict], out: Path, url: str | None = None, sender: Sender | None = None, *,
             key_of: Callable[[dict], str] = key, message_of: Callable[[dict], str] = message) -> dict:
    """Send every notice not sent before. Returns `{sent, failed, off}` (keys); never raises.

    `url` defaults to FEED_NOTIFY_URL; `sender` to `post` (tests inject one). `key_of` / `message_of` let another
    feed (the DFO salmon feed, T211) send its own notices through the same dedupe and webhook shapes.
    """
    url = url if url is not None else os.environ.get(ENV) or None
    if not url:
        if notices:
            print(f"  notify: off ({ENV} unset); {len(notices)} notice{'s' if len(notices) != 1 else ''} "
                  f"in index.json")
        return {"sent": [], "failed": [], "off": True}
    send = sender or post
    path = state_path(out)
    done = _sent(path)
    sent: list[str] = []
    failed: list[str] = []
    for n in notices:
        k = key_of(n)
        if k in done or k in sent:
            continue
        try:
            send(url, message_of(n))
            sent.append(k)
        except Exception as exc:                                # noqa: BLE001 — a reminder never fails the feed
            failed.append(k)
            print(f"  notify: {k} not sent ({exc}); retried next run", file=sys.stderr)
    if sent:
        try:
            path.write_text(json.dumps({"sent": done + sent}), encoding="utf-8")
        except Exception as exc:                                # noqa: BLE001
            print(f"  notify: could not record what was sent ({exc}); it may be sent again", file=sys.stderr)
    return {"sent": sent, "failed": failed, "off": False}
