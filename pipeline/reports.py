"""(T1006) `python -m pipeline reports`: ONE read-only command that lists every new report, from every source.

    python -m pipeline reports [--since YYYY-MM-DD] [--no-save] [--only SOURCE[,SOURCE]] [--json]

Sources (each can fail alone; a failed source is SAID, never skipped):
  worker-prod / worker-staging   the feeds workers' `GET /reports` and `GET /crashes` (Bearer RUN_TOKEN; production's
                                 token is `app/workers/feeds/.run-token.prod`, staging's `.run-token`). GET only.
  testflight                     tester feedback + crash submissions (`deploy/staging/testflight-feedback.mjs`) and the
                                 external review state (`testflight-distribute.mjs --dry-run`, which only reads).
  play                           store reviews: the Android Publisher API `reviews.list` (a GET) with the service
                                 account `play-upload.py` uses, `~/.canifishthis/play-service-account.json`. Google
                                 keeps only the last 7 days of reviews in this list; closed-testing feedback has no API
                                 (Play Console only: the command says so).

Every item is marked with the ticket that cites it (a report id, a crash hash or a TestFlight / Play id written in any
`handoff/PLAN/app-integration/tickets/T*.md`) or NEW. "New" also means newer than the last check: the time of the last
check is kept in `cache/reports-last-check.json` (git-ignored; `--no-save` leaves it alone; `--since` overrides it).
Nothing here writes to a service. A reporter's email is never printed.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
TICKETS = REPO / "handoff" / "PLAN" / "app-integration" / "tickets"
STATE = REPO / "cache" / "reports-last-check.json"
WORKERS = {
    "prod": ("https://feeds.canifishthis.ca", REPO / "app/workers/feeds/.run-token.prod"),
    "staging": ("https://bc-fishing-feeds-staging.horvath-dawson.workers.dev", REPO / "app/workers/feeds/.run-token"),
}
PLAY_PACKAGE = "ca.canifishthis.app"
PLAY_KEY = Path("~/.canifishthis/play-service-account.json").expanduser()
PLAY_SCOPE = ["https://www.googleapis.com/auth/androidpublisher"]
PLAY_REVIEWS = "https://androidpublisher.googleapis.com/androidpublisher/v3/applications/{pkg}/reviews"
SOURCES = ("worker-prod", "worker-staging", "testflight", "play")
DEFAULT_DAYS = 14
TEXT_MAX = 200


class SourceError(Exception):
    """A source could not be read: the reason is printed in its section."""


@dataclass
class Item:
    source: str
    kind: str            # report | crash | feedback | review
    id: str
    when: str            # ISO date or datetime, "" when the source gives none
    summary: str
    cited_in: list[str] = field(default_factory=list)

    @property
    def new(self) -> bool:
        return not self.cited_in


@dataclass
class Section:
    source: str
    items: list[Item] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)      # extra lines (review state, "no API", ...)
    error: str | None = None


# ------------------------------------------------------------------------------------------------ what tickets cite

def ticket_texts(tickets_dir: Path = TICKETS) -> dict[str, str]:
    """{ticket id: text} of every ticket file."""
    out = {}
    for p in sorted(tickets_dir.glob("T*.md")):
        m = re.match(r"T\d+[a-z]?", p.name)
        if m:
            out[m.group(0)] = p.read_text(encoding="utf-8", errors="replace")
    return out


def cited_by(item_id: str, texts: dict[str, str]) -> list[str]:
    """The tickets whose text holds `item_id` (a report id, a crash hash, a feedback id). Ids shorter than 6
    characters are never matched (they would match prose)."""
    if len(item_id) < 6:
        return []
    return [t for t, text in texts.items() if item_id in text]


# ----------------------------------------------------------------------------------------------------- the clock

def parse_when(s: str) -> datetime | None:
    if not s:
        return None
    try:
        d = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def read_state(path: Path = STATE) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def write_state(state: dict, path: Path = STATE) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=1, sort_keys=True) + "\n", encoding="utf-8")


def since_date(arg: str | None, state: dict, now: datetime) -> str:
    """The first day to ask for: `--since`, else the last check's day, else DEFAULT_DAYS back."""
    if arg:
        return arg
    last = parse_when(state.get("last_check", ""))
    return (last or now - timedelta(days=DEFAULT_DAYS)).date().isoformat()


def one_line(text: object, n: int = TEXT_MAX) -> str:
    t = " ".join(str(text or "").split())
    return t if len(t) <= n else t[: n - 1] + "…"


# ---------------------------------------------------------------------------------------------------------- workers

def http_get_json(url: str, token: str, opener=None, timeout: int = 30):
    req = urllib.request.Request(url, method="GET",
                                 headers={"Authorization": f"Bearer {token}", "User-Agent": "pipeline-reports"})
    try:
        with (opener or urllib.request.urlopen)(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise SourceError(f"{url.split('?')[0]} answered HTTP {e.code}") from e
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as e:
        raise SourceError(f"{url.split('?')[0]} could not be read ({e})") from e


def worker_section(env: str, since: str, texts: dict[str, str], opener=None) -> Section:
    base, token_file = WORKERS[env]
    sec = Section(f"worker-{env}")
    try:
        token = token_file.read_text(encoding="utf-8").strip()
    except OSError:
        sec.error = f"no run token at {token_file.relative_to(REPO)}"
        return sec
    if not token:
        sec.error = f"{token_file.relative_to(REPO)} is empty"
        return sec
    q = urllib.parse.urlencode({"since": since})
    problems = []
    try:
        body = http_get_json(f"{base}/reports?{q}", token, opener)
        reps = body.get("reports") if isinstance(body, dict) else None
        if not isinstance(reps, list):
            raise SourceError("/reports holds no report list")
        for r in reps:
            c = r.get("context") or {}
            where = c.get("water") or c.get("place") or ""
            rid = str(r.get("id") or "")
            sec.items.append(Item(sec.source, "report", rid, str(r.get("received") or r.get("created") or ""),
                                  one_line(f"[{r.get('reason', '?')}] {where + ': ' if where else ''}{r.get('text', '')}"),
                                  cited_by(rid, texts)))
    except SourceError as e:
        problems.append(str(e))
    try:
        body = http_get_json(f"{base}/crashes?{q}", token, opener)
        crs = body.get("crashes") if isinstance(body, dict) else None
        if not isinstance(crs, list):
            raise SourceError("/crashes holds no crash list")
        for c in crs:
            h = str(c.get("hash") or "")
            sample = c.get("sample") or {}
            vers = ", ".join(sorted({k.split(" (")[0] for k in (c.get("versions") or {})})[:4])
            sec.items.append(Item(sec.source, "crash", h, str(c.get("last") or ""),
                                  one_line(f"x{c.get('count', '?')} versions {vers or '?'}: "
                                           f"{sample.get('name', '')} {sample.get('message', '')}"),
                                  cited_by(h, texts)))
        counts = body.get("counts") if isinstance(body, dict) else None
        if isinstance(counts, dict) and any(counts.get(k) for k in counts if k != "defect"):
            sec.notes.append("crashes by class (only defects are listed; GET /crashes?class=all for the rest): "
                             + ", ".join(f"{k} {v}" for k, v in sorted(counts.items())))
    except SourceError as e:
        problems.append(str(e))
    if problems:
        sec.error = "; ".join(problems)
    return sec


# ------------------------------------------------------------------------------------------------------ TestFlight

def run_text(cmd: list[str], runner=None, timeout: int = 300) -> str:
    try:
        p = (runner or subprocess.run)(cmd, cwd=REPO, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as e:
        raise SourceError(f"{' '.join(cmd[:3])}: {e}") from e
    if p.returncode != 0:
        raise SourceError(f"{' '.join(cmd[:3])} exited {p.returncode}: {one_line(p.stderr or p.stdout, 160)}")
    return p.stdout


def testflight_section(days: int, texts: dict[str, str], since: datetime, runner=None) -> Section:
    sec = Section("testflight")
    try:
        raw = run_text(["node", "deploy/staging/testflight-feedback.mjs", "--days", str(days), "--json"], runner)
        data = json.loads(raw)
        for kind, key in (("feedback", "screenshots"), ("crash", "crashes")):
            for s in data.get(key) or []:
                a = s.get("attributes") or {}
                when = str(a.get("createdDate") or "")
                d = parse_when(when)
                if d and d < since:
                    continue
                sid = str(s.get("id") or "")
                sec.items.append(Item(sec.source, kind, sid, when,
                                      one_line(f"{a.get('deviceModel', '?')} iOS {a.get('osVersion', '?')}: "
                                               f"{a.get('comment') or '(no comment)'}"), cited_by(sid, texts)))
    except (SourceError, ValueError) as e:
        sec.error = str(e)
    try:
        state = run_text(["node", "deploy/staging/testflight-distribute.mjs", "--dry-run", "--max-wait-min", "1"], runner)
        lines = [ln.strip() for ln in state.splitlines() if "external state" in ln]
        sec.notes.append("external review state: " + (one_line(lines[-1], 240) if lines else "no state line in the output"))
    except SourceError as e:
        sec.notes.append(f"external review state unavailable: {e}")
    return sec


# ---------------------------------------------------------------------------------------------------------- Play

def play_session():
    """An authorised session for the Android Publisher API (the account play-upload.py uses). Imported late: the
    other sources need no Google library."""
    from google.auth.transport.requests import AuthorizedSession
    from google.oauth2 import service_account
    return AuthorizedSession(service_account.Credentials.from_service_account_file(str(PLAY_KEY), scopes=PLAY_SCOPE))


def play_section(texts: dict[str, str], since: datetime, session=None) -> Section:
    sec = Section("play")
    sec.notes.append("Play closed-testing feedback has no API: read it in the Play Console (Testing > Closed testing > "
                     "Feedback); ask the owner if any came in.")
    try:
        if session is None:
            if not PLAY_KEY.exists():
                raise SourceError(f"no service-account key at {PLAY_KEY} (the owner's step; see play-upload.py)")
            session = play_session()
        r = session.get(PLAY_REVIEWS.format(pkg=PLAY_PACKAGE), params={"maxResults": 100}, timeout=30)
        if r.status_code != 200:
            raise SourceError(f"reviews.list answered HTTP {r.status_code}: {one_line(r.text, 160)}"
                              + (" (the service account may need the 'reply to reviews' / view-feedback permission in "
                                 "the Play Console: ask the owner)" if r.status_code in (401, 403) else ""))
        for rv in (r.json().get("reviews") or []):
            uc = ((rv.get("comments") or [{}])[0]).get("userComment") or {}
            secs = (uc.get("lastModified") or {}).get("seconds")
            d = datetime.fromtimestamp(int(secs), timezone.utc) if secs else None
            if d and d < since:
                continue
            rid = str(rv.get("reviewId") or "")
            sec.items.append(Item(sec.source, "review", rid, d.isoformat() if d else "",
                                  one_line(f"{uc.get('starRating', '?')} stars, app {uc.get('appVersionName', '?')}, "
                                           f"{uc.get('deviceMetadata', {}).get('productName', '?')}: {uc.get('text', '')}"),
                                  cited_by(rid, texts)))
        sec.notes.append("reviews.list returns only the last 7 days; run this at least weekly")
    except SourceError as e:
        sec.error = str(e)
    except Exception as e:  # noqa: BLE001 — a Google client error is reported with its type, the other sources go on
        sec.error = f"{type(e).__name__}: {one_line(e, 200)}"
    return sec


# ------------------------------------------------------------------------------------------------------- the report

def collect(only: tuple[str, ...], since: str, days: int, now: datetime, texts: dict[str, str], *, opener=None,
            runner=None, session=None) -> list[Section]:
    since_dt = parse_when(since) or now - timedelta(days=days)
    out = []
    for name in SOURCES:
        if name not in only:
            continue
        if name == "worker-prod":
            out.append(worker_section("prod", since, texts, opener))
        elif name == "worker-staging":
            out.append(worker_section("staging", since, texts, opener))
        elif name == "testflight":
            out.append(testflight_section(max(1, (now - since_dt).days + 1), texts, since_dt, runner))
        else:
            out.append(play_section(texts, since_dt, session))
    return out


def render(sections: list[Section], since: str) -> str:
    lines = [f"reports since {since} (read-only; NEW = no ticket cites the id)"]
    total_new = 0
    for s in sections:
        new = [i for i in s.items if i.new]
        total_new += len(new)
        head = f"== {s.source}: {len(s.items)} item(s), {len(new)} NEW"
        lines.append(head + (f"  [UNREAD: {s.error}]" if s.error else ""))
        for i in sorted(s.items, key=lambda i: i.when, reverse=True):
            mark = "NEW " if i.new else f"{','.join(i.cited_in[:3])} "
            lines.append(f"  {mark}{i.kind} {i.id} {i.when[:19]} {i.summary}")
        lines += [f"  note: {n}" for n in s.notes]
    unread = [s.source for s in sections if s.error]
    lines.append(f"-- {total_new} NEW item(s)" + (f"; UNREAD sources: {', '.join(unread)}" if unread else "; every source read"))
    lines.append("Ticket anything NEW (write its id in the ticket), then add a 'Store checks' line to OWNER-TODO.md.")
    return "\n".join(lines)


def main(argv: list[str] | None = None, *, now: datetime | None = None, opener=None, runner=None, session=None,
         tickets_dir: Path = TICKETS, state_path: Path = STATE, out=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline reports", description=__doc__.split("\n\n")[0])
    ap.add_argument("--since", help="YYYY-MM-DD (default: the last check, else 14 days back)")
    ap.add_argument("--only", help=f"comma list of {', '.join(SOURCES)}")
    ap.add_argument("--no-save", action="store_true", help="do not move the last-check time")
    ap.add_argument("--json", action="store_true", help="print JSON instead of text")
    a = ap.parse_args(argv)
    only = tuple(x.strip() for x in a.only.split(",")) if a.only else SOURCES
    bad = [x for x in only if x not in SOURCES]
    if bad:
        print(f"unknown source(s) {bad}; known: {', '.join(SOURCES)}", file=sys.stderr)
        return 2
    now = now or datetime.now(timezone.utc)
    state = read_state(state_path)
    since = since_date(a.since, state, now)
    sections = collect(only, since, DEFAULT_DAYS, now, ticket_texts(tickets_dir), opener=opener, runner=runner,
                       session=session)
    if a.json:
        text = json.dumps([{"source": s.source, "error": s.error, "notes": s.notes,
                            "items": [i.__dict__ for i in s.items]} for s in sections], indent=1)
    else:
        text = render(sections, since)
    print(text, file=out or sys.stdout)
    # the check moves forward only when every asked source was read, so an unread source is asked again
    if not a.no_save and not a.since and not any(s.error for s in sections) and set(only) == set(SOURCES):
        write_state({"last_check": now.isoformat()}, state_path)
    return 1 if any(s.error for s in sections) else 0
