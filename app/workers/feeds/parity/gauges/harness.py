"""T225 parity harness for the gauge feed: record every source response once, then replay the PYTHON publisher
against the recording with a frozen clock, so the TypeScript port (`src/gauges/`) can be diffed against it.

Run from the repo root with the repo venv:

    .venv/bin/python app/workers/feeds/parity/gauges/harness.py record --fixtures DIR --seed SEED
    .venv/bin/python app/workers/feeds/parity/gauges/harness.py replay --fixtures DIR --out OUT [--notify URL]

FIXTURE DIR: `manifest.json` ({now, urls: {url: {status, file}}}), `bodies/<hash>`, `roster.json` (the
bc_hydrometric_stations.json the run reads), `seed/feed/` (the published prefix before the run: index.json, clim.json,
temp_history.json, <station>.json) and `seed/priv/` (runs/<YYYY-MM>.jsonl, notified.json).

REPLAY OUT: `OUT/feed/` (what Python's --out dir holds after the run), `OUT/priv/runs/` (Python's `<out>.runs/`),
`OUT/priv/notified.json` (Python's `<out>.notified.json`) and `OUT/posts.json` (notify POSTs, in order). The TS side
writes the same layout from its two Stores.

Recording is real network (public ECCC / BC RFC / CMC endpoints, no credits) with the publisher's own pools (8 hourly,
4 archive, 8 series). Every request is patched at `urllib.request.urlopen`, which every fetch in publish, forecast,
fetch_data and notify calls at call time.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import shutil
import sys
import threading
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(REPO))

_real_urlopen = urllib.request.urlopen


def _url_of(req) -> tuple[str, str, bytes | None]:
    if isinstance(req, str):
        return req, "GET", None
    return req.full_url, req.get_method(), req.data


class _Resp:
    def __init__(self, body: bytes, status: int = 200):
        self._b = body
        self.status = status

    def read(self) -> bytes:
        return self._b

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _freeze(now: datetime) -> None:
    from pipeline.gauges.feed import publish

    class Frozen(datetime):
        @classmethod
        def now(cls, tz=None):                      # noqa: D401
            return now.astimezone(tz) if tz else now.replace(tzinfo=None)

    publish.datetime = Frozen                       # publish.publish / main / fetch_temperatures read the clock here


def _seed(seed: Path, out: Path) -> Path:
    """Lay the seed out the way the Python publisher expects it: <out>/feed, <out>/feed.runs, <out>/feed.notified.json."""
    if out.exists():
        shutil.rmtree(out)
    feed = out / "feed"
    shutil.copytree(seed / "feed", feed)
    runs = seed / "priv" / "runs"
    (out / "feed.runs").mkdir(parents=True, exist_ok=True)
    if runs.is_dir():
        for f in runs.iterdir():
            shutil.copy(f, out / "feed.runs" / f.name)
    if (seed / "priv" / "notified.json").exists():
        shutil.copy(seed / "priv" / "notified.json", out / "feed.notified.json")
    return feed


def _run_main(fixtures: Path, feed: Path) -> None:
    """publish.main() exactly as the cron calls it, with the roster at a path named as the default one (so the notices
    name `data/source/bc_hydrometric_stations.json`, as a real run does)."""
    from pipeline.gauges.feed import publish
    rdir = feed.parent / "_roster"
    rdir.mkdir(exist_ok=True)
    shutil.copy(fixtures / "roster.json", rdir / "bc_hydrometric_stations.json")
    publish.SOURCE = rdir
    clim = feed / "clim.json"
    argv = ["publish", "--out", str(feed), "--stations", str(rdir / "bc_hydrometric_stations.json")]
    if clim.exists():
        argv += ["--clim", str(clim)]
    sys.argv = argv
    publish.main()
    shutil.rmtree(rdir)


def record(fixtures: Path, seed: Path, work: Path) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    _freeze(now)
    bodies = fixtures / "bodies"
    bodies.mkdir(parents=True, exist_ok=True)
    urls: dict[str, dict] = {}
    lock = threading.Lock()

    def keep(url: str, status: int, body: bytes) -> None:
        name = hashlib.sha1(url.encode()).hexdigest()[:20]
        with lock:
            old = urls.get(url)
            if old and 200 <= old["status"] < 300 and not 200 <= status < 300:
                return                              # a retried batch: keep the answer that worked
            (bodies / name).write_bytes(body)
            urls[url] = {"status": status, "file": name}

    def rec(req, *a, **k):
        url, method, _ = _url_of(req)
        if method != "GET":
            raise urllib.error.URLError("recorder: no POSTs")
        try:
            r = _real_urlopen(req, *a, **k)
        except urllib.error.HTTPError as e:
            keep(url, e.code, e.read() or b"")
            raise
        except Exception:
            keep(url, 0, b"")
            raise
        with r:
            body = r.read()
        keep(url, r.status, body)
        return _Resp(body, r.status)

    urllib.request.urlopen = rec
    feed = _seed(seed, work)
    _run_main(fixtures, feed)
    (fixtures / "manifest.json").write_text(json.dumps(
        {"now": now.isoformat().replace("+00:00", "Z"), "urls": dict(sorted(urls.items()))}, indent=0))
    print(f"recorded {len(urls)} urls at {now.isoformat()}")


def replay(fixtures: Path, out: Path, notify: str | None) -> None:
    import os
    from pipeline.gauges.feed import publish
    man = json.loads((fixtures / "manifest.json").read_text())
    now = datetime.fromisoformat(man["now"].replace("Z", "+00:00"))
    _freeze(now)
    publish._sleep = lambda s: None
    misses: list[str] = []
    posts: list[dict] = []

    def serve(req, *a, **k):
        url, method, data = _url_of(req)
        if method == "POST":
            posts.append({"url": url, "body": (data or b"").decode("utf-8")})
            return _Resp(b"ok", 200)
        got = man["urls"].get(url)
        if got is None:
            misses.append(url)
            raise urllib.error.URLError("not recorded")
        body = (fixtures / "bodies" / got["file"]).read_bytes()
        if got["status"] == 0:
            raise urllib.error.URLError("recorded as a network failure")
        if not 200 <= got["status"] < 300:
            raise urllib.error.HTTPError(url, got["status"], "recorded", {}, io.BytesIO(body))
        return _Resp(body, got["status"])

    urllib.request.urlopen = serve
    if notify:
        os.environ["FEED_NOTIFY_URL"] = notify
    else:
        os.environ.pop("FEED_NOTIFY_URL", None)
    feed = _seed(fixtures / "seed", out / "_py")
    _run_main(fixtures, feed)
    # normalise to the shared layout
    final = out
    shutil.move(str(feed), str(final / "feed"))
    priv = final / "priv"
    priv.mkdir(parents=True, exist_ok=True)
    if (out / "_py" / "feed.runs").exists():
        shutil.move(str(out / "_py" / "feed.runs"), str(priv / "runs"))
    if (out / "_py" / "feed.notified.json").exists():
        shutil.move(str(out / "_py" / "feed.notified.json"), str(priv / "notified.json"))
    shutil.rmtree(out / "_py")
    (final / "posts.json").write_text(json.dumps(posts))
    (final / "misses.json").write_text(json.dumps(sorted(set(misses))))
    if misses:
        print(f"replay: {len(set(misses))} url(s) not in the recording", file=sys.stderr)


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("record")
    r.add_argument("--fixtures", type=Path, required=True)
    r.add_argument("--seed", type=Path, required=True, help="seed dir (feed/ + priv/); copied into the fixtures")
    p = sub.add_parser("replay")
    p.add_argument("--fixtures", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--notify", default=None)
    a = ap.parse_args()
    if a.cmd == "record":
        a.fixtures.mkdir(parents=True, exist_ok=True)
        if a.seed.resolve() != (a.fixtures / "seed").resolve():
            if (a.fixtures / "seed").exists():
                shutil.rmtree(a.fixtures / "seed")
            shutil.copytree(a.seed, a.fixtures / "seed")
        record(a.fixtures, a.fixtures / "seed", a.fixtures / "_work")
        shutil.rmtree(a.fixtures / "_work", ignore_errors=True)
    else:
        a.out.mkdir(parents=True, exist_ok=True)
        replay(a.fixtures, a.out, a.notify)


if __name__ == "__main__":
    main()
