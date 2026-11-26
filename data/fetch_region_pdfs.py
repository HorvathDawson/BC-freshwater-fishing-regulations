"""T1156: FETCH THE NINE REGION PDFs, checked against a tracked pin file. Beside `fetch_data.py`; light (no GIS imports).

    python -m data.fetch_region_pdfs                    # fetch what is missing into source/official/; check every file
    python -m data.fetch_region_pdfs --served           # download all nine to a temp name and compare; keeps nothing
    python -m data.fetch_region_pdfs --repin NAME.pdf   # the province republished NAME: accept its new bytes, rewrite that row

The nine are gov.bc.ca's own copies of the synopsis chapters (Region 1 ... Region 8), kept as pinned reference copies in
`SOURCE / "official"` (git-ignored). The BOOK is `SOURCE / "fishing_synopsis.pdf"`; validate against nothing else.
Pins: `SOURCE / "region_pdfs.pins.json"` (tracked): per file `sha256`, `bytes`, `date_fetched`; other keys on a row (the
change feed's text hashes, T1166) describe the pinned bytes, so a re-pin DROPS them, says which, and they must be
recomputed. `--served` and `--repin` do not combine (nothing is kept under `--served`).

A SHOWN STOP, never a quiet pass: a changed checksum names the file and says the province republished it (re-pinning is
the separate `--repin`, which prints old and new and rewrites exactly that row); an HTTP error or a network failure names
the status and the file; a download goes to a temp name, must be a non-empty PDF (`%PDF-`) before any checksum is
compared, then is moved, so a partial, empty, HTML or refused one leaves nothing behind and is never re-pinned. A `.part`
file left by a killed run is removed at the start (and said). Exit 1 when any file stopped.
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import shutil
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import Callable

BASE_URL = ("https://www2.gov.bc.ca/assets/gov/sports-recreation-arts-and-culture/outdoor-recreation/"
            "fishing-and-hunting/freshwater-fishing")
#: a generic agent: some CDNs refuse the default `Python-urllib`; no name or address of anyone in it
USER_AGENT = "region-pdf-fetch/1.0"
PINS_FILE = "region_pdfs.pins.json"
FOLDER = "official"


class DownloadError(Exception):
    """A download that did not complete: `status` is the HTTP status, or None for a network failure / a short read."""

    def __init__(self, status: int | None, reason: str):
        super().__init__(reason)
        self.status = status
        self.reason = reason


Download = Callable[[str, Path], None]


def download_url(url: str, dest: Path, timeout: int = 120, opener=urllib.request.urlopen) -> None:
    """Stream `url` into `dest`. HTTP 4xx/5xx and network failures raise `DownloadError`; a body shorter than the
    server's Content-Length does too."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with opener(req, timeout=timeout) as resp, open(dest, "wb") as out:
            shutil.copyfileobj(resp, out)
            want = resp.headers.get("Content-Length")
    except urllib.error.HTTPError as e:
        raise DownloadError(e.code, f"HTTP {e.code} {e.reason}") from e
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise DownloadError(None, f"network failure: {e}") from e
    if want is not None:
        try:
            want_n = int(want)
        except ValueError:
            raise DownloadError(None, f"malformed Content-Length {want!r}") from None
        if want_n != dest.stat().st_size:
            raise DownloadError(None, f"short read: {dest.stat().st_size} bytes of {want}")


def load_pins(path: Path) -> dict:
    """The pin file; a missing or malformed one raises (no default)."""
    if not path.exists():
        raise FileNotFoundError(f"{path}: the region PDF pin file is missing (tracked in git; restore it)")
    blob = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(blob.get("pdfs"), dict) or not blob["pdfs"]:
        raise ValueError(f"{path}: no `pdfs` rows")
    return blob


def write_pins(path: Path, blob: dict) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(blob, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def run(dest_dir: Path, pins_path: Path, download: Download = download_url, *, served: bool = False,
        repin: str | None = None, today: str | None = None, say: Callable[[str], None] = print) -> list[str]:
    """Do the fetch; returns the list of stop messages (empty = all nine are as pinned). Nothing is raised for a bad
    file: every file is tried, each stop is said and returned."""
    blob = load_pins(pins_path)
    pins = blob["pdfs"]
    if repin is not None and repin not in pins:
        return [f"--repin {repin}: not one of the nine ({', '.join(pins)})"]
    if served and repin is not None:
        return ["--served and --repin do not combine: --served keeps nothing"]
    today = today or datetime.date.today().isoformat()
    scratch = tempfile.TemporaryDirectory(prefix="region-pdfs-") if served else None
    if not served:
        dest_dir.mkdir(parents=True, exist_ok=True)
        for old in sorted(dest_dir.glob(".*.part")):
            old.unlink()
            say(f"removed a leftover partial download: {old.name}")
    work = Path(scratch.name) if scratch else dest_dir
    stops: list[str] = []
    for name, pin in pins.items():
        final = dest_dir / name
        if final.exists() and not served and repin != name:
            got = sha256_of(final)
            if got != pin["sha256"]:
                stops.append(f"STOP {name}: the local file differs from its pin (sha256 {got[:8]}, pinned "
                             f"{pin['sha256'][:8]}). Delete it to fetch the pinned copy, or `--repin {name}` if the "
                             f"province republished it.")
            else:
                say(f"ok      {name}  (local, matches the pin)")
            continue
        fd, tmp_name = tempfile.mkstemp(prefix=f".{name}.", suffix=".part", dir=work)
        os.close(fd)
        tmp = Path(tmp_name)
        try:
            try:
                download(f"{BASE_URL}/{name}", tmp)
            except DownloadError as e:
                stops.append(f"FAIL {name}: {e.reason}" + (f" (HTTP status {e.status})" if e.status else "")
                             + f" <- {BASE_URL}/{name}")
                continue
            size = tmp.stat().st_size
            with open(tmp, "rb") as f:
                head = f.read(5)
            if size == 0 or head != b"%PDF-":
                stops.append(f"FAIL {name}: not a PDF ({size} bytes, starts {head!r}); the server did not serve the "
                             f"file <- {BASE_URL}/{name}")
                continue
            sha = sha256_of(tmp)
            if sha != pin["sha256"] and repin != name:
                stops.append(f"STOP {name}: the checksum changed (served sha256 {sha[:8]}, {size} bytes; pinned "
                             f"{pin['sha256'][:8]}, {pin['bytes']} bytes). The province republished it; nothing was "
                             f"written. Look at what changed, then `--repin {name}`.")
                continue
            if served:
                say(f"ok      {name}  (served bytes match the pin)")
                continue
            os.replace(tmp, final)
            if sha != pin["sha256"]:
                say(f"REPIN   {name}: sha256 {pin['sha256']} -> {sha}; bytes {pin['bytes']} -> {size}; "
                    f"date {pin['date_fetched']} -> {today}")
                dropped = sorted(set(pin) - {"sha256", "bytes", "date_fetched"})
                pin.clear()
                pin.update({"sha256": sha, "bytes": size, "date_fetched": today})
                if dropped:
                    say(f"DROPPED {name}: {', '.join(dropped)} described the old bytes; recompute them")
                write_pins(pins_path, blob)
            else:
                say(f"fetched {name}  ({size} bytes, matches the pin)")
        finally:
            tmp.unlink(missing_ok=True)
    if scratch:
        scratch.cleanup()
    for s in stops:
        say(s)
    return stops


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--served", action="store_true", help="download all nine to a temp name and compare to the pins; keep nothing")
    ap.add_argument("--repin", metavar="NAME.pdf", help="accept the republished NAME: write it, rewrite exactly that pin row")
    ap.add_argument("--dest", type=Path, help="the folder to fill (default: source/official)")
    args = ap.parse_args(argv)
    if args.served and args.repin:
        ap.error("--served keeps nothing, so it cannot be combined with --repin")
    from pipeline.common.curated import SOURCE
    dest = args.dest or (SOURCE / FOLDER)
    stops = run(Path(dest), Path(SOURCE / PINS_FILE), served=args.served, repin=args.repin)
    return 1 if stops else 0


if __name__ == "__main__":
    sys.exit(main())
