"""The answers layer's command line.

  python -m pipeline.deliver.answers.cli build --bundle FILE --export-dir DIR --out FILE [--workers N]
      build `ui-rules-answers.json` for the export pair in DIR, check that it decodes back to
      what was built, print its sizes (raw and gzipped; no `.gz` file is written)
  python -m pipeline.deliver.answers.cli tap --answers FILE --export-dir DIR ITEM PART MM-DD FISH ORIGIN
      what one tap reads

Nothing here calls the `claude` CLI.
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
from pathlib import Path

from pipeline.deliver.answers import encode as E
from pipeline.deliver.answers.answers import AnswersError, build, load_export


def _raw(wire: dict) -> bytes:
    return json.dumps(wire, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"


def _write(raw: bytes, out: Path) -> dict:
    """Write the answers JSON alone. The gzip size is reported, not written: a `.gz` copy beside it had no reader
    (audit F19, 2026-10-22); the release gzips what it ships."""
    out.write_bytes(raw)
    return {"raw": len(raw), "gz": len(gzip.compress(raw, compresslevel=9, mtime=0))}


def cmd_build(a) -> int:
    """Build, encode, prove the bytes decode back to the model, write. ONE serialised copy is
    held (DATAFLOW P1b, M9.3): the wire is serialised once, dropped, and the check decodes those
    very bytes (it used to hold the model, the wire, a JSON round-trip copy, the decoded model
    and the bytes at once)."""
    data, guide = load_export(a.export_dir)
    model = build(a.bundle, a.export_dir, workers=a.workers,
                  items=a.items.split(",") if a.items else None, export=(data, guide),
                  verdicts=a.verdicts)
    wire = E.encode(model, data)
    E.check_pair(wire, data, guide)
    per = {}
    for name, sec in wire["sections"].items():
        b = json.dumps(sec, separators=(",", ":")).encode()
        per[name] = {"raw": len(b), "gz": len(gzip.compress(b, 9, mtime=0))}
    for name in ("keys", "segments", "moments", "segment_moments", "parts", "fish", "spec"):
        b = json.dumps(wire[name], separators=(",", ":")).encode()
        per[name] = {"raw": len(b), "gz": len(gzip.compress(b, 9, mtime=0))}
    counts = wire["about"]["counts"]
    raw = _raw(wire)
    del wire
    back = E.decode(json.loads(raw), data)
    if back != model:
        raise AnswersError("answers: the file does not decode to the answers it was built from")
    del back, model
    sizes = _write(raw, a.out)
    print(json.dumps({"file": str(a.out), **sizes, "parts": per, "counts": counts}, indent=1))
    return 0


def cmd_tap(a) -> int:
    data, _ = load_export(a.export_dir)
    wire = json.loads(Path(a.answers).read_text(encoding="utf-8"))
    m, d = (int(x) for x in a.date.split("-"))
    print(json.dumps(E.tap(wire, data, a.item, a.part, m, d, a.fish, a.origin), indent=1))
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--bundle", required=True)
    b.add_argument("--export-dir", required=True, type=Path)
    b.add_argument("--out", required=True, type=Path)
    b.add_argument("--workers", type=int, default=0)
    b.add_argument("--items", help="comma-separated item ids (a partial file, for checks)")
    b.add_argument("--verdicts", default=None, help="default: verdicts.sqlite beside the bundle")
    t = sub.add_parser("tap")
    t.add_argument("--answers", required=True, type=Path)
    t.add_argument("--export-dir", required=True, type=Path)
    for x in ("item", "part", "date", "fish", "origin"):
        t.add_argument(x, type=int if x == "part" else str)
    a = ap.parse_args(argv)
    try:
        return {"build": cmd_build, "tap": cmd_tap}[a.cmd](a)
    except AnswersError as e:
        print(e, file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
