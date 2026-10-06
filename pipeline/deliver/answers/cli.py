"""The answers layer's command line.

  python -m pipeline.deliver.answers.cli build --bundle FILE --export-dir DIR --out FILE [--workers N]
      build `ui-rules-answers.json` (+ `.gz`) for the export pair in DIR, check that it decodes
      back to what was built, print its sizes
  python -m pipeline.deliver.answers.cli tap --answers FILE --export-dir DIR ITEM PART MM-DD FISH ORIGIN
      what one tap reads
  python -m pipeline.deliver.answers.cli page-view --answers FILE --export-dir DIR --golden DIR --out DIR
      the answers projected onto the reference harness's meeting point (`reference/compare.py`)
      for the keys the page's golden outputs hold: `ladder.jsonl.gz`, `answer.jsonl.gz`

Nothing here calls the `claude` CLI.
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
from pathlib import Path

from pipeline.deliver.answers import encode as E
from pipeline.deliver.answers.answers import AnswersError, build, day_of, load_export


def _write(wire: dict, out: Path) -> dict:
    raw = json.dumps(wire, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
    out.write_bytes(raw)
    gz = gzip.compress(raw, compresslevel=9, mtime=0)
    Path(str(out) + ".gz").write_bytes(gz)
    return {"raw": len(raw), "gz": len(gz)}


def cmd_build(a) -> int:
    data, guide = load_export(a.export_dir)
    model = build(a.bundle, a.export_dir, workers=a.workers,
                  items=a.items.split(",") if a.items else None)
    wire = E.encode(model, data)
    E.check_pair(wire, data, guide)
    back = E.decode(json.loads(json.dumps(wire)), data)
    if back != model:
        raise AnswersError("answers: the file does not decode to the answers it was built from")
    sizes = _write(wire, a.out)
    per = {}
    for name, sec in wire["sections"].items():
        b = json.dumps(sec, separators=(",", ":")).encode()
        per[name] = {"raw": len(b), "gz": len(gzip.compress(b, 9, mtime=0))}
    for name in ("keys", "segments", "parts", "fish", "spec"):
        b = json.dumps(wire[name], separators=(",", ":")).encode()
        per[name] = {"raw": len(b), "gz": len(gzip.compress(b, 9, mtime=0))}
    print(json.dumps({"file": str(a.out), **sizes, "parts": per, "counts": wire["about"]["counts"]},
                     indent=1))
    return 0


def cmd_tap(a) -> int:
    data, _ = load_export(a.export_dir)
    wire = json.loads(Path(a.answers).read_text(encoding="utf-8"))
    m, d = (int(x) for x in a.date.split("-"))
    print(json.dumps(E.tap(wire, data, a.item, a.part, m, d, a.fish, a.origin), indent=1))
    return 0


# --------------------------------------------------------------------------------------------
# The meeting point with the consumer page's golden outputs (reference/compare.py)
# --------------------------------------------------------------------------------------------

def part_key_string(data: dict, item: str, pi: int) -> str:
    """The reference harness's `partKey` of an export part (golden.js)."""
    arr = data["waters"][item]["parts"][pi]
    f = arr[4] if len(arr) > 4 else {}
    return "|".join(["" if arr[0] is None else str(arr[0]), "" if arr[1] is None else str(arr[1]),
                     "sw" if f.get("anadromous_rainbow") else "",
                     f.get("steelhead") or "", "sr0" if f.get("steelhead_rules") is False else "",
                     "+".join(f.get("province_except") or []), ",".join(f.get("home_region") or [])])


def page_rules(data: dict, guide: dict) -> dict:
    """{ruleset id: the rule ids a part of it hands the page's ladder} — reach + trib, minus the
    information family (reference/convert.py, consumer §1.2)."""
    from pipeline.tools.export_codec import expand
    m = expand(data, guide)
    return {sid: {r for via in ("reach", "trib") for r in s.get(via, [])
                  if m["rules"][r]["family"] != "information"}
            for sid, s in m["rulesets"].items()}


def ladder_projection(verdict: dict, members: set) -> dict:
    """Our verdict for one fish and origin, as the page's ladder states it: {rule: [state, partly,
    by]} over the rules the page holds."""
    return {k: [v[0], 1 if v[1] else 0, v[3]] for k, v in verdict.items() if k in members}


def answer_projection(d: list) -> dict | None:
    status, daily, win = d
    if status == "no_rule":
        return None
    return {"status": {"no_limit": "nolimit"}.get(status, status), "daily": daily, "winner": win}


def page_view(wire: dict, data: dict, guide: dict, golden: Path) -> dict:
    """For every golden ladder / answer key, what our file says (the page's record keys)."""
    from pipeline.deliver.answers.reference.compare import _files, _lines
    members = page_rules(data, guide)
    parts_by_pk: dict = {}
    for item, w in data["waters"].items():
        for pi in range(len(w["parts"])):
            parts_by_pk.setdefault((item, part_key_string(data, item, pi)), pi)
    rules, fish = data["rule_ids"], wire["fish"]
    cache: dict = {}

    def frame(name, item, pk, md):
        pi = parts_by_pk[(item, pk)]
        k = wire["parts"][item][pi]
        key = wire["keys"][k]
        s = E.segment_index(wire["segments"][key[7]], day_of(md // 100, md % 100))
        sec = wire["sections"][name]
        ref = sec["at"][k][s]
        got = cache.get((name, ref))
        if got is None:
            got = cache[(name, ref)] = E.CODECS[name].decode_frame(sec, ref, rules, fish)
        return got, str(key[0])

    out = {"ladder": {}, "answer": {}}
    for rec in (r for f in _files(golden, "ladder") for r in _lines(f)):
        fr, sid = frame("ladder", rec["w"], rec["pk"], rec["md_read"])
        if rec["fish"] not in fr:
            continue                                   # a fish we do not answer: "missing"
        out["ladder"][(rec["w"], rec["pk"], rec["seg_from"], rec["fish"], rec["origin"])] = \
            ladder_projection(fr[rec["fish"]][rec["origin"]], members[sid])
    for rec in (r for f in _files(golden, "card") for r in _lines(f)):
        fr, _ = frame("answer", rec["w"], rec["pk"], rec["md"])
        for S in rec["answers"]:
            if S not in fr:
                continue
            for o in ("hatchery", "wild"):
                out["answer"][(rec["w"], rec["pk"], rec["date"], S, o)] = answer_projection(fr[S][o])
    return out


def cmd_page_view(a) -> int:
    data, guide = load_export(a.export_dir)
    wire = json.loads(Path(a.answers).read_text(encoding="utf-8"))
    E.check_pair(wire, data, guide)
    view = page_view(wire, data, guide, a.golden)
    a.out.mkdir(parents=True, exist_ok=True)
    for s, recs in view.items():
        with gzip.open(a.out / f"{s}.jsonl.gz", "wt", encoding="utf-8") as f:
            for k, v in sorted(recs.items()):
                f.write(json.dumps({"key": list(k), "value": v}) + "\n")
        print(f"{s}: {len(recs)} records")
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
    t = sub.add_parser("tap")
    t.add_argument("--answers", required=True, type=Path)
    t.add_argument("--export-dir", required=True, type=Path)
    for x in ("item", "part", "date", "fish", "origin"):
        t.add_argument(x, type=int if x == "part" else str)
    p = sub.add_parser("page-view")
    p.add_argument("--answers", required=True, type=Path)
    p.add_argument("--export-dir", required=True, type=Path)
    p.add_argument("--golden", required=True, type=Path)
    p.add_argument("--out", required=True, type=Path)
    a = ap.parse_args(argv)
    try:
        return {"build": cmd_build, "tap": cmd_tap, "page-view": cmd_page_view}[a.cmd](a)
    except AnswersError as e:
        print(e, file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
