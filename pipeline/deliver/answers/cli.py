"""The answers layer's command line.

  python -m pipeline.deliver.answers.cli build --bundle FILE --export-dir DIR --out FILE [--workers N]
      build `ui-rules-answers.json` (+ `.gz`) for the export pair in DIR, check that it decodes
      back to what was built, print its sizes
  python -m pipeline.deliver.answers.cli tap --answers FILE --export-dir DIR ITEM PART MM-DD FISH ORIGIN
      what one tap reads
  python -m pipeline.deliver.answers.cli page-view --answers FILE --export-dir DIR --golden DIR --out DIR
      the answers projected onto the reference harness's meeting point (`reference/compare.py`)
      for the keys the page's golden outputs hold: one `<stream>.jsonl.gz` per stream
      (ladder, answer, row, gear, licence)

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
from pipeline.deliver.calendar import day_of


def _raw(wire: dict) -> bytes:
    return json.dumps(wire, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"


def _write(raw: bytes, out: Path) -> dict:
    out.write_bytes(raw)
    gz = gzip.compress(raw, compresslevel=9, mtime=0)
    Path(str(out) + ".gz").write_bytes(gz)
    return {"raw": len(raw), "gz": len(gz)}


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
    for name in ("keys", "segments", "parts", "fish", "spec"):
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


_LINE_KEYS = ("t", "r", "a", "b", "take", "o", "status", "daily", "outer", "only")


def _inf(v):
    return "Infinity" if v is None else v


def decided_projection(d, rules: list) -> dict | None:
    """Our rows section's decided answer for one fish and origin, as the page's evalSp states it."""
    if d is None:
        return None
    lines = []
    for l in d["lines"]:
        x = {}
        for k in _LINE_KEYS:
            if k not in l:
                continue
            v = l[k]
            if k in ("r", "outer"):
                v = rules[v]
            elif k == "b":
                v = _inf(v)
            elif k == "daily" and l["t"] in ("xref", "origin2"):
                v = _inf(v)
            x[k] = v
        lines.append(x)
    return {"status": d["status"], "daily": "Infinity" if d["status"] == "nolimit" else d["daily"],
            "winner": rules[d["win"]], "narrow": None if d["narrow"] is None else rules[d["narrow"]],
            "lines": lines,
            "roles": {rules[k]: [role, None if by is None else rules[by]] for k, role, by in d["roles"]}}


def row_projection(r: dict, rules: list) -> dict:
    rd = r.get("real_daily")
    return {"kind": r["kind"], "pool": None if r["pool"] is None else rules[r["pool"]],
            "win": None if r["win"] is None else rules[r["win"]],
            "members": r["members"], "all_members": r["all_members"],
            "daily": (("Infinity" if r["kind"] == "nolimit" else r["daily"])
                      if r["pool"] is not None else None),
            "real_daily": None if not rd else [rd["n"], rd["all"], rd["sum"], rd["capped_sum"],
                                               rd["rb"]],
            "everyone": [[l["t"], rules[l["r"]], sorted(l["members"])] for l in r["everyone"]],
            "groups": [[sorted(g["members"]), [[l["t"], rules[l["r"]]] for l in g["facts"]]]
                       for g in r["groups"]]}


def gear_projection(g: dict, closed: bool, rules: list) -> dict:
    if g.get("tidal"):
        return {"tidal": True}                 # FIX D12: the documented tidal state, no gear
    if closed:
        return {"closed": True}
    always = sum(len(rs) for acts in g["conduct"].values() for _, rs in acts)
    return {"closed": False,
            "counts": {k: [rules[v["by"][0]], v["by"][1]] for k, v in g["counts"].items()},
            "elems": {k: [rules[v["by"][0]], v["verdict"]] for k, v in g["elements"].items()},
            "hook": g["hook"], "bait_ban": g["bait_ban"],
            "bait": [[b["element"], b["ok"]] for b in g["bait"]],
            "ways_no": sorted(w["method"] for w in g["ways"] if not w["allowed"]),
            "circ": sorted([rules[c["clause"][0]], c["clause"][1]] for c in g["circumstantial"]),
            "always": always}


def licence_projection(prof: dict, closed: bool, data: dict) -> dict:
    if prof.get("tidal"):
        return {"tidal": True}                 # FIX D12: the documented tidal state, no licence
    if closed:
        return {"closed": True}
    lic, names = data["licensing_ids"], data["licences"]

    def name(d):
        n = (names.get(d) or {}).get("name") or d.replace("_", " ")
        return n[:1].upper() + n[1:]
    mine = [lic[r["req"]] for r in prof["requirements"]] + [lic[k] for k in prof.get("guiding", [])]
    return {"closed": False, "none": prof["none_needed"],
            "buy": sorted([name(d["doc"]), d["base"]] for d in prof["documents"]),
            "mine": sorted(mine)}


def page_view(wire: dict, data: dict, guide: dict, golden: Path) -> dict:
    """For every golden record of the five streams, what our file says (the page's record keys)."""
    from pipeline.deliver.answers.reference.compare import _files, _lines
    members = page_rules(data, guide)
    parts_by_pk: dict = {}
    for item, w in data["waters"].items():
        for pi in range(len(w["parts"])):
            parts_by_pk.setdefault((item, part_key_string(data, item, pi)), pi)
    rules, fish = data["rule_ids"], wire["fish"]
    profiles = wire["sections"]["licence"]["profiles"]
    cache: dict = {}

    def frame(name, item, pk, md):
        pi = parts_by_pk[(item, pk)]
        k = wire["parts"][item][pi]
        key = wire["keys"][k]
        s = E.segment_index(wire["segments"][key[E.SEG_SLOT]], day_of((md // 100, md % 100)))
        sec = wire["sections"][name]
        ref = sec["at"][k][s]
        got = cache.get((name, ref))
        if got is None:
            got = cache[(name, ref)] = E.CODECS[name].decode_frame(sec, ref, rules, fish)
        return got, str(key[0])

    out = {"ladder": {}, "answer": {}, "row": {}, "gear": {}, "licence": {}}
    for rec in (r for f in _files(golden, "ladder") for r in _lines(f)):
        fr, sid = frame("ladder", rec["w"], rec["pk"], rec["md_read"])
        if rec["fish"] not in fr:
            continue                                   # a fish we do not answer: "missing"
        out["ladder"][(rec["w"], rec["pk"], rec["seg_from"], rec["fish"], rec["origin"])] = \
            ladder_projection(fr[rec["fish"]][rec["origin"]], members[sid])
    for rec in (r for f in _files(golden, "card") for r in _lines(f)):
        fr, _ = frame("rows", rec["w"], rec["pk"], rec["md"])
        for S, by_o in fr["fish"].items():
            for o in ("hatchery", "wild"):
                out["answer"][(rec["w"], rec["pk"], rec["date"], S, o)] = \
                    decided_projection(by_o[o], rules)
        for r in fr["rows"]:
            k = r["pool"] if r["pool"] is not None else r["win"]
            out["row"][(rec["w"], rec["pk"], rec["date"], r["kind"], rules[k])] = \
                row_projection(r, rules)
    for rec in (r for f in _files(golden, "gear") for r in _lines(f)):
        g, _ = frame("gear", rec["w"], rec["pk"], rec["md"])
        st, _ = frame("display", rec["w"], rec["pk"], rec["md"])
        out["gear"][(rec["w"], rec["pk"], rec["date"])] = gear_projection(
            g, st["status"] == "closed", rules)
    for rec in (r for f in _files(golden, "licence") for r in _lines(f)):
        lf, _ = frame("licence", rec["w"], rec["pk"], rec["md"])
        st, _ = frame("display", rec["w"], rec["pk"], rec["md"])
        for i, prof in enumerate(profiles):
            out["licence"][(rec["w"], rec["pk"], rec["date"], prof)] = licence_projection(
                lf["profiles"][i], st["status"] == "closed", data)
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
    b.add_argument("--verdicts", default=None, help="default: verdicts.sqlite beside the bundle")
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
