"""THE WORDS THE REACH READS, AND A GUARD ON THEM (T679 D4, owner 2026-11-12).

The reach reads a few printed WORDS to decide where a rule applies (owner D4: "leave" them as regexes over `verbatim`):
  * at a confluence cut, whether the joining water is in or out — "including X" (`build._rule_includes`), "not
    including X" (`_rule_excludes`), signs below / above X's confluence (`_signs_below`, `_signs_above`); and the same
    "not including" for a lake row's through-river (`through_river`);
  * "(also in M.U. x-y)" in a row's name widens its regions (`outside._ALSO_IN`);
  * whether a row prints steelhead (`steelhead.prints_steelhead`).
The owner's guard: THE RUN STOPS AND NAMES THE ROW WHEN A ROW THAT MATCHED BEFORE STOPS MATCHING. "Before" is the
accepted list `data/curated/regulations/word_reads.json` (`CURATED.regulations.word_reads`): every read that matched,
one key per row and read. A read that matches now and is not listed is reported (new), never a stop. A key whose row is
gone from the corpus is reported (gone), not a stop: nothing reads it any more.

    key = "<entry_id>|<rule_id>|<read>|<water or MU>"   (rule_id empty for a row-level read)

Every read is RE-ASKED from the corpus's words and the run's diagnostics (`confluence_cut`, `through_river`: which
water joined where), by the same functions the reach calls, so the list can be made from any finished run:

    python -m pipeline.atlas.reach.word_reads --run <reach run dir> --out data/curated/regulations/word_reads.json

(the accept step, after reading what moved; it prints every key added and dropped). The reach CLI writes the run's
own list (`word_reads.json`) and stops on a lost read (`A_WORD_READ_THAT_STOPS_MATCHING_STOPS_THE_RUN` in `build`).
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable, Mapping

FILE = "word_reads.json"
FORMAT = "word_reads/1"
ABOUT = ("T679 D4 (owner 2026-11-12): every printed-word read the reach makes that MATCHED on the accepted run — one key "
         "per row and read, `<entry_id>|<rule_id>|<read>|<water or MU>`: confluence `includes` / `excludes` / "
         "`signs_below` / `signs_above` of the joining water, a lake row's through-river `through_excludes`, a row "
         "name's `also_in` MU, a row's `prints_steelhead`. The reach run STOPS, naming the row, when a key listed here "
         "no longer matches and its row is still in the corpus. Machine-made, then ACCEPTED: written only by `python -m "
         "pipeline.atlas.reach.word_reads --run <reach run> --out <this file>` after the moves are read; read by the "
         "reach CLI.")


def _payload(d) -> dict:
    p = d.get("payload") if isinstance(d, Mapping) else getattr(d, "payload", None)
    return json.loads(p) if isinstance(p, str) else dict(p or {})


def _get(d, k):
    return d.get(k) if isinstance(d, Mapping) else getattr(d, k, None)


def _words(rule) -> str:
    return " ".join(str(_get(rule, k) or "") for k in ("verbatim", "extent_text"))


def reads(entries: Iterable, diagnostics: Iterable) -> list[str]:
    """Every word read that matches, as keys. `entries`: the corpus (dicts or models: `entry_id`, `name`, `rules`).
    `diagnostics`: the run's rule diagnostics (`Diagnostic` objects or `rule_diagnostic.jsonl` rows)."""
    from pipeline.atlas.reach import build as B
    from pipeline.atlas.reach.outside import _ALSO_IN
    from pipeline.atlas.reach.steelhead import prints_steelhead
    if isinstance(entries, Mapping):
        entries = entries.values()
    rules: dict[tuple[str, str], object] = {}
    out: set[str] = set()
    for e in entries:
        eid = str(_get(e, "entry_id") or "")
        for r in _get(e, "rules") or ():
            rules[(eid, str(_get(r, "rule_id") or ""))] = r
        for mu in _ALSO_IN.findall(str(_get(e, "name") or "")):
            out.add(f"{eid}||also_in|{mu}")
        if prints_steelhead(e):
            out.add(f"{eid}||prints_steelhead|")
    conf = (("includes", B._rule_includes), ("excludes", B._rule_excludes),
            ("signs_below", B._signs_below), ("signs_above", B._signs_above))
    for d in diagnostics:
        kind = _get(d, "kind")
        if kind not in ("confluence_cut", "through_river"):
            continue
        eid, rid = str(_get(d, "entry_id") or ""), str(_get(d, "rule_id") or "")
        rule = rules.get((eid, rid))
        water = str(_payload(d).get("water") or "")
        if rule is None or not water or water == "(unnamed)":
            continue
        words = _words(rule)
        if kind == "confluence_cut":
            for name, fn in conf:
                if fn(words, water):
                    out.add(f"{eid}|{rid}|{name}|{water}")
        elif B._rule_excludes(words, water):
            out.add(f"{eid}|{rid}|through_excludes|{water}")
    return sorted(out)


def present(entries: Iterable) -> set[str]:
    """`entry_id|rule_id` for every rule, and `entry_id|` for every row, in the corpus."""
    out: set[str] = set()
    for e in (entries.values() if isinstance(entries, Mapping) else entries):
        eid = str(_get(e, "entry_id") or "")
        out.add(f"{eid}|")
        for r in _get(e, "rules") or ():
            out.add(f"{eid}|{_get(r, 'rule_id') or ''}")
    return out


def compare(now: Iterable[str], accepted: Iterable[str], rows: set[str]) -> dict[str, list[str]]:
    """`lost` (accepted, not matching now, its row/rule still in the corpus: the stop), `gone` (accepted, its row or
    rule no longer in the corpus), `new` (matching now, not accepted)."""
    now, accepted = set(now), set(accepted)
    lost, gone = [], []
    for k in sorted(accepted - now):
        eid, rid = k.split("|")[:2]
        (lost if f"{eid}|{rid}" in rows else gone).append(k)
    return {"lost": lost, "gone": gone, "new": sorted(now - accepted)}


def load(path=None) -> list[str]:
    """The accepted list. Missing is refused (AGENTS 37): a missing list is not "nothing matched before"."""
    if path is None:
        from pipeline.common.curated import CURATED
        path = CURATED.regulations.word_reads
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"word_reads: {p} is missing — accept a run's list: python -m pipeline.atlas.reach.word_reads "
                         "--run <reach run dir> --out <this file>")
    d = json.loads(p.read_text(encoding="utf-8"))
    if d.get("format") != FORMAT:
        raise SystemExit(f"word_reads: {p}: format {d.get('format')!r}, expected {FORMAT}")
    return list(d["reads"])


def document(keys: Iterable[str]) -> str:
    return json.dumps({"_about": ABOUT, "format": FORMAT, "reads": sorted(set(keys))}, indent=0,
                      ensure_ascii=False) + "\n"


def lost_message(diff: Mapping[str, list[str]]) -> str:
    return (f"WordReadLost: {len(diff['lost'])} printed-word read(s) that matched before no longer match (T679 D4) — the "
            "row's words or the water at its cut changed; read each, then fix the row or accept the run "
            "(`python -m pipeline.atlas.reach.word_reads --run <this run> --out <the accepted list>`):\n  "
            + "\n  ".join(diff["lost"]))


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run", required=True, help="a finished reach run dir (its word_reads.json, else its "
                                                 "rule_diagnostic.jsonl + the corpus)")
    ap.add_argument("--out", required=True, help="the accepted list to write")
    a = ap.parse_args(argv)
    run = Path(a.run)
    if (run / FILE).exists():
        keys = json.loads((run / FILE).read_text(encoding="utf-8"))["reads"]
    else:
        from pipeline.regs.parsing import io as parse_io
        entries = parse_io.read_all_entries()
        diags = [json.loads(x) for x in (run / "rule_diagnostic.jsonl").read_text(encoding="utf-8").splitlines() if x]
        keys = reads(entries, diags)
    old = set(load(a.out)) if Path(a.out).exists() else set()
    for k in sorted(set(keys) - old):
        print(f"  + {k}")
    for k in sorted(old - set(keys)):
        print(f"  - {k}")
    Path(a.out).write_text(document(keys), encoding="utf-8")
    print(f"wrote {a.out}: {len(set(keys)):,} read(s) ({len(set(keys) - old)} added, {len(old - set(keys))} dropped)")


if __name__ == "__main__":
    main()
