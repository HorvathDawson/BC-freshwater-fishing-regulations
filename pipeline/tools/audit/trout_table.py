"""THE TROUT TABLE BUILDER OVER A BUILT RELEASE (trout table plan, T810 ticket F's smoke; G2's catalogue reads the same
walk later). Reads one built release; changes nothing.

    python -m pipeline.tools.audit.trout_table <release dir> [--keys N] [--examples K] [--out FILE]

Every answer key of the release, decoded through the release's own reference decoders (`answers_diff.Release`), goes
through `answers/trout/table.compose_key` exactly as ticket I will call it (the post-pass rows frames, the display
frames, the key's segment starts and moments, the release's rule table). The report counts the tables built, the NULL
cells by reason (Q-D3: closed, tidal, no trout or char fish), the mixed rows split (the A0 amendment), the year bars and
date sets, and every key the builder refused, grouped by its message (the build stops on each: AGENTS W46), with
example waters. Exit 0, or 2 when the release cannot be read.

A release built before the switch carries the trout `group` on its rows (rows 33), which the builder reads; a release
cut after L dropped it cannot be re-built from, by design (J proves the shipped tables instead).
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Optional

from pipeline.deliver.answers import common
from pipeline.deliver.answers.trout import table as T

TC = common.trout_char()


def _keys(R):
    """(key index, starts, moments | None) of every answer key of the release."""
    w = R.wire
    for k, key in enumerate(w["keys"]):
        starts = w["segments"][key[9]]
        moms = None if key[10] is None else [w["moments"][i] for i in w["segment_moments"][key[10]]]
        yield k, starts, moms


def run(d, limit: Optional[int] = None, examples: int = 5, log=print) -> dict:
    from pipeline.deliver.store.release import province_store
    from pipeline.tools.audit import answers_diff as AD
    t0 = time.time()
    R = AD.Release(Path(d))
    st = province_store(Path(d))
    try:
        sub = st.export_subset(check=True)
    finally:
        st.close()
    rules = T.Rules({i: T.rule_of(sub["rules"][rid]) for i, rid in enumerate(R.rule_ids)})
    rows, disp = R.sections["rows"], R.sections["display"]
    frames: dict = {}

    def frame(ref):
        if ref not in frames:
            frames[ref] = R.codecs["rows"].decode_frame(rows, ref, R.rule_ids, R.fish)
        return frames[ref]
    names = defaultdict(Counter)
    for h, a in R.akey.items():
        if a:
            names[a][R.item_name.get(R.water.get(h, ("?", 0))[0], "?")] += 1
    name = lambda k: (names[k + 1].most_common(1) or [("answer key %d" % (k + 1), 0)])[0][0]
    n = Counter()
    errors = defaultdict(list)
    tops = Counter()
    for k, starts, moms in _keys(R):
        if limit is not None and k >= limit:
            break
        n["keys"] += 1
        fs = [frame(ref) for ref in rows["at"][k]]
        ds = [disp["frames"][i] for i in disp["at"][k]]
        try:
            got = T.compose_key(fs, ds, starts, moms, rules)
        except Exception as e:                                   # noqa: BLE001 — each refusal is counted, by message
            msg = re.sub(r"\d+", "N", str(e).split("\n")[0])[:160]
            errors[f"{type(e).__name__}: {msg}"].append(name(k))
            n["keys_refused"] += 1
            n["cells_refused"] += len(starts)
            continue
        for j, (f, dsp, t) in enumerate(zip(fs, ds, got)):
            n["cells"] += 1
            mixed = sum(1 for r in f["rows"] if set(r.get("all_members") or ()) & TC
                        and set(r.get("all_members") or ()) - TC)
            n["mixed_rows_split"] += mixed if t is not None else 0
            if t is None:
                why = dsp.get("status") if dsp.get("status") in ("closed", "tidal") else "no trout or char fish"
                n[f"null: {why}"] += 1
                continue
            n["tables"] += 1
            tb = t["table"]
            n["year_bars"] += tb["year"] is not None
            n["date_sets"] += t["dates"] is not None
            n["together"] += tb["together"] is not None
            n["box_labels"] += sum(1 for b in tb["boxes"] if b["label"])
            n["twin_rows"] += sum(1 for b in tb["boxes"] for r in b["rows"] if r["origin"])
            n["possession_bands"] += sum(1 for b in tb["boxes"] for x in b["bands"] if x["kind"] == "possession")
            tops[re.sub(r"\d+", "N", tb["top"]["lead"])] += 1
    rep = {"release": str(d), "seconds": round(time.time() - t0), "counts": dict(n),
           "refused": {m: {"keys": len(v), "waters": sorted(set(v))[:examples]}
                       for m, v in sorted(errors.items(), key=lambda kv: -len(kv[1]))},
           "top_forms": dict(tops.most_common())}
    return rep


def text(rep: dict) -> str:
    out = [f"trout table builder over {rep['release']} ({rep['seconds']} s)"]
    out += [f"  {k}: {v:,}" for k, v in rep["counts"].items()]
    out.append(f"refused: {len(rep['refused'])} kinds")
    for m, x in rep["refused"].items():
        out.append(f"  {x['keys']:>6} keys  {m}  e.g. {', '.join(x['waters'])}")
    out.append("top-line forms:")
    out += [f"  {c:>7}  {f}" for f, c in rep["top_forms"].items()]
    return "\n".join(out)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("release")
    ap.add_argument("--keys", type=int, default=None, help="only the first N answer keys")
    ap.add_argument("--examples", type=int, default=5)
    ap.add_argument("--out", default=None, help="also write the report as JSON here")
    a = ap.parse_args(argv)
    try:
        rep = run(a.release, a.keys, a.examples)
    except Exception as e:                                       # noqa: BLE001
        print(f"trout_table: cannot read {a.release}: {type(e).__name__}: {e}", file=sys.stderr)
        return 2
    print(text(rep))
    if a.out:
        Path(a.out).write_text(json.dumps(rep, indent=1, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
