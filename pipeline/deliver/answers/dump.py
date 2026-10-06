"""Write every answers table as JSON for inspection, with sizes and build times.

    python -m pipeline.deliver.answers.dump --out DIR [--bundle FILE] [--export FILE]
                                            [--guide FILE] [--only rules,gear,licence,parts]
                                            [--keys N]

Not the shipping encoder (that is the answers v0 work's `encode.py`): the producers here are pure
(`gear.produce`, `licence.produce`, `display.produce_rules` / `produce_parts`); this module only
interns their output format-2 style (rules and records as export indexes) to measure it.

Reads the bundle and (for the part facts) the export pair decoded by `export_codec.expand`;
writes `rules.json`, `gear.json`, `licence.json`, `parts.json` and `report.json` to DIR. The
tables are deterministic (sorted iteration, interned in first-use order, no clocks); only
`report.json` carries times. Nothing is written beside the bundle or the export.
"""
from __future__ import annotations

import argparse
import gzip
import json
import time
from pathlib import Path

from pipeline.deliver.answers import common, display, gear, licence


def _dump(x) -> bytes:
    return json.dumps(x, sort_keys=False, separators=(",", ":"), ensure_ascii=False).encode()


def _public(d: dict) -> dict:
    return {k: v for k, v in d.items() if not k.startswith("_")}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--bundle", default=None)
    ap.add_argument("--export", type=Path, default=None,
                    help="ui-rules-export.json (default: the generated regs dir)")
    ap.add_argument("--guide", type=Path, default=None)
    ap.add_argument("--only", default="rules,gear,licence,parts")
    ap.add_argument("--keys", type=int, default=0, help="gear: only the first N rule keys")
    a = ap.parse_args(argv)
    only = set(a.only.split(","))
    a.out.mkdir(parents=True, exist_ok=True)
    report: dict = {"tables": {}, "seconds": {}}
    t0 = time.time()
    B = common.load(a.bundle)
    report["bundle"] = {"path": B.path, **B.digest}
    report["seconds"]["load"] = round(time.time() - t0, 2)
    print(f"bundle {B.path}: {len(B.rules)} rules, {len(B.keys)} rule keys")

    def write(name: str, obj) -> None:
        raw = _dump(obj)
        (a.out / f"{name}.json").write_bytes(raw)
        report["tables"][name] = {"raw": len(raw), "gz": len(gzip.compress(raw, 9, mtime=0))}
        print(f"  wrote {name}.json: {len(raw):,} B raw, {report['tables'][name]['gz']:,} B gz")

    out_gear = out_lic = None
    if "rules" in only or "parts" in only:
        t = time.time()
        write("rules", {"rules": display.build_rules(B), "decisions": display.DECISIONS})
        report["seconds"]["rules"] = round(time.time() - t, 2)
    if "gear" in only or "parts" in only:
        t = time.time()
        keys = list(B.keys)[:a.keys] if a.keys else None
        out_gear = gear.build(B, keys)
        write("gear", _public(out_gear))
        report["seconds"]["gear"] = round(time.time() - t, 2)
    if "licence" in only or "parts" in only:
        t = time.time()
        out_lic = licence.build(B.path)
        write("licence", {**_public(out_lic), "decisions": licence.DECISIONS})
        report["seconds"]["licence"] = round(time.time() - t, 2)
    if "parts" in only:
        t = time.time()
        # the export pair beside the bundle's generated tree (data/generated/regs/)
        exp = a.export or (Path(B.path).parents[1] / "regs" / "ui-rules-export.json")
        gd = a.guide or exp.with_name("ui-rules-guide.json")
        doc = display.load_export(exp, gd, B)
        parts = display.produce_parts(B, doc)
        gk, lk = out_gear["_key_index"], out_lic["_key_index"]
        for w in parts.values():
            for p in w["parts"].values():
                p["gear"] = gk[p.pop("rule_key")]
                p["licence"] = lk[p.pop("licence_key")]
                p["steelhead_line"] = [[d, c] for d, c in p["steelhead_line"].items()]
        write("parts", parts)
        report["seconds"]["parts"] = round(time.time() - t, 2)
    report["seconds"]["total"] = round(time.time() - t0, 2)
    report["total"] = {"raw": sum(v["raw"] for v in report["tables"].values()),
                       "gz": sum(v["gz"] for v in report["tables"].values())}
    (a.out / "report.json").write_text(json.dumps(report, indent=1))
    print(json.dumps(report["seconds"]), json.dumps(report["total"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
