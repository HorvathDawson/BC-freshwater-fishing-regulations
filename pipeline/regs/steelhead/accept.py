"""T324: ACCEPT A NEW KNOWN-STEELHEAD LIST — the review moment, as one command.

    python -m pipeline.regs.steelhead.accept            # print the diff: waters added / dropped (writes nothing)
    python -m pipeline.regs.steelhead.accept --write    # ... and write the curated list

The generator (`python -m pipeline.regs.steelhead.known_waters`) writes `data/generated/steelhead/steelhead_waters.json`;
the curated list (`CURATED.waters.steelhead_waters`) is what the reach run reads. Accepting a new generated list is a
DECISION (owner 2026-11-05, data-flow-decisions.md "Regulations"), so a rerun of the generator never changes the curated
list by itself: this prints what accepting it would change — every water added and every water dropped, by item id with
its note, plus the notes that moved — and only `--write` writes the curated file.

What `--write` writes: the curated file's own `$comment` (kept — it is the curated list's documentation, not the
generator's), then the generated `generated` block (source, generator, date, fingerprint) and `waters`, in the
generator's order. `pipeline.atlas.reach.steelhead.load_document` validates the result before it is written, and the
fingerprint must equal the generated list's own — so what lands is exactly what was reviewed.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipeline.common.curated import CURATED, GENERATED, REPO_ROOT


def generated_path() -> Path:
    return Path(GENERATED.base) / "steelhead" / "steelhead_waters.json"


def _key(w: dict) -> str:
    """A water's identity in the list: its item id, else its name (+ region / mu)."""
    if w.get("item_id"):
        return str(w["item_id"])
    return "name:" + "|".join(str(w.get(k) or "") for k in ("name", "region", "mu"))


def diff(curated: dict, generated: dict) -> dict:
    """{added, dropped, renoted} between the curated list and a generated one, each sorted by key."""
    have = {_key(w): w for w in curated.get("waters") or []}
    want = {_key(w): w for w in generated.get("waters") or []}
    return {"added": [want[k] for k in sorted(set(want) - set(have))],
            "dropped": [have[k] for k in sorted(set(have) - set(want))],
            "renoted": [(have[k], want[k]) for k in sorted(set(have) & set(want))
                        if (have[k].get("note") or "") != (want[k].get("note") or "")]}


def accepted(curated: dict, generated: dict) -> dict:
    """The curated document accepting `generated`: the curated `$comment`, the generated `generated` + `waters`."""
    return {"$comment": curated.get("$comment"), "generated": generated["generated"], "waters": generated["waters"]}


def render(d: dict) -> list[str]:
    out = [f"added {len(d['added'])}, dropped {len(d['dropped'])}, note changed {len(d['renoted'])}"]
    for w in d["added"]:
        out.append(f"  + {_key(w)}  {w.get('note') or ''}")
    for w in d["dropped"]:
        out.append(f"  - {_key(w)}  {w.get('note') or ''}")
    for old, new in d["renoted"]:
        out.append(f"  ~ {_key(new)}  {old.get('note') or ''}  ->  {new.get('note') or ''}")
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.steelhead.accept",
                                 description=__doc__.split("\n")[0])
    ap.add_argument("--write", action="store_true", help="write the curated list (default: print the diff only)")
    ap.add_argument("--generated", type=Path, default=None, help="the generated list (default: data/generated/steelhead/"
                                                                 "steelhead_waters.json)")
    a = ap.parse_args(argv)
    from pipeline.atlas.reach.steelhead import SteelheadList, fingerprint

    gen_path = a.generated or generated_path()
    if not gen_path.is_file():
        raise SystemExit(f"accept: no generated list at {gen_path} — make it:\n"
                         "    python -m pipeline.regs.steelhead.known_waters")
    cur_path = Path(CURATED.waters.steelhead_waters)
    curated = json.loads(cur_path.read_text(encoding="utf-8"))
    generated = json.loads(gen_path.read_text(encoding="utf-8"))
    print_ = fingerprint(generated["waters"])
    block = generated.get("generated")
    if block is not None and block.get("fingerprint") != print_:
        raise SystemExit(f"accept: {gen_path} does not match its own fingerprint — regenerate it, never hand-edit it")

    d = diff(curated, generated)
    same = (curated.get("generated") or {}).get("fingerprint") == print_
    if same and block is None:
        print(f"(the generated file predates the generator's `generated` block; its fingerprint {print_} is the "
              "curated list's)")
    if not same and block is None:
        raise SystemExit(f"accept: {gen_path} has no `generated` block (an output of the old generator) — regenerate "
                         "it with `python -m pipeline.regs.steelhead.known_waters` before accepting it")
    print(f"{cur_path.relative_to(REPO_ROOT)}  <-  {gen_path}")
    print("\n".join(render(d)))
    if same:
        print("the curated list already is this generated list — nothing to accept")
        return 0
    if not a.write:
        print("\nnothing written. Accept it with --write, then the reach run and the bundle (pipeline release).")
        return 0
    doc = accepted(curated, generated)
    SteelheadList.model_validate(doc)          # the reach run's own model refuses a bad document before it lands
    from pipeline.common.curated_write import write_any       # T600: backed up and logged
    write_any(cur_path, json.dumps(doc, indent=1, ensure_ascii=False) + "\n", tool="pipeline.regs.steelhead.accept",
              source=f"generated list {doc['generated']['fingerprint']}")
    print(f"wrote {cur_path} ({len(doc['waters'])} waters, fingerprint {doc['generated']['fingerprint']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
