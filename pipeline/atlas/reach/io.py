"""Write and read a reach-builder run.

**JSONL, not parquet.** Parquet is available, but these tables are small (3,038 rules,
~7.6k bindings) and the properties that matter here are *byte-identical rebuilds* and
*a readable diff in review*. Sorted JSONL gives both for free; parquet's row-group
metadata and compression settings are one more thing that has to be pinned to stay
deterministic. Revisit if a table reaches millions of rows.

Determinism: every row is written in sorted key order with sorted JSON keys and no
timestamps. `report.json` is the one file carrying a duration, so it is excluded from the
digest — otherwise no two builds could ever match.

**Except the bindings (T73).** `rule_section` reached 149 M rows / 20 GB as one JSON object
per (rule, section). It is written as `rule_section.jsonl.gz`: gzip (mtime 0, so still
byte-identical) of ONE line per bound rule, `{"entry_id", "rule_id", "sections": [sorted],
"trib": [the subset reached by the tributary walk]}`. Every reader goes through
`rule_section_file` + `iter_rule_sections`, which yield the same (entry, rule, section, scope)
rows in the same sorted order from either form, so a run written before T73 (plain
`rule_section.jsonl`) still reads, and `binding_diff` compares the two forms directly.
`python -m pipeline.atlas.reach.io convert <run>` rewrites an old run in place.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
import time
from dataclasses import asdict
from itertools import groupby
from pathlib import Path
from typing import Iterable, Iterator

from pipeline.atlas.reach.models import Outcome, iter_entries

TABLES = ("rule_section", "rule_unresolved", "rule_extent", "rule_diagnostic")
#: The licensing records, placed by the same builder. Separate files, because a record is not a
#: rule — its id is unique within its entry among LICENSING records, not among rules — and a
#: reader that joined the two on (entry_id, id) would be joining different namespaces.
LICENSING_TABLES = ("licensing_placement", "licensing_section", "licensing_diagnostic")
#: `steelhead: known | possible` per section (`pipeline.atlas.reach.steelhead`): one row per section
#: that has the attribute — the steelhead row that binds it (or `curated list`, or `zp:steelhead` for
#: possible), whether the book (`regulations`) or the curated list (`listed`) makes it known, and
#: whether a rainbow over 50 cm is a steelhead there (`anadromous`).
STEELHEAD_TABLE = "steelhead_presence"
#: THE WATER THE BUILDER TOOK OUT OF EVERY BINDING, written so the bundle reads it rather than
#: deriving it again: `tidal` — {section_id, entry_id}, the book's tidal water with the row that
#: says so (`outside.tidal_owner`); `outside_bc` — {section_id}, water past the border
#: (`outside.outside_bc`). Both facts of the water, outside the rules' digest, each with a digest
#: of its own in report.json (`tidal_digest`, `outside_digest`).
TIDAL_TABLE = "tidal"
OUTSIDE_TABLE = "outside_bc"
#: (T73) the bindings' file: one gzip line per bound rule (see the module note)
RULE_SECTION_FILE = "rule_section.jsonl.gz"
#: the form before T73: one plain line per (rule, section) — still read, never written
LEGACY_RULE_SECTION_FILE = "rule_section.jsonl"


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for r in rows:
            f.write(json.dumps(r, sort_keys=True, ensure_ascii=False, separators=(",", ":")))
            f.write("\n")


def _line(obj) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":")) + "\n"


def rule_section_file(run_dir: str | Path) -> Path:
    """The run's bindings file: `rule_section.jsonl.gz`, or a pre-T73 run's `rule_section.jsonl`.
    RAISES when there is neither (a run with no bindings file is not a run that binds nothing —
    every water would render open), and when there are both (which one is the run?)."""
    d = Path(run_dir)
    new, old = d / RULE_SECTION_FILE, d / LEGACY_RULE_SECTION_FILE
    if new.exists() and old.exists():
        raise SystemExit(f"{d} holds both {RULE_SECTION_FILE} and {LEGACY_RULE_SECTION_FILE} — "
                         "a half-finished conversion; remove the one that is not the run")
    if new.exists():
        return new
    if old.exists():
        return old
    raise SystemExit(f"no rule_section.jsonl(.gz) in the reach run at {d} — re-run the reach "
                     f"builder:\n    python -m pipeline.atlas.reach.cli --build <atlas> --out {d}")


def _legacy_row(line: str) -> tuple[str, str, str, str]:
    """(entry_id, rule_id, section_id, scope) of a pre-T73 line. The writer's own form (sorted keys,
    compact) is split on its quotes — 20 GB through `json.loads` was most of a read's time; any
    other line is parsed as JSON."""
    p = line.split('"')
    if (len(p) == 17 and p[1] == "entry_id" and p[5] == "rule_id" and p[9] == "scope"
            and p[13] == "section_id" and "\\" not in line):
        return p[3], p[7], p[15], p[11]
    r = json.loads(line)
    return r["entry_id"], r["rule_id"], r["section_id"], r.get("scope", "")


def _rows_unchecked(path: Path) -> Iterator[tuple[int, tuple[str, str, str, str]]]:
    if path.name.endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            for n, line in enumerate(fh, 1):
                if not line.strip():
                    continue
                g = json.loads(line)
                eid, rid, trib = g["entry_id"], g["rule_id"], g.get("trib") or ()
                tset = frozenset(trib)
                if not tset <= frozenset(g["sections"]):
                    raise ValueError(f"{path}:{n} lists tributary sections it does not bind")
                for s in g["sections"]:
                    yield n, (eid, rid, s, "trib" if s in tset else "reach")
    else:
        with path.open(encoding="utf-8") as fh:
            for n, line in enumerate(fh, 1):
                if line.strip():
                    yield n, _legacy_row(line)


def iter_rule_sections(path: str | Path) -> Iterator[tuple[str, str, str, str]]:
    """Every binding row of a bindings file (either form) as (entry_id, rule_id, section_id, scope),
    in `write_run`'s order (entry, rule, section). A file out of that order — or with a row twice —
    is REFUSED, never misread: the merge-walks that read it (`binding_diff`, the parity gate) are
    only right on sorted input."""
    path = Path(path)
    last = None
    for n, row in _rows_unchecked(path):
        key = row[:3]
        if last is not None and key <= last:
            raise ValueError(f"{path}:{n} is out of order ({key} after {last}) — not a "
                             "`write_run` table")
        last = key
        yield row


def write_rule_sections(path: str | Path, rows: Iterable[tuple[str, str, str, str]]) -> int:
    """Write (entry_id, rule_id, section_id, scope) rows, SORTED as `iter_rule_sections` yields
    them, in the T73 form. Returns the row count. Byte-identical for identical rows (gzip mtime 0,
    no file name in the header)."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    n = 0
    with open(tmp, "wb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0,
                                               compresslevel=6) as gz:
        for (eid, rid), grp in groupby(rows, key=lambda r: (r[0], r[1])):
            secs, trib = [], []
            for _, _, s, scope in grp:
                secs.append(s)
                if scope == "trib":
                    trib.append(s)
                elif scope != "reach":
                    raise ValueError(f"{eid}#{rid}: scope {scope!r} is neither reach nor trib")
            n += len(secs)
            gz.write(_line({"entry_id": eid, "rule_id": rid, "sections": secs,
                            "trib": trib}).encode("utf-8"))
    tmp.replace(path)
    return n


def write_run(out_dir: str | Path, result, entries=None, *,
              stamps: dict | None = None) -> dict[str, int]:
    """Write the rule tables, the licensing tables and report.json. Returns row counts per table.
    `stamps`: further digests for report.json (the CLI's `registry_digest`, P2)."""
    out = Path(out_dir)

    # rule_section — the bindings. One row per (rule, section) so it joins cleanly.
    #
    # `scope` says WHY this rule reaches this section: "reach" if the rule names this water,
    # "trib" if it arrived by the tributary walk. It is not decoration. A tributary sweep is
    # the part of this pipeline that can be wrong over thousands of kilometres at once — the
    # Kootenay case in tributaries.py is 3,205 km of water that a prefix shortcut would have
    # closed — and a row that cannot say how it got here cannot be audited by anyone.
    # (T73) written per bound rule, not per row (see the module note), and generated rule by rule
    # rather than as 149 M row objects: the same rows, in the same (entry, rule, section) order.
    by_rule: dict[tuple[str, str], list] = {}
    for b in result.bindings:
        if b.outcome is Outcome.bound:
            trib = frozenset(b.via_tributary)
            by_rule.setdefault((b.entry_id, b.rule_id), []).extend(
                (s, "trib" if s in trib else "reach") for s in b.sections)
    n_sections = sum(len(v) for v in by_rule.values())

    def sections():
        for (eid, rid) in sorted(by_rule):
            for s, scope in sorted(by_rule[(eid, rid)]):
                yield eid, rid, s, scope

    # rule_unresolved — EVERY failure, typed. The table that must never be silently short.
    unresolved = [
        {"entry_id": b.entry_id, "rule_id": b.rule_id,
         "reason": b.reason.value if b.reason else None, "detail": b.detail}
        for b in result.bindings if b.outcome is Outcome.unresolved
    ]
    unresolved.sort(key=lambda r: (r["entry_id"], r["rule_id"]))

    # rule_extent — the AUTHORED form, kept beside the resolution so a binding can be
    # explained and rebuilt without re-deriving it from geometry.
    extents: list[dict] = []
    for e in iter_entries(entries or []):
        for rule in e.get("rules") or []:
            for seq, ex in enumerate(rule.get("extents") or []):
                extents.append({
                    "entry_id": e["entry_id"], "rule_id": rule["rule_id"], "seq": seq,
                    "op": ex.get("op"), "splits": list(ex.get("splits") or ()),
                    "item_id": ex.get("item_id"), "item_ids": list(ex.get("item_ids") or ()),
                    "area_id": ex.get("area_id"),
                })
    extents.sort(key=lambda r: (r["entry_id"], r["rule_id"], r["seq"]))

    diagnostics = [
        {"entry_id": d.entry_id, "rule_id": d.rule_id, "kind": d.kind,
         "payload": json.dumps(d.payload, sort_keys=True)}
        for d in result.diagnostics
    ]
    diagnostics.sort(key=lambda r: (r["entry_id"], r["rule_id"], r["kind"], r["payload"]))

    tables = {"rule_unresolved": unresolved, "rule_extent": extents, "rule_diagnostic": diagnostics}

    # licensing_placement — EVERY placed record, once, with where it ended up. The table that
    # says a record exists at all; `licensing_section` only says where the bound ones are.
    lic = list(result.licensing)
    tables["licensing_placement"] = [
        {"entry_id": p.entry_id, "record_id": p.record_id, "kind": p.kind,
         "placement": p.placement, "reason": p.reason, "detail": p.detail,
         "tributaries_pending": p.tributaries_pending}
        for p in lic]
    # licensing_section — one row per (record, section), `scope` as the rules have it, plus
    # `trib_pending` for a walk that was not done: a pending binding is never complete.
    tables["licensing_section"] = sorted((
        {"entry_id": p.entry_id, "record_id": p.record_id, "kind": p.kind, "section_id": s,
         "scope": ("trib_pending" if p.tributaries_pending
                   else "trib" if s in trib else "reach")}
        for p in lic if p.placement == "sections"
        for trib in (frozenset(p.via_tributary),)
        for s in p.sections),
        key=lambda r: (r["entry_id"], r["record_id"], r["section_id"]))
    tables["licensing_diagnostic"] = sorted((
        {"entry_id": d.entry_id, "record_id": d.rule_id, "kind": d.kind,
         "payload": json.dumps(d.payload, sort_keys=True)}
        for d in result.licensing_diagnostics),
        key=lambda r: (r["entry_id"], r["record_id"], r["kind"], r["payload"]))
    tables[STEELHEAD_TABLE] = list(getattr(result, "steelhead", None) or [])
    tables[TIDAL_TABLE] = [{"section_id": s, "entry_id": e}
                           for s, e in sorted((getattr(result, "tidal", None) or {}).items())]
    tables[OUTSIDE_TABLE] = [{"section_id": s}
                             for s in sorted(getattr(result, "outside", None) or ())]
    for name, rows in tables.items():
        _write_jsonl(out / f"{name}.jsonl", rows)
    legacy = out / LEGACY_RULE_SECTION_FILE
    if legacy.exists():                     # a rerun into a pre-T73 run's directory
        legacy.unlink()
    write_rule_sections(out / RULE_SECTION_FILE, sections())

    report = asdict(result.report)
    report["digest"] = digest(result)
    report["licensing_digest"] = licensing_digest(result)
    report["steelhead_digest"] = _table_digest(tables[STEELHEAD_TABLE])
    report["tidal_digest"] = _table_digest(tables[TIDAL_TABLE])
    report["outside_digest"] = _table_digest(tables[OUTSIDE_TABLE])
    if isinstance(entries, dict):           # the corpus the run was made from (P2)
        from pipeline.regs.parsing.io import corpus_digest, entry_digests
        report["entries_digest"] = corpus_digest(entries)
        # per entry, so the review tool can say WHICH entries this run was made from (T133)
        report["entry_digests"] = entry_digests(entries)
    report.update(stamps or {})
    (out / "report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"rule_section": n_sections, **{k: len(v) for k, v in tables.items()}}


def _table_digest(rows: list[dict]) -> str:
    return hashlib.sha256(json.dumps(
        rows, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]


def read_table(run_dir: str | Path, name: str) -> list[dict]:
    """One of the run's JSONL tables, as rows. RAISES when the file is absent — a run that
    predates the table is not a run with none of it (a missing `tidal` would read as "nothing is
    tidal", and Nitinat Lake would be open under the general rules)."""
    p = Path(run_dir) / f"{name}.jsonl"
    if not p.exists():
        raise SystemExit(f"{p} is missing — the reach run predates the `{name}` table. Re-run the "
                         f"reach builder:\n    python -m pipeline.atlas.reach.cli --build <atlas> "
                         f"--out {run_dir}")
    with p.open(encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def digest(result) -> str:
    """Content hash of the BINDINGS — the thing that must not change between identical
    builds. Excludes timings and anything else that varies for non-data reasons."""
    payload = [
        [b.entry_id, b.rule_id, b.outcome.value, list(b.sections),
         b.reason.value if b.reason else None]
        for b in result.bindings
    ]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:16]


def licensing_digest(result) -> str:
    """Content hash of the licensing placements — kept apart from `digest`, so adding licensing
    to a run did not move the rules' digest and a rules diff stays a rules diff."""
    payload = [
        [p.entry_id, p.record_id, p.kind, p.placement, list(p.sections), p.reason,
         p.tributaries_pending]
        for p in result.licensing
    ]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:16]


def read_run(run_dir: str | Path) -> dict[tuple[str, str], dict]:
    """Read a previous run back as ``{(entry_id, rule_id): {...}}`` — for `diff`."""
    run = Path(run_dir)
    out: dict[tuple[str, str], dict] = {}

    for eid, rid, s, _scope in iter_rule_sections(rule_section_file(run)):
        key = (eid, rid)
        out.setdefault(key, {"outcome": "bound", "sections": set(), "reason": None})
        out[key]["sections"].add(s)

    p = run / "rule_unresolved.jsonl"
    if p.exists():
        for line in p.read_text(encoding="utf-8").splitlines():
            if not line:
                continue
            r = json.loads(line)
            out[(r["entry_id"], r["rule_id"])] = {
                "outcome": "unresolved", "sections": set(), "reason": r["reason"]}
    return out


def convert(run_dir: str | Path, to: str | Path | None = None) -> dict:
    """(T73) Rewrite a pre-T73 run's `rule_section.jsonl` as `rule_section.jsonl.gz`, PROVE the two
    hold the same rows (both streamed, side by side), then remove the old file. Returns the sizes,
    rows and seconds. A run already converted is refused (nothing to do), and on any mismatch the
    new file is removed and the old one kept.

    `to`: write the converted run to ANOTHER directory instead (its other files copied), leaving
    `run_dir` untouched — so `binding_diff run_dir to` can prove the two forms bind the same."""
    import shutil
    d = Path(run_dir)
    old = d / LEGACY_RULE_SECTION_FILE
    if not old.exists():
        raise SystemExit(f"{old} is missing — nothing to convert"
                         + (" (the run is already in the T73 form)" if (d / RULE_SECTION_FILE).exists() else ""))
    if to is not None:
        dest = Path(to)
        if dest.exists() and any(dest.iterdir()):
            raise SystemExit(f"{dest} is not empty")
        dest.mkdir(parents=True, exist_ok=True)
        for f in sorted(d.iterdir()):
            if f.is_file() and f.name != LEGACY_RULE_SECTION_FILE:
                shutil.copy2(f, dest / f.name)
    else:
        dest = d
    new = dest / RULE_SECTION_FILE
    if new.exists():
        raise SystemExit(f"{new} exists beside {old} — remove the half-finished one first")
    t0 = time.time()
    n = write_rule_sections(new, iter_rule_sections(old))
    t1 = time.time()
    try:
        m = 0
        for a, b in zip(iter_rule_sections(old), iter_rule_sections(new), strict=True):
            if a != b:
                raise ValueError(f"row {m + 1}: {a} became {b}")
            m += 1
    except ValueError:
        new.unlink()
        raise
    out = {"run": str(d), "rows": n, "old_bytes": old.stat().st_size, "new_bytes": new.stat().st_size,
           "write_seconds": round(t1 - t0, 1), "verify_seconds": round(time.time() - t1, 1)}
    if to is None:
        old.unlink()
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="reach run files (T73)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("convert", help="rewrite a pre-T73 run's rule_section.jsonl as "
                                       "rule_section.jsonl.gz (verified, then the old file removed)")
    c.add_argument("run", nargs="+")
    c.add_argument("--to", help="write the converted run here instead (one run only); the source is kept")
    a = ap.parse_args(argv)
    if a.to and len(a.run) != 1:
        ap.error("--to takes one run")
    for run in a.run:
        print(json.dumps(convert(run, a.to)), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
