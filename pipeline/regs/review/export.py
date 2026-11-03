"""T266: THE ONE EXPORTER — the review marks in force -> per-domain DECIDED-DATA files.

    python -m pipeline.regs.review.export            # write data/curated/decided/*.json
    python -m pipeline.regs.review.export --check    # exit 1 when the files are not export(marks)

`marks.jsonl` is REVIEW STATE (who checked what, when, the verdicts, the history). What the marks
DECIDE is data, and lives in its own files, one per domain, each record with its provenance:

    tidal.json            scope tidal: the tidal boundaries (+ the owner's calls made before the
                          Tidal tab, folded in first when a file is given; none since T326)
    dfo.json              scope dfo: DFO locations' extent fixes and their new cuts
    entries.json          scopes entry / rule: extent fixes, tributary calls, lake-split-later
    splits.json           scope split: calls on existing cuts
    sloughs.json          scope slough: stream / not stream (T55)
    ponds.json            scope q154: pool / real lake (Q154) — CLOSED 2026-10-30 (T329), so not written
    names.json            scope name: accept / reject (T224)
    proposed_splits.json  every distinct cut any uncleared mark proposed (`marks.proposal_groups`,
                          the shared-cut history; superseded marks' cuts too, as the store holds them)

A record is the mark IN FORCE on its key (`marks.latest`: the last one, a CLEARED key has none) —
its whole line (the decision and the prints it was held against, which the readers need to tell
a stale decision) — and `provenance`: {mark: sha256 of its line (12 hex), line: its line number,
reviewer, date, source}. `rank` is the record's place in `latest`'s order (first appearance of the
key in the store), which the proposals are made in; append-only, so a new key only adds at the end.

THE READERS (apply paths) read these files, never marks.jsonl: `proposals` / `apply_proposals`
(extent fixes, tributaries, new splits, tidal flags -> splits.json, the entries, the DFO files),
`tools.mined_names` (names -> waters/mined_names.json), `dfo_salmon.suggest`, `tools.audit.tidal_preview`.
The BUILDS (atlas, reach, tiles, deliver) read neither: they read the curated data those land in.
Review-state tools (gate, tickets, precheck, carry, the review app) still read the marks.

CLOSED DOMAINS (T325, `review.closed`): a domain on the closed list is not exported — its file is not written, and
a leftover one is removed — and `load` does not ask for it. Its marks stay in marks.jsonl (never edited) and still
count in `rank`, so closing or reopening one domain moves no byte of another's file.

Deterministic and idempotent (no clock in a file); `--check` is the test that the committed files
are export(marks). REFUSED (never written) on a conflict: two tidal decisions in force that cut the
same water at different points.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Mapping, Optional, Sequence

from pipeline.regs.review import marks as M

#: domain -> the mark scopes it holds
DOMAINS: dict[str, tuple[str, ...]] = {
    "tidal": ("tidal",), "dfo": ("dfo",), "entries": ("entry", "rule"), "splits": ("split",),
    "sloughs": ("slough",), "ponds": ("q154",), "names": ("name",),
    # T331: the retired accept step's marks (scope "proposal", 7, 2026-10-27) — a domain only so it can be CLOSED
    "proposals": ("proposal",),
}
PROPOSED = "proposed_splits"
FILES = tuple(DOMAINS) + (PROPOSED,)
FORMAT = "decided/1"


def default_dir() -> Path:
    from pipeline.common.curated import CURATED
    return Path(CURATED.decided)


def _rel(p: Path) -> str:
    from pipeline.common.curated import REPO_ROOT
    try:
        return str(Path(p).resolve().relative_to(REPO_ROOT.resolve()))
    except ValueError:
        return str(p)


def _line_dict(m: M.Mark) -> dict:
    return json.loads(M.line(m))


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


@dataclass(frozen=True)
class Decided:
    """What the marks decide: the marks in force (in `marks.latest` order) and the shared proposed
    cuts (`marks.proposal_groups`), with each in-force mark's provenance."""
    inforce: tuple                                   # Mark, in latest() order
    groups: tuple                                    # proposal_groups rows
    provenance: Mapping = field(default_factory=dict)  # mark key -> provenance dict
    #: the owner's pre-Tab tidal calls file folded in first (repo-relative), or None
    tidal_decisions: Optional[str] = None

    def latest(self) -> dict:
        return {m.key: m for m in self.inforce}

    def scope(self, *scopes: str) -> list:
        return [m for m in self.inforce if m.scope in scopes]

    def shared(self, overrides: Sequence = ()) -> list[dict]:
        """The proposed splits with their confidence (`marks.proposals`)."""
        return M.with_confidence(self.groups, overrides)


def decide(marks: Iterable[M.Mark], *, tidal_decisions: Optional[Path] = None,
           source: str = "marks.jsonl", candidates: Optional[Path] = None) -> Decided:
    """export(marks) in memory. `tidal_decisions`: a file of tidal calls made before the Tidal tab, folded in
    before the marks (None, the default since T326: the owner's calls live in `waters/tidal.json` as rows with no
    mark, and the three pre-tab calls are all superseded by marks)."""
    from pipeline.regs.review import proposals as P
    early = P.tidal_decision_marks(tidal_decisions, candidates) if tidal_decisions else []
    store = list(marks)
    history = early + store
    prov: dict[tuple, dict] = {}
    for m in early:
        prov[m.key] = {"mark": _sha(M.line(m)), "line": None, "reviewer": m.reviewer, "date": m.date,
                       "source": _rel(Path(tidal_decisions))}
    for i, m in enumerate(store, 1):
        prov[m.key] = {"mark": _sha(M.line(m)), "line": i, "reviewer": m.reviewer, "date": m.date,
                       "source": source}
    inforce = tuple(M.latest(history).values())
    d = Decided(inforce=inforce, groups=tuple(M.proposal_groups(history)),
                provenance={m.key: prov[m.key] for m in inforce},
                tidal_decisions=_rel(Path(tidal_decisions)) if tidal_decisions else None)
    conflicts = refusals(d)
    if conflicts:
        raise ValueError("review.export: conflicting decisions — nothing exported:\n  " + "\n  ".join(conflicts))
    return d


def refusals(d: Decided) -> list[str]:
    """Conflicts the export refuses: two tidal DECISIONS (verified, one cut) in force on one water whose
    cuts are not the same cut (`marks.same_anchor`)."""
    from pipeline.regs.review import proposals as P
    by_water: dict[str, list] = {}
    for m in d.inforce:
        if P._tidal_decides(m) and m.new_splits[0].on_item:
            by_water.setdefault(m.new_splits[0].on_item, []).append(m)
    out = []
    for water, ms in sorted(by_water.items()):
        for a, b in zip(ms, ms[1:]):
            if not M.same_anchor(a.new_splits[0], b.new_splits[0]):
                out.append(f"{water}: tidal {a.target} ({a.date}) and {b.target} ({b.date}) cut it at different points")
    return out


# --------------------------------------------------------------------------- files

def _dump_rows(head: dict, key: str, rows: list) -> str:
    """One record per line inside a JSON document (diff-friendly, still one json.loads)."""
    top = json.dumps(head, ensure_ascii=False, sort_keys=True)[:-1]
    body = ",\n".join(json.dumps(r, ensure_ascii=False, sort_keys=True) for r in rows)
    return f"{top}, \"{key}\": [\n{body}\n]}}\n" if rows else f"{top}, \"{key}\": []}}\n"


def documents(d: Decided, closed: Iterable[str] = ()) -> dict[str, str]:
    """{file name: text} for every domain file, a CLOSED domain's left out (T325)."""
    out = {}
    closed = frozenset(closed)
    for dom, scopes in DOMAINS.items():
        if dom in closed:
            continue
        rows = [{"key": [m.scope, m.target, m.rule_id], "rank": i, "provenance": d.provenance[m.key],
                 "mark": _line_dict(m)} for i, m in enumerate(d.inforce) if m.scope in scopes]
        head = {"format": FORMAT, "domain": dom, "scopes": list(scopes), "tidal_decisions": d.tidal_decisions,
                "_about": "T266: the review marks in force for these scopes, exported by "
                          "`python -m pipeline.regs.review.export` — never edit by hand; marks.jsonl is the review log"}
        out[f"{dom}.json"] = _dump_rows(head, "decisions", rows)
    rows = [{"id": g["id"], "split": _split_json(g["split"]),
             "members": [_split_json(n) for n in g["members"]],
             "marks": [dict(w) for w in g["marks"]]} for g in d.groups]
    head = {"format": FORMAT, "domain": PROPOSED, "tidal_decisions": d.tidal_decisions,
            "_about": "T266: every distinct cut an uncleared mark proposed (marks.proposal_groups), in the order "
                      "first proposed — exported by `python -m pipeline.regs.review.export`"}
    out[f"{PROPOSED}.json"] = _dump_rows(head, "groups", rows)
    return out


def write(d: Decided, out_dir: Path, closed: Iterable[str] = ()) -> list[Path]:
    """Write every open domain's file; REMOVE a closed domain's leftover file (T325 — the exporter is the one writer
    of decided/, so it is the one that stops holding a closed domain)."""
    from pipeline.regs.parsing.io import atomic_write
    out_dir = Path(out_dir)
    written = []
    for name, text in documents(d, closed).items():
        p = out_dir / name
        if not p.exists() or p.read_text(encoding="utf-8") != text:
            atomic_write(p, text)
            written.append(p)
    for dom in sorted(set(closed) & set(DOMAINS)):
        p = out_dir / f"{dom}.json"
        if p.exists():
            p.unlink()
            written.append(p)
    return written


def load(out_dir: Optional[Path] = None, closed: Optional[Iterable[str]] = None) -> Decided:
    """The decided data as the readers take it. A missing file RAISES (AGENTS 37), naming the command. A CLOSED
    domain (`closed`; default the curated closed list, T325) is not read: it has no file, and nothing applies it."""
    from pipeline.regs.review import closed as C
    out_dir = Path(out_dir or default_dir())
    shut = C.domains() if closed is None else frozenset(closed)
    inforce, td = [], None
    for dom in DOMAINS:
        if dom in shut:
            continue
        p = out_dir / f"{dom}.json"
        if not p.exists():
            raise FileNotFoundError(f"no decided-data file {p} — make it: python -m pipeline.regs.review.export")
        doc = json.loads(p.read_text(encoding="utf-8"))
        if doc.get("format") != FORMAT:
            raise ValueError(f"{p}: format {doc.get('format')!r}, expected {FORMAT}")
        td = doc.get("tidal_decisions")
        for r in doc["decisions"]:
            inforce.append((r["rank"], r["provenance"], M.Mark.model_validate(r["mark"])))
    inforce.sort(key=lambda t: t[0])          # `rank`: the record's place in `marks.latest` order, across domains
    p = out_dir / f"{PROPOSED}.json"
    if not p.exists():
        raise FileNotFoundError(f"no decided-data file {p} — make it: python -m pipeline.regs.review.export")
    gdoc = json.loads(p.read_text(encoding="utf-8"))
    groups = tuple({"id": g["id"], "split": M.NewSplit.model_validate(g["split"]),
                    "members": [M.NewSplit.model_validate(n) for n in g["members"]],
                    "marks": [dict(w) for w in g["marks"]]} for g in gdoc["groups"])
    return Decided(inforce=tuple(m for _r, _p, m in inforce), groups=groups,
                   provenance={m.key: pr for _r, pr, m in inforce}, tidal_decisions=td)


def _split_json(n) -> dict:
    """A NewSplit as the export writes it: the T355 area fields left out while unset (the file it was)."""
    got = json.loads(n.model_dump_json())
    for k in M.NEW_SPLIT_OPTIONAL[2:]:          # area / area_end stay as they were written
        if got.get(k) is None:
            got.pop(k, None)
    return got


def stale(out_dir: Optional[Path] = None, marks_path: Optional[Path] = None) -> list[str]:
    """The decided files that are not export(marks) now ([] when current). The tidal calls file is the one the
    files name (`tidal_decisions`); with no files, the default."""
    from pipeline.common.curated import CURATED, REPO_ROOT
    out_dir = Path(out_dir or default_dir())
    td: object = None
    if (out_dir / "tidal.json").exists():
        named = json.loads((out_dir / "tidal.json").read_text(encoding="utf-8")).get("tidal_decisions")
        td = REPO_ROOT / named if named else None
    from pipeline.regs.review import closed as C
    shut = C.domains(C.path(marks_path))
    want = documents(decide(M.load(marks_path or CURATED.regulations.review.marks), tidal_decisions=td), shut)
    return [n for n, t in sorted(want.items())
            if not (out_dir / n).exists() or (out_dir / n).read_text(encoding="utf-8") != t] + [
        f"{dom}.json (closed: remove it)" for dom in sorted(shut & set(DOMAINS)) if (out_dir / f"{dom}.json").exists()]


def main(argv: Optional[list[str]] = None) -> int:
    from pipeline.common.curated import CURATED
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.review.export", description=__doc__.split("\n")[0])
    ap.add_argument("--marks", type=Path, default=None, help="the marks store (default: CURATED's)")
    ap.add_argument("--out", type=Path, default=None, help="the decided-data dir (default: CURATED.decided)")
    ap.add_argument("--check", action="store_true", help="exit 1 when the files are not export(marks); write nothing")
    a = ap.parse_args(argv)
    from pipeline.regs.review import closed as C
    out = Path(a.out or default_dir())
    shut = C.domains(C.path(a.marks))
    d = decide(M.load(a.marks or CURATED.regulations.review.marks))
    if a.check:
        bad = stale(out, a.marks)
        print("decided data is current" if not bad else
              "decided data is OUT OF DATE: " + ", ".join(bad) + " — python -m pipeline.regs.review.export")
        return 1 if bad else 0
    got = write(d, out, shut)
    n = {dom: len(d.scope(*sc)) for dom, sc in DOMAINS.items() if dom not in shut}
    print(f"review.export: {sum(n.values())} decisions in force ({', '.join(f'{k} {v}' for k, v in n.items())}), "
          f"{len(d.groups)} proposed cuts -> {_rel(out)} ({len(got)} file(s) changed)"
          + (f"; closed, not exported: {', '.join(sorted(shut))}" if shut else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
