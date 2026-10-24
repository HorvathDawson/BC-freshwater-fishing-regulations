"""THE ARTIFACT'S MARKS, BACK INTO THE REPO (T184 phase 1): the review page's db docs (collection "marks")
become marks in the repo's store, in exactly the shape `POST /api/marks` writes (`marks.Mark`, one line
each, `marks.line`), so `gate pilot-report`, `tickets` and `commit_marks` read them unchanged.

    python -m pipeline.regs.review.import_artifact_marks --docs <dir of <doc_id>.json | one list .json> \
        --export curation-review/artifact/out/export.json [--reviewers ids.json] [--marks <store>] [--dry-run]

A doc is what the page wrote: {scope, target, rule_id, verdict, answers, problem, note, seconds,
reviewer (the viewer's `user` id), fp, pages, at}. What the doc does NOT decide:
  - the PRINTS: looked up in the export by the doc's `fp` (`export_artifact.fp_of`) — the vintage the
    reviewer saw. A doc whose fp is not in the export, or names another target than the export's unit
    with that fp, is refused (the doc is viewer-written, untrusted data).
  - the book's sha256: the export's (`pdf_sha256`).
  - the verdict: re-derived from the answers (`marks.verdict_from_answers`); a doc whose verdict the
    answers do not give is refused (deferred stays the reviewer's own call, with a note). The model
    (`Mark._consistent`) then refuses what `POST /api/marks` would.
  - the reviewer's NAME: the page stores only ids; `--reviewers` maps id -> name, otherwise the mark
    names `artifact:<id>`.
`date` is the doc's `at` (its UTC day); `seconds` is the page's clock, as the screen's.

IDEMPOTENT: a mark whose line is already in the store is not appended again, so the import can be run
after every pass. A doc changed since (the reviewer re-marked) appends its new mark — the store is the
history, and the last mark is the one in force (`marks.latest`).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from pipeline.regs.review import marks as M


def read_docs(path: Path) -> list[dict]:
    """The docs, from a directory of `<doc_id>.json` files or one JSON file (a list, or `{docs: [...]}`).
    A doc wrapped as ArtifactData returns it ({id, data: {...}}) is unwrapped."""
    path = Path(path)
    raw: list = []
    if path.is_dir():
        for p in sorted(path.glob("*.json")):
            got = json.loads(p.read_text(encoding="utf-8"))
            raw += got if isinstance(got, list) else [got]
    else:
        got = json.loads(path.read_text(encoding="utf-8"))
        raw = got.get("docs", got.get("documents", [])) if isinstance(got, Mapping) else got
    out = []
    for d in raw:
        if isinstance(d, Mapping) and "scope" not in d and isinstance(d.get("data"), Mapping):
            d = {**d["data"], "_id": d.get("id") or d.get("doc_id")}
        out.append(dict(d))
    return out


def _units(export: Mapping) -> dict[str, dict]:
    """fp -> the unit the export says it is (scope, target, rule_id, pages)."""
    out = {}
    for t in export["targets"]:
        for u in t["units"]:
            out[u["fp"]] = {"scope": u["scope"], "target": t["id"], "rule_id": u.get("rule_id"),
                            "pages": t.get("pages") or []}
    return out


def to_mark(doc: Mapping, export: Mapping, reviewers: Mapping[str, str] | None = None) -> M.Mark:
    """One page doc -> the repo mark. Raises ValueError (in the reviewer's words where the model has
    them) for a doc the store would refuse."""
    units = _units(export)
    fp = doc.get("fp")
    unit = units.get(fp)
    if unit is None or fp not in export["prints"]:
        raise ValueError(f"fp {fp!r} is not a unit of this export (another vintage's page?)")
    for k in ("scope", "target", "rule_id"):
        if (doc.get(k) or None) != (unit[k] or None):
            raise ValueError(f"{k} {doc.get(k)!r} is not the export's {unit[k]!r} for fp {fp}")
    answers = {k: v for k, v in (doc.get("answers") or {}).items()}
    verdict = doc.get("verdict")
    if verdict != "deferred":
        derived = M.verdict_from_answers(unit["scope"], answers)
        if derived is None:
            raise ValueError(M.unanswered_words(unit["scope"]))
        if verdict != derived:
            raise ValueError(f"verdict {verdict!r} is not what the answers give ({derived!r})")
    rid = doc.get("reviewer")
    if not rid or not str(rid).strip():
        raise ValueError(M.refusal_words("", "reviewer"))
    name = (reviewers or {}).get(rid) or f"artifact:{rid}"
    at = str(doc.get("at") or "")
    try:
        day = dt.datetime.fromisoformat(at.replace("Z", "+00:00")).astimezone(dt.timezone.utc).date().isoformat()
    except ValueError:
        raise ValueError(f"at {at!r} is not an ISO time")
    pages = [p for p in (doc.get("pages") or unit["pages"]) if isinstance(p, int)]
    seconds = doc.get("seconds")
    body = {"scope": unit["scope"], "target": unit["target"], "rule_id": unit["rule_id"],
            "verdict": verdict, "answers": answers, "problem": doc.get("problem") or None,
            "note": doc.get("note") or "", "reviewer": name, "date": day,
            "pdf_sha256": export["pdf_sha256"], "pages": pages, "prints": export["prints"][fp],
            "seconds": float(seconds) if isinstance(seconds, (int, float)) else None}
    from pydantic import ValidationError
    try:
        return M.Mark.model_validate(body)
    except ValidationError as ex:
        raise ValueError("; ".join(M.refusal_words(e["msg"], str(e["loc"][0]) if e["loc"] else None)
                                   for e in ex.errors()))


def import_docs(docs: Sequence[Mapping], export: Mapping, store: Path, *,
                reviewers: Mapping[str, str] | None = None, dry_run: bool = False) -> dict:
    """Append every doc's mark not already in the store. Returns {added, already, refused: [...]}.
    Docs are taken in the order of their `at` (a re-mark after an import lands after it)."""
    have = set(Path(store).read_text(encoding="utf-8").splitlines()) if Path(store).exists() else set()
    M.load(store)                                       # the store is checked in: raise if absent / bad
    new, already, refused, seen = [], 0, [], set()
    for d in sorted(docs, key=lambda d: (str(d.get("at") or ""), str(d.get("_id") or ""))):
        try:
            m = to_mark(d, export, reviewers)
        except ValueError as ex:
            refused.append({"doc": d.get("_id") or f"{d.get('scope')}:{d.get('target')}:{d.get('rule_id')}",
                            "why": str(ex)})
            continue
        ln = M.line(m).rstrip("\n")
        if ln in have or ln in seen:
            already += 1
            continue
        seen.add(ln)
        new.append(m)
    if new and not dry_run:
        M.append(store, new)
    return {"added": len(new), "already": already, "refused": refused,
            "marks": [{"scope": m.scope, "target": m.target, "rule_id": m.rule_id, "verdict": m.verdict,
                       "reviewer": m.reviewer} for m in new]}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.review.import_artifact_marks",
                                 description=__doc__.split("\n\n")[0])
    ap.add_argument("--docs", type=Path, required=True, help="a dir of <doc_id>.json, or one JSON list")
    ap.add_argument("--export", type=Path, required=True, help="the export the page was built from")
    ap.add_argument("--reviewers", type=Path, help="JSON {user id: name}")
    ap.add_argument("--marks", type=Path, help="the store (default: the curated one)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    if a.marks is None:
        from pipeline.common.curated import CURATED
        a.marks = Path(CURATED.regulations.review.marks)
    export = json.loads(a.export.read_text(encoding="utf-8"))
    reviewers = json.loads(a.reviewers.read_text(encoding="utf-8")) if a.reviewers else None
    got = import_docs(read_docs(a.docs), export, a.marks, reviewers=reviewers, dry_run=a.dry_run)
    print(json.dumps({k: v for k, v in got.items() if k != "marks"}, indent=1))
    for m in got["marks"]:
        print(f"  {'would add' if a.dry_run else 'added'} {m['scope']} {m['target']}"
              f"{'#' + m['rule_id'] if m['rule_id'] else ''}: {m['verdict']} ({m['reviewer']})")
    return 1 if got["refused"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
