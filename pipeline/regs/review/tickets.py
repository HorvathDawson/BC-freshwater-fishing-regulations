"""THE TICKET EXPORT (T135, curation-review PLAN §4 phase 4): every "wrong" and "unsure" mark in
force becomes ticket text the owner files (PLAN §6 Q3: reviewers mark, the owner fixes).

    ticket_text(mark, ...)   one mark -> a Markdown ticket: what was marked, the answers, the
                             problem, the reviewer's note, the entry or split it is about, and the
                             work (fix in fix mode, then the rebuild that ships it)
    parse_ticket(text)       the mark's fields back from a ticket — THE ROUND TRIP: what the owner
                             files is exactly what the reviewer said (`test_review_queue.py`)
    export(marks, ...)       the marks in force whose verdict is wrong or unsure, as tickets

A ticket names the mark it came from (scope, target, rule, date, reviewer); the store is append-
only, so the mark stays the record and the ticket is its copy for the work list.

CLI (reads the marks store, the catalogue and the vintage on disk for what changed since; writes
only into `--out`, or prints — the same text as the review app's `/api/review/tickets`):

    python -m pipeline.regs.review.tickets                    # every ticket, to stdout
    python -m pipeline.regs.review.tickets --out <dir>        # one file per ticket
    python -m pipeline.regs.review.tickets --verdict wrong    # only wrong (or unsure)
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Iterable, Mapping, Sequence

#: the verdicts that become tickets
TICKET_VERDICTS = ("wrong", "unsure")
#: the checks' words, as the review screen asks them (PLAN §2)
CHECK_WORDS = {"T": "text", "R": "rules", "P": "place", "A": "answer"}
#: what separates tickets in one stream (stdout, the API's one-file form)
SEPARATOR = "\n\n---8<--- ticket ---8<---\n\n"
_NO_NOTE = "_(no note)_"
_FIELD = re.compile(r"^- \*\*(?P<k>[^*]+):\*\* (?P<v>.*)$")


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")[:80]


def ticket_name(mark) -> str:
    """The file a ticket is written to: `review-<verdict>-<scope>-<target>[-<rule>].md`."""
    parts = [mark.verdict, mark.scope, mark.target] + ([mark.rule_id] if mark.rule_id else [])
    return "review-" + "-".join(_slug(p) for p in parts) + ".md"


def _quote(text: str) -> list[str]:
    if not text:
        return [_NO_NOTE]
    return [("> " + ln) if ln else ">" for ln in text.split("\n")]


def _unquote(lines: list[str]) -> str:
    if lines == [_NO_NOTE]:
        return ""
    out = []
    for ln in lines:
        if ln == ">":
            out.append("")
        elif ln.startswith("> "):
            out.append(ln[2:])
        else:
            raise ValueError(f"not a line of the reviewer's note: {ln!r}")
    return "\n".join(out)


def _work(mark) -> list[str]:
    from pipeline.regs.review.vintage import Waiting, rebuild_commands
    split = mark.scope == "split"
    cmds = rebuild_commands([Waiting.split_moved if split else Waiting.reach_entry])
    checks = [k for k, v in mark.answers.items() if v in ("n", "?")]
    what = ", ".join(f"{k} ({CHECK_WORDS[k]})" for k in sorted(checks, key="TRPA".index))
    lines = [f"1. {'Decide' if mark.verdict == 'unsure' else 'Fix'} {what or 'the mark'}"
             + (" — the owner's call (PLAN §6 Q3)." if mark.verdict == "unsure" else "."),
             "2. Fix mode: `bash curation-review/run.sh --fix` "
             + ("(the split card: move / delete / rename)." if split
                else "(the entry's editors; a cut pick is the extent's `cut_pick`)."),
             "3. Rebuild, then the mark goes stale and the target re-queues for review:"]
    lines += [f"   - `{c}`" for c in cmds]
    return lines


def ticket_text(mark, *, entry: Mapping | None = None, split: Mapping | None = None,
                status: Mapping | None = None) -> str:
    """One mark as a Markdown ticket. `entry` (the entry it marks, for its name, row and rules),
    `split` (a split mark's split) and `status` (the mark's status now: `{status, reasons}`) are
    context only — `parse_ticket` reads the mark's own fields back, never these."""
    for field in ("target", "rule_id", "problem", "reviewer"):
        if "\n" in str(getattr(mark, field) or ""):
            raise ValueError(f"a mark's {field} is one line; this one is not: {getattr(mark, field)!r}")
    name = (split or {}).get("label") or (entry or {}).get("name") or mark.target
    lines = [f"# Review: {mark.verdict} — {name}", ""]
    lines += [f"- **Scope:** {mark.scope}",
              f"- **Target:** `{mark.target}`"]
    if mark.rule_id:
        lines.append(f"- **Rule:** `{mark.rule_id}`")
    lines += [f"- **Verdict:** {mark.verdict}",
              "- **Answers:** " + " · ".join(f"{k} {mark.answers[k]}" for k in "TRPA"
                                             if k in mark.answers),
              *([f"- **Problem:** {mark.problem}"] if mark.problem is not None else []),
              f"- **Reviewer:** {mark.reviewer}",
              f"- **Date:** {mark.date}",
              "- **Printed pages:** " + (", ".join(str(p) for p in mark.pages) or "—"),
              f"- **Book:** sha256 `{mark.pdf_sha256}`"]
    if status and status.get("status") == "stale":
        lines.append("- **Now:** changed since marked (" + ", ".join(status.get("reasons") or [])
                     + ") — read it again before fixing")
    lines += ["", "## What the reviewer said", ""] + _quote(mark.note) + [""]
    if entry is not None:
        lines += ["## The entry", ""]
        lines.append(f"{entry.get('name', '')} (`{entry.get('entry_id', '')}`), printed p. "
                     + ", ".join(str(p) for p in entry.get("source_pages") or []))
        if entry.get("regs_verbatim"):
            lines += ["", "The row:", ""] + ["    " + ln for ln in str(entry["regs_verbatim"]).split("\n")]
        rules = [r for r in entry.get("rules") or []
                 if not mark.rule_id or r.get("rule_id") == mark.rule_id]
        if rules:
            lines += ["", "Rules:", ""]
            lines += [f"- `{r.get('rule_id')}` — {r.get('label') or r.get('verbatim') or ''}"
                      for r in rules]
        lines.append("")
    if split is not None:
        lines += ["## The split point", "",
                  f"`{split.get('id', mark.target)}` ({split.get('kind', '')}) on "
                  f"{split.get('waterbody', '?')}"]
        if split.get("_note"):
            lines += ["", "Its note: " + str(split["_note"])]
        lines.append("")
    lines += ["## Work", ""] + _work(mark)
    return "\n".join(lines) + "\n"


def parse_ticket(text: str) -> dict:
    """The mark's own fields from a ticket: scope, target, rule_id, verdict, answers, problem,
    note, reviewer, date, pages, pdf_sha256. ValueError on a ticket missing one."""
    lines = text.split("\n")
    got: dict[str, str] = {}
    for ln in lines:
        m = _FIELD.match(ln)
        if m and m["k"] not in got:
            got[m["k"]] = m["v"]
    need = ("Scope", "Target", "Verdict", "Answers", "Reviewer", "Date",
            "Printed pages", "Book")
    missing = [k for k in need if k not in got]
    if missing:
        raise ValueError(f"not a review ticket: no {', '.join(missing)}")
    try:
        start = lines.index("## What the reviewer said") + 2
    except ValueError:
        raise ValueError("not a review ticket: no reviewer's note section") from None
    end = start
    while end < len(lines) and lines[end] != "":
        end += 1
    tick = lambda v: v[1:-1] if v.startswith("`") and v.endswith("`") else v  # noqa: E731
    answers = {}
    for part in filter(None, got["Answers"].split(" · ")):
        k, v = part.split(" ", 1)
        answers[k] = v
    return {
        "scope": got["Scope"], "target": tick(got["Target"]),
        "rule_id": tick(got["Rule"]) if "Rule" in got else None,
        "verdict": got["Verdict"], "answers": answers,
        "problem": got.get("Problem"),
        "note": _unquote(lines[start:end]), "reviewer": got["Reviewer"], "date": got["Date"],
        "pages": [] if got["Printed pages"] == "—" else [int(p) for p in got["Printed pages"].split(", ")],
        "pdf_sha256": got["Book"].removeprefix("sha256 ").strip("`"),
    }


def mark_fields(mark) -> dict:
    """What a ticket carries of a mark (and `parse_ticket` returns)."""
    return {"scope": mark.scope, "target": mark.target, "rule_id": mark.rule_id,
            "verdict": mark.verdict, "answers": dict(mark.answers), "problem": mark.problem,
            "note": mark.note, "reviewer": mark.reviewer, "date": mark.date,
            "pages": list(mark.pages), "pdf_sha256": mark.pdf_sha256}


def export(marks: Iterable, *, entries: Mapping[str, Mapping] | None = None,
           splits: Mapping[str, Mapping] | None = None,
           statuses: Mapping[tuple, Mapping] | None = None,
           verdicts: Sequence[str] = TICKET_VERDICTS) -> list[tuple[str, str]]:
    """(file name, ticket text) for every mark IN FORCE (`marks.latest`) whose verdict is one of
    `verdicts`, in the store's order. `statuses` (mark key -> `{status, reasons}`) adds what changed
    since; a mark no longer in force (superseded) is never a ticket."""
    from pipeline.regs.review.marks import latest
    out = []
    seen: dict[str, tuple] = {}
    for key, m in latest(marks).items():
        if m.verdict not in verdicts:
            continue
        name = ticket_name(m)
        if name in seen:                 # two targets slugged alike: one file would overwrite the other
            raise ValueError(f"two marks make the ticket file {name}: {seen[name]} and {key}")
        seen[name] = key
        out.append((name, ticket_text(
            m, entry=(entries or {}).get(m.target) if m.scope != "split" else None,
            split=(splits or {}).get(m.target) if m.scope == "split" else None,
            status=(statuses or {}).get(key))))
    return out


def statuses_now(marks: Iterable, *, entries: Mapping[str, Mapping], ctx,
                 verdicts: Sequence[str] = TICKET_VERDICTS) -> dict[tuple, dict]:
    """Mark key -> `{status, reasons}` NOW (`marks.status` against the target's fingerprints in
    `ctx`, a `fingerprint.Context` of the vintage on disk), for the marks in force that export
    would turn into tickets — what a ticket says changed since. The API (`review.tickets`) and the
    CLI both ask here, so the file and the screen say the same thing."""
    from pipeline.regs.review import fingerprint as F
    from pipeline.regs.review.marks import Prints, latest, status
    out: dict[tuple, dict] = {}
    for key, m in latest(marks).items():
        if m.verdict not in verdicts:
            continue
        if m.scope == "split":
            try:
                now = Prints.model_validate(F.split_fingerprints(m.target, ctx))
            except KeyError:                      # the split is gone from splits.json
                now = None
        else:
            e = entries.get(m.target)
            now = Prints.model_validate(F.entry_fingerprints(e, ctx)) if e is not None else None
        st = status(m, now)
        out[key] = {"status": st["status"], "reasons": st["reasons"]}
    return out


def split_index(splits_json: Mapping) -> dict[str, dict]:
    """split id -> the split, with its waterbody's name (a ticket's context)."""
    return {sp["id"]: {**sp, "waterbody": wb.get("name")}
            for wb in splits_json.get("waterbodies", []) for sp in wb.get("splits", [])
            if sp.get("id")}


def main(argv: list[str] | None = None) -> int:
    from pipeline.common.curated import CURATED, GENERATED
    from pipeline.deliver.bundle.read import BUNDLE
    from pipeline.regs.review.fingerprint import Context
    from pipeline.regs.review.marks import load
    ap = argparse.ArgumentParser(prog="python -m pipeline.regs.review.tickets",
                                 description="wrong and unsure review marks as ticket text")
    ap.add_argument("--marks", type=Path, default=CURATED.regulations.review.marks)
    ap.add_argument("--entries", type=Path, default=CURATED.regulations.entries.catalogue)
    ap.add_argument("--verdict", choices=TICKET_VERDICTS, action="append")
    ap.add_argument("--out", type=Path, help="write one file per ticket here (else stdout)")
    a = ap.parse_args(argv)
    from pipeline.regs.review.precheck import _corpus
    # what changed since each mark, against the vintage on disk — as the review app's export says it
    ctx = Context.on_disk(CURATED.waters.splits, CURATED.regulations.overrides,
                          GENERATED.registry(), BUNDLE)
    # the corpus as the review app reads it (`io.read_entryfile` with the registry), so a mark's
    # prints are taken from the same entry
    entries = {e["entry_id"]: e for e in _corpus(a.entries, ctx.registry)}
    marks = load(a.marks)
    verdicts = tuple(a.verdict or TICKET_VERDICTS)
    got = export(marks, entries=entries, splits=split_index(ctx.splits), verdicts=verdicts,
                 statuses=statuses_now(marks, entries=entries, ctx=ctx, verdicts=verdicts))
    if a.out:
        a.out.mkdir(parents=True, exist_ok=True)
        for name, text in got:
            (a.out / name).write_text(text, encoding="utf-8")
        print(f"{len(got)} ticket(s) -> {a.out}", file=sys.stderr)
    else:
        sys.stdout.write(SEPARATOR.join(text for _, text in got))
        print(f"{len(got)} ticket(s)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
