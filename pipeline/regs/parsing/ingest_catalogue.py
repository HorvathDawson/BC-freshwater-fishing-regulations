"""Ingest agent responses into the checked-in catalogue entry files — the validation gate.

Takes the JSON a parsing agent produced for one or more batches, runs every entry through the SAME
gate the agent was told to run (`validate_catalogue`), and writes only what passes.

    PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.parsing.ingest_catalogue \
        batches/batch_000.json responses/batch_000.json --dry-run

Two rules that are not negotiable, both learned the hard way:

* **`regs_verbatim` is taken from the BATCH, never from the model.** The agent writes both the
  passage and the rules that quote it, so a model-supplied passage makes the chain of custody
  self-referential — an invented sentence validates against its own invention. Two did.
* **Nothing partial is written.** An entry either validates whole or is reported and left out. A
  half-ingested entry is a water with some of its regulations, which is indistinguishable from a
  water with no regulation.

And one guard, because a re-parse replaces an entry wholesale and there is no `locked` any more:

* **A curator's edit is not overwritten.** Every entry ingest writes is recorded in a ledger in the
  work dir (`ingested.json`: entry_id -> digest of the entry as written). Before replacing an
  entry already on disk, ingest compares the file's copy with the ledger. If they differ — a
  curator bound a reach, added a tributary exclude, or anything else since the parse — or the
  ledger has no record of it, the entry is KEPT and reported, and nothing about it is written.
  `--replace-entry <entry_id>` (repeatable) replaces ONE such entry after you have read the
  report; `--replace-edited` replaces every one in the run (the curated files are in git, so
  `git diff` shows what the replace changed). EITHER WAY THE CURATOR'S PLACEMENT IS CARRIED
  (T553 item 4): the edited copy's entry-level placement and curation (`CARRIED_ENTRY_FIELDS`) and,
  for every rule / licensing record whose id AND quote are unchanged, its placement fields
  (`fingerprint.PLACEMENT_FIELDS`: extents, tributary scope and excludes, undrawn part, side …)
  are kept on the re-parsed entry, and every one that could not be carried is listed.
  `--no-carry` replaces whole.
  AN ENTRY THAT CARRIES AN OWNER MARK IS NEVER REPLACED THIS WAY (T628): either flag refuses it, naming the mark
  (`owner_marks`: the marks in force on the entry or one of its rules, any verdict). There is no flag past it — a
  reviewed entry changes by a reviewed edit to the curated file.

  THE LEDGER IS SEEDED, NOT GROWN FROM NOTHING. With no `ingested.json` every entry is one "the
  ledger never saw", so the first repass would keep all of them. `--seed-ledger` records every
  entry as it is on disk NOW — declaring the checked-in corpus the curated truth — and from then
  on only an edit made after the seed is kept. `run_parse.sh seed-ledger` runs it (no credits).

  THE LEDGER'S DIGEST IS `io.entry_digest` (T628), the one the reach run, the bundle and the review vintage hold for
  an entry, and the file says so (`{"digest": "io.entry_digest", "entries": {...}}`). A ledger written before that (a
  flat `entry_id -> sha256`) cannot be compared: its entries read as never seen — every one KEPT — and each run says
  so until the re-seed, which belongs to the annual run (`handoff/PLAN/runbooks/annual-synopsis.md` step 9), not
  before: seeding declares every edit made since the last seed to be ingest's own write.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

from pipeline.regs.parsing import io
from pipeline.regs.parsing.catalogue import CatalogueEntry, clean_verbatim
from pipeline.regs.parsing.io import dump_entry
from pipeline.regs.parsing.validate_catalogue import default_extents, gate, squash


def load_batch(paths: list[str]) -> dict[str, dict]:
    """{key: batch item} keyed by BOTH `entry_id` and `#index`.

    The agent is told to copy each item's `index` back verbatim, and that is the reliable join:
    `entry_id` is a value the model retypes, so keying on it alone makes a typo look like an
    invented entry. A batch file is ours — `{batch, rows_digest, items: [...]}` — and nothing
    else is read (`rows_digest` by `check_rows_digest`)."""
    items: dict[str, dict] = {}
    for p in paths:
        data = json.loads(Path(p).read_text(encoding="utf-8"))
        for it in data["items"]:
            items.setdefault(str(it["entry_id"]), it)
            items[f"#{it['index']}"] = it
    return items


def check_rows_digest(batch_paths: list[str], rows_path: Path | None = None) -> str | None:
    """None when every batch was exported from the rows ingest would read now; else the problem.

    Each batch carries `rows_digest` (`batch_exporter.compute_rows_digest` over every extracted row). A batch exported
    before the extraction moved hands the model rows that are no longer the book's, and ingest takes `regs_verbatim`
    from the batch — so the stored text would silently be last extraction's (T553 item 2: the field was written and
    never read). `rows_path` is the extraction the export read (`--rows`; default the live
    `synopsis_raw_data.json`)."""
    from pipeline.regs.parsing.batch_exporter import compute_rows_digest
    from pipeline.regs.parsing.rows import load_synopsis_rows
    got: dict[str, list[str]] = defaultdict(list)
    for p in batch_paths:
        d = json.loads(Path(p).read_text(encoding="utf-8")).get("rows_digest")
        got[str(d)].append(Path(p).name)
    if len(got) > 1:
        return ("the batches come from different exports (rows_digest " +
                "; ".join(f"{d[:12]}: {', '.join(sorted(n)[:3])}" for d, n in sorted(got.items())) + ")")
    (stored, names), = got.items()
    now = compute_rows_digest(load_synopsis_rows(rows_path))
    if stored != now:
        return (f"the batches ({len(names)}) were exported from other rows than {rows_path or 'the live extraction'} "
                f"(rows_digest {stored[:12]} vs now {now[:12]}): re-export, or pass the --rows the export read")
    return None


def response_coverage(batch: dict[str, dict], candidates: list[dict]) -> tuple[list[str], list[str]]:
    """([batch item with NO response row], [index answered more than once]), each as `#index entry_id`.

    A rejected candidate prints a FAIL line; a row the model never returned printed nothing and counted nowhere, so in
    the annual update a changed row it skipped surfaced only as `removed` — the list the owner confirms as gone from
    the book. And a second row with one index silently replaced the first (T553 item 2)."""
    items = {k[1:]: v for k, v in batch.items() if k.startswith("#")}
    seen: dict[str, int] = defaultdict(int)
    for c in candidates:
        if c.get("_batch_index") is not None:
            seen[str(c["_batch_index"])] += 1
    missing = [f"#{i} {items[i].get('entry_id')}" for i in sorted(items, key=int) if not seen.get(i)]
    dupes = [f"#{i} {items[i].get('entry_id') if i in items else '<not in the batch>'}"
             for i in sorted(seen, key=int) if seen[i] > 1]
    return missing, dupes


def response_rows(path: str | Path) -> list[dict]:
    """A response file -> its entries, each carrying `_batch_index` (the join key).

    ONE SHAPE: the array `dispatch` writes, `[{"index": N, "entry": {...}}, ...]`. Anything else
    is refused with the file named — a response in another shape was produced by something
    other than this pipeline, and guessing at it is how a prose-era file was once ingested as
    catalogue output."""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"{Path(path).name}: a response is a JSON array of {{index, entry}}")
    out: list[dict] = []
    for row in data:
        if not (isinstance(row, dict) and isinstance(row.get("entry"), dict)
                and isinstance(row.get("index"), int)):
            raise ValueError(f"{Path(path).name}: every row must be {{\"index\": int, "
                             f"\"entry\": {{...}}}} — got {str(row)[:80]!r}")
        out.append(dict(row["entry"], _batch_index=row["index"]))
    return out


def _passthrough(item: dict) -> dict:
    """The entry fields that are facts about the synopsis ROW, exactly as the batch carries them."""
    return {
        "entry_id": str(item["entry_id"]),
        "regs_verbatim": str(item["raw_regs"]),
        "name": item.get("name") or "",
        "display_name": item.get("display_name") or "",
        "region": item.get("region") or "",
        "symbols": list(item.get("symbols") or []),
        "source_pages": list(item.get("pages") or []),
        "matched": ([item["item_id"], *(item.get("also_item_ids") or [])]
                    if item.get("item_id") else []),
    }


def ingest_registry():
    """The registry ingest checks against: the default build's (`GENERATED.registry()`), so an
    absorbed item id is refused at ingest exactly as the review app refuses it (T119). It raises
    naming the build command when there is no build — an ingest with no registry would skip a
    check, which is the gap this closes."""
    from pipeline.atlas.registry import load_registry
    from pipeline.common.curated import GENERATED
    return load_registry(str(GENERATED.registry()))


def ingest(candidates: list[dict], batch: dict[str, dict], *,
           registry) -> tuple[dict[str, CatalogueEntry], list[str]]:
    """Run every candidate through THE gate (`validate_catalogue.gate`) with its batch item as the
    cut-point menu and `registry` for the absorbed-id check (None only where a caller has no build
    — tests). Returns ({entry_id: accepted entry}, [problem lines])."""
    accepted: dict[str, CatalogueEntry] = {}
    problems: list[str] = []
    for data in candidates:
        eid = str(data.get("entry_id") or "<no entry_id>")
        idx = data.pop("_batch_index", None)
        item = batch.get(f"#{idx}") if idx is not None else batch.get(eid)
        if item is not None and item.get("entry_id"):
            # IDENTITY COMES FROM THE BATCH, exactly like regs_verbatim. The agent retypes
            # entry_id and 375 of 1099 came back without their `@MU` suffix — close enough to
            # look right, different enough that `--skip-existing` no longer recognises the row.
            # 368 entries were stored under the truncated id, so a resume re-parsed waters that
            # were already done and would have written each one twice under two ids.
            eid = str(item["entry_id"])
            data["entry_id"] = eid
        if item is None:
            problems.append(f"{eid}: not in the batch — an entry_id was invented or altered")
            continue
        # EVERY FACT ABOUT THE ROW COMES FROM THE BATCH — the model gets no vote on any of them,
        # and nothing it wrote in these fields survives, even where the batch is empty. Each was
        # left to the model once and came back wrong: 843 of 1,021 names retyped (66 losing the
        # parenthetical that carries the reach), Classified kept on 20 of 68 rows, Stocked on 11
        # of 304. `symbols` are the printed glyphs and nothing else: a water the TEXT calls
        # classified is a fact for the rules, not a symbol.
        data = json.loads(json.dumps(data))     # deep copy: the split check rewrites in place
        data.update(_passthrough(item))
        # A QUOTE IS THE PRINTED SENTENCE: the extraction's `**` / `[Includes Tributaries]` stay in
        # `regs_verbatim` (the batch's own text) and come out of every record the model quoted.
        for group in ("rules", "licensing", "see"):
            for rec in data.get(group) or []:
                if isinstance(rec, dict) and isinstance(rec.get("verbatim"), str):
                    rec["verbatim"] = clean_verbatim(rec["verbatim"])
        source = data["regs_verbatim"]
        # A RULE THAT SAYS NOTHING ABOUT PLACE takes the whole water — ingest's normalisation of
        # the parser's output, written into the file (AGENTS 13). Not part of the gate: a curator's
        # draft is never widened.
        default_extents(data)
        # `item` carries the boundary menu, so this also checks every split id and rewrites an
        # alias to its canonical spelling — the same gate the agent runs on itself and the review
        # app runs on a save.
        verdict = gate(data, menu=item, registry=registry, source_text=source)
        entry, errors = verdict.entry, verdict.messages()
        fatal = [e for e in errors if not e.startswith("ADVISORY")]
        for e in errors:
            problems.append(f"{'ADVISORY ' if e.startswith('ADVISORY') else ''}{eid}: {e}")
        if entry is not None and not fatal:
            accepted[eid] = entry
    return accepted, problems


def default_ledger() -> Path:
    """Where ingest records what it wrote — beside the batches and reviews it was made from."""
    return io.default_work_dir() / "ingested.json"


#: What a ledger written by this code says its digests are. A ledger that says nothing was written before T628.
LEDGER_DIGEST = "io.entry_digest"


def digest(entry: dict) -> str:
    """The ledger's digest of one entry as stored: `io.entry_digest` — the same value the reach run, the bundle and
    the review vintage hold for it (T628; it was a second, full-length SHA-256 over another serialisation)."""
    return io.entry_digest(entry)


def ledger_is_legacy(path: Path) -> bool:
    """Whether `path` is a ledger from before `LEDGER_DIGEST` (a flat `entry_id -> sha256`). A missing file is not."""
    if not Path(path).exists():
        return False
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return not (isinstance(data, dict) and "digest" in data)


def _read_ledger(path: Path) -> dict[str, str]:
    """entry_id -> digest of the entry as ingest last wrote it. No file: no record. A legacy ledger: no record
    either (its digests are another function's; `run` says so) — never a comparison of two different digests. A
    ledger naming a digest this code does not write is refused."""
    if not path.exists() or ledger_is_legacy(path):
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("digest") != LEDGER_DIGEST or not isinstance(data.get("entries"), dict):
        raise ValueError(f"{path}: not an ingest ledger this code reads (digest {data.get('digest')!r}, "
                         f"wants {LEDGER_DIGEST!r} and an `entries` map)")
    return dict(data["entries"])


def _write_ledger(path: Path, record: dict[str, str]) -> None:
    io.atomic_write(path, json.dumps({"digest": LEDGER_DIGEST, "entries": record}, indent=1, sort_keys=True) + "\n")


def seed_ledger(out_dir: Path, ledger: Path) -> int:
    """Record every entry in `out_dir` as ingest's own write, exactly as it is on disk now.

    Digested from the file's dicts, as `write` compares them, so an entry left untouched after the
    seed is replaceable by a repass and one edited after it is KEPT. Replaces any ledger there is:
    seeding declares the checked-in corpus the truth. Returns the number of entries recorded."""
    record = {eid: digest(e) for eid, e in io.read_entries_dir(Path(out_dir)).items()}
    _write_ledger(ledger, record)
    return len(record)


def owner_marks(entry_ids, marks_path: Path | None = None) -> dict[str, list[str]]:
    """entry_id -> the owner marks IN FORCE on it (on the entry, or on one of its rules), each in words: what it is
    on, its verdict, who made it and when. Read from the marks store (`CURATED.review.marks`; a missing store raises,
    `marks.load`). A cleared mark is not in force (`marks.latest`); any other verdict is — a "wrong" mark is the
    owner's finding on this copy as much as a "verified" one is the owner's sign-off."""
    from pipeline.regs.review import marks as M
    if marks_path is None:
        from pipeline.common.curated import CURATED
        marks_path = CURATED.review.marks
    want = set(entry_ids)
    out: dict[str, list[str]] = defaultdict(list)
    for m in M.latest(M.load(Path(marks_path))).values():
        if m.scope in ("entry", "rule") and m.target in want:
            on = f"rule {m.rule_id}" if m.scope == "rule" else "the entry"
            out[m.target].append(f"{m.verdict} on {on}, {m.reviewer} {m.date}")
    return {k: sorted(v) for k, v in out.items()}


#: The entry's own fields a curator sets that the parser never does (or that a curator corrects):
#: where the entry lands, and what the book states about its water but lists nowhere.
CARRIED_ENTRY_FIELDS = ("extents", "includes_tributaries", "tidal", "anadromous_rainbow", "scope_note")


def carry_curation(on_disk: dict, new: CatalogueEntry) -> tuple[CatalogueEntry, list[str]]:
    """`new` (a re-parse) with the curated placement of `on_disk` (the edited copy) carried onto it.

    Entry level: `CARRIED_ENTRY_FIELDS`, the disk copy's value (or its absence). Per record: a rule
    (by `rule_id`) or licensing record (by `id`) whose quote is unchanged (squashed) takes the disk
    copy's `PLACEMENT_FIELDS` — it is the same statement, so where the curator put it still holds.
    A record whose quote changed, or that the re-parse dropped, carries nothing: it is named in the
    returned notes, for the curator to place again. The result is re-validated whole."""
    from pipeline.regs.review.fingerprint import PLACEMENT_FIELDS
    nd = dump_entry(new)
    notes: list[str] = []
    for f in CARRIED_ENTRY_FIELDS:
        if nd.get(f) != on_disk.get(f):
            notes.append(f"carried entry {f}")
        if f in on_disk:
            nd[f] = on_disk[f]
        else:
            nd.pop(f, None)
    for group, key in (("rules", "rule_id"), ("licensing", "id")):
        old = {r.get(key): r for r in on_disk.get(group) or []}
        seen = set()
        for rec in nd.get(group) or []:
            was = old.get(rec.get(key))
            if was is None or squash(was.get("verbatim") or "") != squash(rec.get("verbatim") or ""):
                continue
            seen.add(rec.get(key))
            moved = sorted(f for f in PLACEMENT_FIELDS if rec.get(f) != was.get(f))
            for f in PLACEMENT_FIELDS:
                if f in was:
                    rec[f] = was[f]
                else:
                    rec.pop(f, None)
            if moved:
                notes.append(f"carried {rec.get(key)}: {', '.join(moved)}")
        for k, was in old.items():
            if k not in seen and any(f in was for f in PLACEMENT_FIELDS):
                notes.append(f"NOT carried {k}: its quote changed or the re-parse dropped it — place it again")
    return CatalogueEntry.model_validate(nd), notes


def write(accepted: dict[str, CatalogueEntry], out_dir: Path, dry_run: bool = False, *,
          ledger: Path, replace_edited: bool = False, replace_ids: frozenset = frozenset(),
          carry: bool = True, report: list | None = None,
          marked: dict[str, list[str]] | None = None,
          refused: list | None = None) -> tuple[dict[str, int], list[str]]:
    """Merge into region files by the entry's own `region`, replacing an entry of the same id —
    unless the copy on disk is not the one ingest last wrote (see the module docstring).

    An edited entry is replaced only when `replace_edited` or its id is in `replace_ids`, and then
    with its curation carried (`carry_curation`) unless `carry` is False. `report` collects one
    line per such replace (what was carried, what could not be).

    `marked` is `owner_marks` of the accepted entries: an edited entry in it is never replaced — it is left as it
    is on disk, listed as kept, and one line naming its marks goes to `refused` (T628). THE CALLER READS THE STORE:
    `run`, the one caller that replaces on the checked-in corpus, always passes it; `None` is a caller stating its
    entries have no marks store (a test's own tree), never a default for the live one.

    Returns ({file name: entries written}, [entry ids KEPT because they were edited])."""
    record = _read_ledger(ledger)
    by_region: dict[str, list[CatalogueEntry]] = defaultdict(list)
    for e in accepted.values():
        by_region[e.region or "unknown"].append(e)

    written: dict[str, int] = {}
    kept: list[str] = []
    for region, entries in sorted(by_region.items()):
        path = out_dir / f"region-{region}.json"
        merged: dict[str, object] = dict(io.read_entryfile(path, missing_ok=True))   # file order; may create it
        n = 0
        for i, e in enumerate(entries):
            on_disk = merged.get(e.entry_id)
            if on_disk is not None and record.get(e.entry_id) != digest(on_disk):
                if not (replace_edited or e.entry_id in replace_ids):
                    kept.append(e.entry_id)
                    continue
                if marked and marked.get(e.entry_id):
                    if refused is not None:
                        refused.append(f"{e.entry_id}: carries an owner mark ("
                                       + "; ".join(marked[e.entry_id]) + ")")
                    kept.append(e.entry_id)
                    continue
                if carry:
                    e, notes = carry_curation(on_disk, e)
                    entries[i] = e
                    if report is not None:
                        report.append(f"REPLACED {e.entry_id} (edited)"
                                      + "".join(f"\n    {x}" for x in notes))
                elif report is not None:
                    report.append(f"REPLACED {e.entry_id} (edited) WHOLE — nothing carried (--no-carry)")
            merged[e.entry_id] = e
            n += 1
        # write_entryfile validates the WHOLE file as written — a duplicate entry_id or a
        # broken neighbour is a failure of the file, and writing it would ship the break.
        if not dry_run and n:
            io.write_entryfile(path, region, merged.values())
            for e in entries:
                if merged[e.entry_id] is e:
                    record[e.entry_id] = digest(dump_entry(e))
        written[path.name] = n
    if not dry_run and any(written.values()):
        _write_ledger(ledger, record)
    return written, kept


def run(batch_paths: list[str], response_paths: list[str], out_dir: str,
        dry_run: bool = False, ledger: Path | None = None, replace_edited: bool = False,
        rows_path: Path | None = None, replace_ids: frozenset = frozenset(), carry: bool = True,
        marks_path: Path | None = None) -> int:
    drift = check_rows_digest(batch_paths, rows_path)
    if drift:
        print(f"FAIL {drift}\nnothing ingested")
        return 1
    batch = load_batch(batch_paths)
    candidates: list[dict] = []
    for p in response_paths:
        candidates += response_rows(p)
    missing, dupes = response_coverage(batch, candidates)
    if dupes:       # which of two answers is the model's? neither is written
        twice = {d.split()[0][1:] for d in dupes}
        candidates = [c for c in candidates if str(c.get("_batch_index")) not in twice]
    accepted, problems = ingest(candidates, batch, registry=ingest_registry())
    for p in problems:
        print(("WARN " if p.startswith("ADVISORY") else "FAIL ") + p)
    for d in dupes:
        print(f"FAIL {d}: answered more than once in the responses — neither answer is written")
    for m in missing:
        print(f"FAIL {m}: no response row — the model never returned it (not rejected: never seen)")

    unknown = sorted(set(replace_ids) - set(accepted))
    for u in unknown:
        print(f"FAIL --replace-entry {u}: not an accepted entry of this run — nothing to replace it with")
    report: list[str] = []
    refused: list[str] = []
    ledger = ledger or default_ledger()
    if ledger_is_legacy(ledger):
        print(f"NOTE {ledger} was written before the ledger's digest became {LEDGER_DIGEST} (T628): none of its "
              f"records can be compared, so every entry already on disk is KEPT. The re-seed belongs to the annual "
              f"run (handoff/PLAN/runbooks/annual-synopsis.md step 9), not before.")
    replacing = replace_edited or bool(replace_ids)
    marked = owner_marks(accepted, marks_path) if replacing else None      # the store is read only for a replace
    written, kept = write(accepted, Path(out_dir), dry_run=dry_run,
                          ledger=ledger, replace_edited=replace_edited,
                          replace_ids=frozenset(replace_ids), carry=carry, report=report,
                          marked=marked, refused=refused)
    for line in report:
        print(line)
    for line in refused:
        print(f"REFUSED {'--replace-entry' if line.split(': ')[0] in replace_ids else '--replace-edited'} {line} — "
              f"not replaced; a reviewed entry changes by a reviewed edit, never by a re-parse")
    rejected = len(candidates) - len(accepted)
    print(f"\n{len(accepted)}/{len(candidates)} entries accepted"
          + (f", {rejected} rejected" if rejected else "")
          + (f", {len(missing)} batch row(s) with no response" if missing else "")
          + (f", {len(dupes)} index(es) answered twice" if dupes else ""))
    for name, n in sorted(written.items()):
        print(f"  {'would write' if dry_run else 'wrote'} {n} entries -> {name}")
    if kept:
        print(f"\nKEPT {len(kept)} entr(y/ies) — the curated copy is not the one ingest last "
              f"wrote (edited since, or parsed before the ledger), so the re-parse was NOT "
              f"applied:")
        for eid in kept:
            print(f"  {eid}")
        print("  Compare each with its response; re-run with --replace-entry <id> (one entry; its placement is "
              "carried) or --replace-edited (all of them). An entry that carries an owner mark is refused by both.")
    return 1 if (rejected or kept or missing or dupes or unknown) else 0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    # `action="append"` alone takes ONE value per flag, and run_parse.sh passes a shell GLOB — so
    # `--batch batches/batch_*.json` handed argparse 46 paths, it consumed one, and the run died on
    # "unrecognized arguments" AFTER the whole parse had been paid for. `extend` + `nargs="+"`
    # accepts both a glob and a repeated flag.
    ap.add_argument("--batch", action="extend", nargs="+",
                    help="batch file(s) from the exporter; a glob is fine")
    ap.add_argument("--response", action="extend", nargs="+",
                    help="candidate JSON from the agent; a glob is fine")
    ap.add_argument("--out", default="data/curated/regulations/synopsis")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--rows", type=Path, default=None,
                    help="the extraction the batches were exported from (the exporter's --rows; default the live "
                    "synopsis_raw_data.json) — its digest must equal every batch's rows_digest")
    ap.add_argument("--ledger", type=Path, default=None,
                    help="what ingest last wrote, per entry (default: <work dir>/ingested.json)")
    ap.add_argument("--replace-edited", action="store_true",
                    help="replace EVERY entry whose curated copy differs from what ingest last wrote "
                    "(placement carried unless --no-carry); read the KEPT report first. An entry that "
                    "carries an owner mark is refused, naming the mark")
    ap.add_argument("--replace-entry", action="append", default=[], metavar="ENTRY_ID",
                    help="replace this one edited entry (repeatable); its curated placement is carried. "
                    "Refused, naming the mark, when the entry carries an owner mark")
    ap.add_argument("--no-carry", action="store_true",
                    help="with --replace-entry/--replace-edited: replace whole, carrying no curation")
    ap.add_argument("--seed-ledger", action="store_true",
                    help="record every entry now in --out as ingest's own write (see the module "
                    "docstring), and ingest nothing")
    a = ap.parse_args()
    if a.seed_ledger:
        if a.batch or a.response:
            ap.error("--seed-ledger ingests nothing; it takes no --batch/--response")
        ledger = a.ledger or default_ledger()
        print(f"seeded {ledger}: {seed_ledger(Path(a.out), ledger)} entries recorded as ingested")
        sys.exit(0)
    if not (a.batch and a.response):
        ap.error("--batch and --response are required (or --seed-ledger)")
    sys.exit(run(a.batch, a.response, a.out, dry_run=a.dry_run, ledger=a.ledger,
                 replace_edited=a.replace_edited, rows_path=a.rows,
                 replace_ids=frozenset(a.replace_entry), carry=not a.no_carry))


if __name__ == "__main__":
    main()
