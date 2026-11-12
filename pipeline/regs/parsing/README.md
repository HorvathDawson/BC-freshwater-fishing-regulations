# pipeline/regs/parsing — synopsis rows → validated catalogue entries

Turns each row of the BC fishing synopsis's water-specific tables into a `CatalogueEntry`
(`catalogue.py`): typed rules bound to the water's cut-points, plus the entry's `licensing` list.
The checked-in region files `data/curated/regulations/synopsis/region-*.json` are the
**single source of truth**; curator edits are written straight back to them.

The format itself is `pipeline/docs/18-how-regulations-are-stored.md`; how to run the parse is also
in `prompts/CHAT_INVOCATION.md`.

## The one entry point

Everything runs through the shell script — the Python modules are building blocks it calls:

```bash
bash pipeline/regs/parsing/run_parse.sh <all|parse|parse-dry|review|repass|ingest|seed-ledger|status>
```

| subcommand | what it does | credits? |
|---|---|---|
| `all`       | `parse`, then `review` | **yes** |
| `parse`     | export the rows not yet in the catalogue → parse → ingest | **yes** |
| `parse-dry` | export the batches only, to read a prompt before spending anything | no |
| `review`    | an independent agent reviews the last parse's batches; findings → the work dir's `reviews/` | **yes** |
| `repass`    | re-parse ONLY the entries the review flagged high/medium, with the findings as hints, on `ESCALATE_MODEL`. `repass --replace-edited` also overwrites entries edited since ingest | **yes** |
| `ingest`    | re-apply the work dir's responses, no dispatch. `ingest --replace-edited` is how to act on a KEPT report without paying for the parse again | no |
| `seed-ledger` | record every checked-in entry as ingest's own write (`ingested.json`) — see below | no |
| `status`    | entry counts, review states and flagged entries, synopsis rows still unparsed, what to run next | no |

⛔ **HUMAN-ONLY**: `all` / `parse` / `review` / `repass` spend the user's credits — Claude/agents must
hand over the command, never run it. `parse-dry` / `ingest` / `seed-ledger` / `status` are local.

Env knobs: `REGISTRY`, `BATCH_SIZE`, `MODEL` (parse), `REVIEW_MODEL`, `ESCALATE_MODEL` (repass),
`CONCURRENCY`, `CLAUDE_BIN`.

## Dataflow

```
synopsis rows ──match──▶ batches/batch_NNN.json         one item per ROW; entry_id = r{region}:{name}@{MUs}
   (rows.py)             batches/batch_NNN.prompt.txt   the self-contained prompt
                                   │
                          dispatch (claude CLI)
                                   ▼
                         responses/batch_NNN.json       [{index, entry}, …]
                                   │
                    ingest_catalogue (the gate)  ──────▶ regulations/synopsis/region-*.json
                                   │                     + ingested.json (the ledger, work dir)
                    dispatch --review (claude CLI)
                                   ▼
                         reviews/batch_NNN.review.json  {verdict, issues:[{index, severity, problem, fix}]}
                                   │                     or {error: review_failed} if unreadable
                    batch_exporter --flagged
                                   ▼
                         repass.json + new batches      the flagged entries only, findings as hints
```

All of it except the region files lives in the work dir, `data/generated/regs/parse/`.

**Reviews are never written onto an entry.** A catalogue entry has no field for a verdict. A review
is a finding about one parse of one batch; `io.read_review_findings` joins each issue's `index` to
the batch item's `entry_id`. `repass` reads the findings BEFORE its export replaces the batches (an
export deletes every review older than the batches it writes), and keeps them in `repass.json`.

**A repass does not overwrite a curator's edit.** Ingest records a digest of every entry it writes
in the work dir's `ingested.json`. It replaces an entry already on disk only if the file's copy is
still the one it wrote; an entry edited since — or one the ledger never saw — is reported as KEPT
and left alone. To act on the report one entry at a time (T553), name it:
`python -m pipeline.regs.parsing.ingest_catalogue --batch … --response … --out data/curated/regulations/synopsis
--replace-entry <entry_id>` (repeatable). `run_parse.sh ingest --replace-edited` replaces every KEPT entry at once
(`repass --replace-edited` does it in the same run). Either way the curator's placement is carried onto the re-parse
(`carry_curation`: the entry's extents / tributary scope / tidal / anadromous_rainbow / scope_note, and the placement
fields of every rule or licensing record whose quote is unchanged), and each record that could not be carried is
listed; `--no-carry` replaces whole. **An entry that carries an owner mark is never replaced by either flag** (T628):
the run prints `REFUSED`, naming each mark in force on the entry or one of its rules, leaves the entry as it is and
exits 1. There is no flag past it; a reviewed entry changes by a reviewed edit to the curated file.
**Nor by a plain repass** (T628 item 5): an entry nobody edited is still replaced by a re-parse, unless it carries an
owner mark and the re-parse says something else than the copy on disk — then `REFUSED re-parse of <entry_id>` names the
mark, the entry stays as reviewed and the run exits 1 (a dry run says the same). A re-parse equal to the reviewed copy
is no change and is not refused. To change a marked entry: a reviewed edit to the curated file, or the owner clears the
mark in the review app first. Ingest reads the marks store (`CURATED.review.marks`) before it writes anything, and only
when it would put other content over an entry on disk.

**The ledger is seeded, not grown from nothing.** Without `ingested.json` every entry is one the
ledger never saw, so a repass would keep all of them. `run_parse.sh seed-ledger`
(`ingest_catalogue --seed-ledger`) records every checked-in entry as it is on disk now — the
checked-in corpus IS the curated truth — so a repass replaces an entry nobody has touched since and
keeps one edited after the seed. It was seeded on 2026-09-23 (1,480 entries).

The ledger's digest is `io.entry_digest` (T628: the one the reach run, the bundle and the review vintage hold), and the
file names it (`{"digest": "io.entry_digest", "entries": {…}}`). The 2026-09-23 ledger predates that: none of its
records can be compared, so ingest prints a `NOTE` and keeps every entry already on disk until the re-seed — which
belongs to the annual run (runbook step 9), not before.

## Modules (building blocks — not the user entry point)

- `catalogue.py` — the model: `CatalogueEntry` / `CatalogueRule` / licensing records, validators,
  and the generated labels.
- `io.py` — the single home for shared IO: region-file read/write (`write_entryfile`: atomic, keeps
  the file's indent and order, and validates what it wrote), batch items, reviews, LLM-JSON
  extraction, work-dir paths. **A strict leaf** (stdlib, plus `catalogue` lazily to write).
- `entry_models.py` — the op + split `Extent` that extents are checked against.
- `rows.py` — load synopsis rows. The species are the book's list in `catalogue.py`
  (`BOOK_FAMILIES`, `species_menu()`); scientific names are `catalogue.SCIENTIFIC_NAMES` (`species.py` is deleted).
- `parse_context.py` — build the parse prompt (identity + bindable-boundary menu + species + regs).
- `review_exporter.py` — build the review prompt (the checklist + each row's menu, regs and parsed
  entry + the one output envelope).
- `batch_exporter.py` — rows → batches (`--skip-existing` / `--only-changed` / `--flagged`).
- `dispatch.py` — run parse/review through the `claude` CLI (the credit step).
- `validate_catalogue.py` — the checks; also a CLI that runs the ingest gate on a candidate, for
  the parse agent to run on itself.
- `ingest_catalogue.py` — the gate: row facts from the batch, validate, write, record the ledger.
- `backfill_matched.py`, `remap_boundaries.py` — local repairs after a registry change.

## Safety notes

- Every region-file write goes through `io.write_entryfile`. The file is validated as written, as
  a whole, through `CatalogueFile`; an untouched neighbour is written back byte for byte.
- Nothing partial is written: an entry validates whole or is reported and left out.
- The region files are the durable progress. The work dir is transient — deleting it never loses
  a parsed entry, but it does lose the review findings and the ledger. Re-seeding is NOT a routine
  step: it declares every entry on disk unedited, so the next repass would replace curated entries
  (their placement is carried, the rest is the re-parse's). Seed only after the work dir was lost or
  a new book was adopted (annual runbook step 9), and act on KEPT entries with `--replace-entry`.
