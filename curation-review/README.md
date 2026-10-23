# Curation Review — parsed-entry review tool

A small, **local** app for a human to review the LLM-parsed catalogue entries against their source:
the original regulation text, the parsed rules, the split bindings, the water on a map, and the
answer an angler gets — to correct what is wrong, and to mark each entry verified. Edits are written straight back to the catalogue region files.

> Status: **built and in use.** It reads and writes the current catalogue model (`CatalogueEntry`
> with `rules` and `licensing`) — every field of it; see "What the editor covers".

## Run it

```bash
bash curation-review/run.sh          # REVIEW MODE: backend :8787 + frontend :5173, Ctrl-C stops both
bash curation-review/run.sh --fix    # FIX MODE (the owner): the editors as well
```

Then open **http://127.0.0.1:5173**. Local only — no LLM calls, no credits. Both servers bind
127.0.0.1 only (Vite `host: "127.0.0.1"`, uvicorn `--host 127.0.0.1`), and the backend's CORS admits only the
Vite origin: the tool is not reachable from another machine. `PORT` / `CURATION_VITE_PORT` move
the two ports (a second copy beside the owner's).

**Two modes (T134).** REVIEW MODE, the default, serves reads and ONE write: a mark
(`POST /api/marks`). The editors' routes are not registered at all, so nothing a reviewer clicks
can change the catalogue or splits.json. FIX MODE (`--fix`, which sets `CURATION_FIX=1`) is the
owner's: entry save and the live draft check, split add / move / delete / rename, and the
pending-build panel's list of what waits. A fix in fix mode puts the entry (or split) in
**awaiting build** with the exact commands to run (the tool never runs them); when the rebuild
lands the screen picks it up with no restart, a new or moved split is queued in tier S0 to confirm, the
marks whose binding moved read "binding moved", and every other mark stands.

### The review screen (T134)

Evidence first, then the checklist, with the verdict bar always visible:

- **Evidence.** The heading is the RAW name as printed ("ADAM RIVER (except Eve River)"; the
  app's display name beside it). A water row is cropped from the PDF at 2.6 px/pt, its water column
  above its regulation column, so it reads at ~17 px text (the extractor's crop was the whole 530 pt
  row at 24 %); "show page" marks the row on its printed page. The boxes come from the
  `row_boxes.json` sidecar (`python -m pipeline.regs.extraction.row_boxes`, a one-off, no credits,
  ~2 min); without it the pane says so with that command. A zone entry's passage is FOUND BY TEXT
  on its chapter page and the share of its words found is shown. The **coverage strip** is the
  row's text with every stretch no rule or record quotes in red.
- **Checklist** (PLAN §2): T text, R rules, P place, A answer, each y / n / ?. Any n is "wrong" and
  asks for a problem category and a note; any ? is "unsure"; all y is "verified"; "defer" needs a
  note. The server decides the verdict (`marks.verdict_from_answers`). While the entry awaits its
  build, P and A wait. A zone entry is marked rule by rule, each rule with its impact ("binds N
  sections in region R").
- **The answer** is the app's own page, opened by its start link
  (`<CURATION_APP_URL>?regs=<item>&part=<i>&date=<day>`, default `http://127.0.0.1:39218/`) in a
  new tab — the part where the entry's rules hold, its own water first.
- **Split points (tier S0)**: "work → split points" lists the cut-points still to verify (an open
  auto-proposal, a "Verify:" note — which a fix-mode add or move stamps — or a split mark gone
  stale), L grade first, each as a card with its note, grade and the map; one check, P.
- **Keys**: `t r p a` pick a check, `y n ?` answer it, Enter records the mark and moves on,
  `j` / `k` next / previous, `u` the next unsure entry, `o` opens the app page, `m` the map large.
  The URL hash is the open entry (`#r1:adam_river_except_eve_river@1-10`, `#split:<id>`): a reload
  resumes in place, and a marked row stays in the list.
- **Progress and ETA** per tier (S0 P1 Z P2 P3 P4 sweep, `pipeline.regs.review.tiers`) and
  overall; the ETA is the reviewer's own median time over their last 20 entries in this browser.

### The one queue, pre-checks and the output (T135)

- **One queue across all regions** ("all regions", the default) in the **review order**: the split
  points still to verify (S0, L grade first) lead it, then the entries tier by tier (P1 Z P2 P3 P4
  sweep), each tier led by its most urgent **risk factor** — no match, unresolved, undrawn part,
  ambiguous cut, cut, several waters, flag — then the most severity-A pre-checks, then the book.
  One function decides it (`tiers.queue_rank` / `split_rank`); `j` / `k` walk from the last split
  card into P1. "book (page)" is still offered. The `unresolved` and `ambiguous cut` factors are
  read from the served reach run (`pipeline.regs.review.runfacts`); with no complete run the queue
  says so and does not claim them.
- **Pre-checks** (`pipeline.regs.review.precheck`; CLI `python -m pipeline.regs.review.precheck`):
  severity A are facts (fails the gate, binds nothing, unresolved, unplaced pieces, ambiguous cut,
  binds through an S0 split, parser-uncertain, unresolved locator, undrawn part), B are hints (a cut
  picked by hand, a quote not in the row, more than 15 characters no rule quotes). Each entry's are
  listed under its evidence, with every ambiguous cut's candidates and the one taken.
- **Statuses** say what the parse left, not review progress: `parser-uncertain` (was "flagged") and
  `plain` (was "unreviewed").
- **Fix mode's `cut_pick`**: every split of a cut extent has a pick — the ruling (E16, unset),
  lower, upper, one of the run's candidate measures, or a measure typed.
- **Output** ("output" beside the progress bar): the wrong and unsure marks as ticket text
  (`/api/review/tickets.md`; files: `python -m pipeline.regs.review.tickets --out <dir>`), and the
  command that commits the marks file and nothing else: `python -m pipeline.regs.review.commit_marks`.

The build it serves is `GENERATED.build()` (`data/generated/atlas/full`); `reuse.py` reads it, so
repointing the app at another build is a config edit, not a code change.

**No endpoint starts a process.** The app never builds or promotes an atlas (T96: an 18-minute,
9 GB build outside `scripts/heavy.sh` sat beside the review buttons). Split edits are applied by a
side-atlas build the owner runs from a terminal (`python -m pipeline build --atlas`, then `--promote`, through `scripts/heavy.sh`);
the app's caches drop themselves when the served files change.

---

## Why

The parser (`pipeline/regs/parsing/`) turns each synopsis row into a `CatalogueEntry` whose `rules`
are each bound to a reach by `extents` (op + curated split ids). The parse is validated structurally,
but a rule can be **confident-but-wrong** (bound to the wrong reach, wrong species, a restriction
missed). A human needs to check each entry against the source text — and, where the parser recorded
uncertainty (a rule's `review_reason`, `unresolved_locators`, or an entry with no `matched` item),
make the call the model couldn't.

## Going through every entry (the review pass)

The queue's **order** is the review order by default (T135, above); `book (page)` goes by region,
then printed page, the region chapter's entries before that page's table rows. Each entry has a bar at the top:

- The bar shows the marks in force on the entry and ← / → to step through the list; the
  checklist below it makes them (`POST /api/marks`, T134 — see "The review screen").
- **Marks (T97)** live in the append-only store `data/curated/regulations/review/marks.jsonl`
  (`CURATED.regulations.review.marks`; `CURATION_MARKS` overrides it), never inside an entry. Each
  mark names its reviewer, date, the book's sha256 and the printed pages, and holds the target's
  FINGERPRINTS (`pipeline.regs.review.fingerprint`: text, meaning, binding + its bundle half,
  answer). When something it was held against changes, the mark reads **changed since marked** with
  typed reasons — book text changed / rule changed / binding moved / answer changed / awaiting build
  — so a no-op migration stales nothing and a moved split stales exactly the entries on its water.
  A mark is refused on an entry that fails the gate. A split rename carries marks over
  (`review.carry_marks`); a split added mid-review reads `unmarked` in `GET /api/marks/splits`.
- The **review** filter lists to do / never marked / changed since marked / wrong / unsure /
  deferred / verified, with counts; the bar beside it is progress (region, and the whole corpus).
- An entry opens by URL: `http://127.0.0.1:5173/#r5:dean_river@5-9`.

On each entry the reviewer sees: the book (the row crop, and **show page** for the whole printed
page — open by default for a region-chapter entry, which has no row crop; page numbers link to the
repo's PDF at the right PDF page, since `source_pages` are PRINTED numbers and the PDF index differs
by 2-6); every rule's generated label, quote, `review_reason`, not-yet-mapped part and unresolved
place; **where each rule binds** — the reach builder's answer for the entry as saved, with what it
decided (tributary walk, `rest` of the water, tidal water removed, confluence cuts, straddling
pieces, ambiguous cuts, region clips). The "What an angler is told" panel was removed (T96): it
was a debug table of reader rows, not the app page, and it opened on the wrong water; Phase 3 of
`handoff/PLAN/curation-review/PLAN.md` puts the app's own page there. Its endpoints (below) remain.

## What the curator does per entry

1. Read the **original reg text** (`regs_verbatim`) next to the **parsed rules** (each with the label
   the reader will see, generated by `catalogue.label`).
2. Check each rule's **binding**: `op` + which **splits** it selected, species, season, tributary
   scope.
3. See the **water on a map** with its split points and what each rule resolves to.
4. **Edit** what is wrong (an extent, `matched`, species), and **Save** — the backend validates and
   writes. For an entry with no `matched` item, **attach** one from a registry search.

There is no confirm or lock: a catalogue entry has no such field. A re-parse does not overwrite an
entry edited here — ingest keeps any entry whose file copy is not the one it last wrote, and reports
it (`pipeline/regs/parsing/ingest_catalogue.py`). Git is the record of who changed what.

---

## Data it reads / writes

| Source | Path | Role |
|---|---|---|
| Entries (read **and write**) | `data/curated/regulations/entries/catalogue/region-*.json` | the review surface |
| Registry (read) | `data/generated/atlas/full/registry.json` | item identity, **`boundaries`** (the bindable splits), `variants`, `mus`, `section_ids` |
| Geometry (read) | `data/generated/atlas/full/graph.pkl`, `geometries.pkl`, `waterbody_polys.pkl` | streams, split points, lakes, areas — served as GeoJSON for the map |
| Splits (read **and write**) | `data/curated/waters/splits.json` | the split editor; a rebuild run from a terminal applies it |

**Write model:** `reuse.save_entry` validates the edited entry through
`pipeline.regs.parsing.catalogue.CatalogueEntry` plus a split-id check against the item's boundaries,
then writes through `pipeline.regs.parsing.io.write_entryfile` — atomic, every other entry in the file
written back exactly as it was, and the whole file validated as written. A field the model does not
know is refused (422), not dropped. The one exception is each rule's `label`, which the backend
stamps on what it serves and removes again on save.

**Every write goes through `backend/writes.py`** (entry saves, split edit/delete/rename, the
marks store):

- **Backed up first.** Each target is copied, byte for byte, into a snapshot
  `data/generated/regs/entries_backup/curation-review/<UTC stamp>-<action>/<dir>/<file>` (the same
  root `reparse_candidates --backup-dir` uses; `CURATION_BACKUP_DIR` overrides it). The newest 500
  snapshots are kept (`CURATION_BACKUP_KEEP`); older ones are pruned. To undo a save, copy the
  file back from its snapshot.
- **All or nothing.** Every new file is computed and validated before anything is written; if a
  later write fails, the files already written are restored. A split **rename** rewrites
  splits.json, every catalogue extent and every DFO salmon location binding the id in one commit,
  and is refused for an id named in pipeline code (`CNR_BRIDGE` and three others).
- **Splits are validated by the pipeline's own loader** (`_flatten_waterbodies` +
  `SplitDef.from_dict`); a split the build would skip is refused (422) with the model's reason.

**Caches follow the build.** Before every `/api/` request the backend stats the served build's
files and the bundle (`reuse.ensure_fresh`); if any changed (a promote, or a rebuild from a
terminal) every build-derived cache is dropped. The place-namer is keyed on splits.json, so an
edited split label shows on the next request.

## API

`API.md` has the request/response shapes. The write endpoint is `PUT /api/entries/{entry_id}`.

## What the editor covers

Every part of the catalogue model, labelled with the model's own keys, so a validator error lands on
the control that edits it (`rules.2.gear.0.max`). Every option list comes from `GET /api/vocab`,
read off `catalogue.py`; nothing is hard-coded in the UI. Every edit is checked live
(`POST /api/check`): the errors appear on their fields and the generated label of every rule and
record is recomputed, so the curator reads what the app will say before saving.

- **entry**: `extents` (the scope that clips every rule), `includes_tributaries`, `scope_note`,
  `matched` (add from a registry search, remove). Read-only, with the reason shown: `entry_id`,
  `name`, `display_name`, `region`, `regs_verbatim`, `source_pages`, `symbols` — passed through from
  the synopsis row, and refused by the backend if changed.
- **rules**, every type (incl. `angler_closure`): `type`, `obligation`, `verbatim`; `species`,
  `species_except`, `when_targeting`, `closed_to` (Who), `water`, `origin`; `take`, `unlimited`,
  `may_target`, `period`, `per_daily`, `within`, `record_retention`, `lengths` (ordered ranges);
  `when` (dates, hours, weekdays; `unparsed` read-only); `gear` (ordered clauses — slot, of,
  allow/only/ban, except, members, max/min/unlimited, must_be, requires, when, unless), `while`,
  `conduct`; `aspect`, `level`, `max_power_kw`, `max_kmh`; `exempts`, `suspended_while`,
  `condition_of`, `derived_from`, `authority`, `notice`, `standing`; `extents`,
  `includes_tributaries`, `tributaries_only`, `tributary_excludes`, `extent_text`,
  `unresolved_locators`; `review_reason`.
- **licensing**, every kind: designation (classified, unit, unit_name, when, extents, tributaries,
  excludes, steelhead stamp during/waived, suspended_while), not_classified, requirement (doing,
  who, who_except, satisfied_by paths — hold / accompanied_by / as, with quota — conduct, water, on,
  authority, when, extents, restates), licence_terms, exemption, alternative. Add a record of any
  kind; remove one.
- A **raw JSON** view on every rule and record, validated live like everything else — for an extent
  key the Extent model does not declare, or to see exactly what will be written.

The source row-crop is found by the row's printed text; when several rows print the same text and
none carries this entry's name, no crop is shown rather than a wrong one.

## Tests

`pipeline/tests/test_curation_review_api.py` drives the API with `TestClient` on a temp copy of the
catalogue: GET, unchanged PUT (byte-identical), an edit to each editor's field, and invalid edits
refused at the field — one entry of every rule type and every licensing kind.

T135 / T142: `test_review_queue.py` (the order, the pre-checks, the ticket round trip, the marks
commit), `test_curation_review_queue.py` (the one queue, the run's facts on screen, a split added
mid-review, a wrong mark as a ticket, refusals in the reviewer's words) and the BROWSER test
`test_curation_review_browser.py` (`-m slow`: the real backend in fix mode and Vite, each on a free
port of its own, driven by Playwright — the fix-mode draft guard on `j`, and Enter after clicking an
answer records the mark).

To run the app against a copy (never the real files): `CURATION_ENTRIES_DIR=<copy>` on the backend,
and `CURATION_API=http://127.0.0.1:<port>` on `vite` when the backend is on another port:

```bash
# backend on :8791 against a copy, frontend on :5181 pointed at it
(cd curation-review/backend && CURATION_ENTRIES_DIR=/tmp/cat PYTHONPATH="$PWD/../.." \
   ../../.venv/bin/uvicorn app:app --port 8791) &
(cd curation-review/frontend && CURATION_API=http://127.0.0.1:8791 npx vite --port 5181 --strictPort)
```

`--reload` is convenient while editing the backend, but a reload with a page open can hang in
"Waiting for connections to close"; restart the process instead. The first requests after a start
load the graph and geometry (~30 s); the loaders are serialised so concurrent first requests do not
each load their own copy.
