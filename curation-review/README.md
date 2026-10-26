# Curation Review — the local entry review app

A small, **local** app for a human to review every parsed catalogue ENTRY against the book: that it
was parsed correctly (T text, R rules), that its rules land on the right water and extent (P place),
and — the final check — that the app's own answer agrees (A). An entry verified on all four is
saved **human-reviewed**; it comes back for a re-check when its text, rules or extent change.

> Status: **built and in use; review only** (T184, T185 — 2026-10-24). The app writes ONE thing, a
> reviewer's mark. It has no editors: a wrong or unsure mark becomes a ticket, and the fix is a
> reviewed change to the curated files (T185), never something this app writes.

## Run it

```bash
bash curation-review/run.sh          # backend :8787 + frontend :5173, Ctrl-C stops both
curl -s http://127.0.0.1:8787/api/ready   # wait for "ready": true (each warm step is timed)
```

Then open **http://127.0.0.1:5173**. Local only — no LLM calls, no credits. Both servers bind
127.0.0.1 only (Vite `host: "127.0.0.1"`, uvicorn `--host 127.0.0.1`), and the backend's CORS admits
only the Vite origin: the tool is not reachable from another machine. `PORT` / `CURATION_VITE_PORT`
move the two ports (a second copy beside the owner's).

**One mode (T134, T185).** The app serves reads and ONE write: a mark (`POST /api/marks`). There is
no fix mode any more — the entry / split editors, entry save, the draft check and split add / move /
delete / rename were removed with their routes, and `test_curation_review_safety` pins that no
backend module but `writes.py` writes a file, its one caller being the mark.
`test_curation_review_screen` calls every route and proves nothing under `data/curated` changed.

**DFO salmon extents (T164).** The top bar's "DFO salmon extents" (or `#dfo/<location_id>`) opens
the second queue: every scraped DFO location, its notice words and rows, its extent on the map. A
DFO mark (scope `dfo`, T R P) carries an extent fix and NEW split proposals by anchor (confluence,
lake inlet / outlet, bridge, dam, sign, map point, existing split), each checked by
`GET /api/dfo/locations/{id}/anchor` and stamped with where it resolved — in the mark only; T185
turns it into a reviewed change.

## What is reviewed: entries, and splits by proxy (T184)

- **Entries only.** The queue holds entries; there is no split-point tier (the old S0) and no split
  card. Every rule's splits are reviewed **through the entries that use them**.
- **The map labels every split each rule names** (`GET /api/entries/{id}/splits`): drawn over
  everything, labelled with its name, amber while still to verify (an open auto-proposal or a
  "Verify:" note), faded when the reach shown belongs to a rule that does not name it; a split
  the served atlas cannot place is listed "not on the map" with why. Each rule shows a **where**
  line with its splits by name, and the entry lists "Splits the rules name" (click to fly to one).
- **A wrong split is a "Place: wrong" mark** on an entry that uses it (problem "split misplaced");
  the ticket names the split for the fix. A rule binding through a split still to verify carries the
  pre-check `split_to_verify` ("check its place on the map").
- The split marks already in the store (from before T184) are history, kept and readable
  (`GET /api/marks?scope=split`, `/api/marks/splits`); a new one is refused.

### The review screen (T134)

Evidence first, then the checklist, with the verdict bar always visible:

- **Evidence.** The heading is the RAW name as printed ("ADAM RIVER (except Eve River)"; the
  app's display name beside it). A water row is cropped from the PDF at 2.6 px/pt, its water column
  above its regulation column, so it reads at ~17 px text; "show page" marks the row on its printed
  page. The boxes come from the `row_boxes.json` sidecar (`python -m
  pipeline.regs.extraction.row_boxes`, a one-off, no credits, ~2 min); without it the pane says so
  with that command. A zone entry's passage is FOUND BY TEXT on its chapter page and the share of
  its words found is shown. The **coverage strip** is the row's text with every stretch no rule or
  record quotes in red.
- **Checklist** (PLAN §2): T text, R rules, P place, A answer, each y / n / ?. Any n is "wrong" and
  asks for a problem category and a note; any ? is "unsure"; all y is "verified" — **human-reviewed**;
  "defer" needs a note. The server decides the verdict (`marks.verdict_from_answers`). While the
  entry awaits its build, P and A wait. A zone entry is marked rule by rule, each rule with its
  impact ("binds N sections in region R"); it is human-reviewed once every rule is.
- **Human-reviewed** (T184): the bar at the top says "Human-reviewed by X on date"; the queue row
  carries "✓ reviewed · X · date". When the entry's fingerprint moves (text, rules or extent — T97's
  typed stale reasons) it reads "Was human-reviewed … changed since: re-check it" and is back in the
  queue; the review filter's "human-reviewed" and "back for a re-check" list each.
- **The answer** is the app's own page, opened by its start link
  (`<CURATION_APP_URL>?regs=<item>&part=<i>&date=<day>`, default `http://127.0.0.1:39218/`) in a
  new tab — the part where the entry's rules hold, its own water first.
- **Keys**: `t r p a` pick a check, `y n ?` answer it, Enter records the mark and moves on,
  `j` / `k` next / previous, `u` the next unsure entry, `o` opens the app page, `m` the map large.
  The URL hash is the open entry (`#r1:adam_river_except_eve_river@1-10`): a reload resumes in
  place, and a marked row stays in the list.
- **Progress and ETA** per tier (P1 Z P2 P3 P4 sweep, `pipeline.regs.review.tiers`) and overall;
  the ETA is the reviewer's own median time over their last 20 entries in this browser.

### The one queue, pre-checks and the output (T135)

- **One queue across all regions** ("all regions", the default) in the **review order**: the
  entries tier by tier (P1 Z P2 P3 P4 sweep), each tier led by its most urgent **risk factor** — no
  match, unresolved, undrawn part, ambiguous cut, cut, several waters, flag — then the most
  severity-A pre-checks, then the book. One function decides it (`tiers.queue_rank`). "book (page)"
  is still offered. The `unresolved` and `ambiguous cut` factors are read from the served reach run
  (`pipeline.regs.review.runfacts`); with no complete run the queue says so and does not claim them.
- **Pre-checks** (`pipeline.regs.review.precheck`; CLI `python -m pipeline.regs.review.precheck`):
  severity A are facts (fails the gate, binds nothing, unresolved, unplaced pieces, ambiguous cut,
  binds through a split still to verify, parser-uncertain, unresolved locator, undrawn part), B are
  hints (a cut picked by hand, a quote not in the row, more than 15 characters no rule quotes).
  Each entry's are listed under its evidence, with every ambiguous cut's candidates and the one taken.
- **Statuses** say what the parse left, not review progress: `parser-uncertain` and `plain`.
- **Output** ("output" beside the progress bar): the wrong and unsure marks as ticket text
  (`/api/review/tickets.md`; files: `python -m pipeline.regs.review.tickets --out <dir>`), and the
  command that commits the marks file and nothing else: `python -m pipeline.regs.review.commit_marks`.

The build it serves is `GENERATED.build()` (`data/generated/atlas/full`); `reuse.py` reads it, so
repointing the app at another build is a config edit, not a code change.

**No endpoint starts a process.** The app never builds or promotes an atlas (T96). A change to the
curated files (an entry, a split) lands through a rebuild the owner runs from a terminal — the
pending-build panel lists what awaits it and prints the commands (`scripts/heavy.sh`); the app's
caches drop themselves when the served files change, with no restart.

### The gate before the full review (T136)

- **Ready signal.** `run.sh` sets `CURATION_WARM=1`: at startup the backend loads everything a first
  entry reads (registry, bundle facts, vintage, run facts, graph, geometry, then the queue once) in
  a background thread. `curl -s http://127.0.0.1:8787/api/ready` lists each step with the second it
  finished and says `"ready": true` when done (or the `error`). Start it before the reviewer sits
  down.
- **Parity (AGENTS 16).** `scripts/heavy.sh parity 30 -- python -m pipeline.regs.review.parity
  --json /tmp/parity.json` compares the app's binding of every rule (outcome, reason, section set)
  with the served reach run's; only a typed cause explains a difference (`parity.EXPLAINED`).
- **False stale.** `python -m pipeline.regs.review.gate stale` reads every mark in force: held,
  stale by an edit, stale by a rebuild alone (`build_only` — the false stales; the gate is 0).
- **A rehearsal on copies.** Every path the tool reads can point at a copy: `CURATION_ENTRIES_DIR`,
  `CURATION_SPLITS_JSON`, `CURATION_MARKS`, `CURATION_BACKUP_DIR` (inputs) and `CURATION_BUILD`,
  `CURATION_REACH_DIR`, `CURATION_BUNDLE`, `CURATION_RELEASE_ROOT` (= the bundle's directory +
  `/release`), `CURATION_TILES` (outputs). The pending-build panel then prints the rebuild ON THOSE
  COPIES, step by step (`vintage.Target`, `rebuild_commands`) — never a live path. A tool serving
  some live outputs and some copies prints no command at all.

---

## Why

The parser (`pipeline/regs/parsing/`) turns each synopsis row into a `CatalogueEntry` whose `rules`
are each bound to a reach by `extents` (op + curated split ids). The parse is validated
structurally, but a rule can be **confident-but-wrong** (bound to the wrong reach, wrong species, a
restriction missed). A human checks each entry against the book, and signs it off as reviewed.

## Marks

- **Marks (T97)** live in the append-only store `data/curated/regulations/review/marks.jsonl`
  (`CURATED.regulations.review.marks`; `CURATION_MARKS` overrides it), never inside an entry. Each
  mark names its reviewer, date, the book's sha256 and the printed pages, and holds the target's
  FINGERPRINTS (`pipeline.regs.review.fingerprint`: text, meaning, binding + its bundle half,
  answer). When something it was held against changes, the mark reads **changed since marked** with
  typed reasons — book text changed / rule changed / binding moved / answer changed / awaiting build
  — so a no-op migration stales nothing and a moved split stales exactly the entries on its water.
  A mark is refused on an entry that fails the gate.
- The **review** filter lists to do / never marked / back for a re-check / changed since marked /
  wrong / unsure / deferred / human-reviewed, with counts; the bar beside it is progress.
- An entry opens by URL: `http://127.0.0.1:5173/#r5:dean_river@5-9`.

On each entry the reviewer sees: the book (the row crop, and **show page** for the whole printed
page; page numbers link to the repo's PDF at the right PDF page, since `source_pages` are PRINTED
numbers and the PDF index differs by 2-6); every rule's generated label, quote, where it binds (its
splits by name), `review_reason`, not-yet-mapped part and unresolved place; the reach builder's
answer for the entry as it is in the file, with what it decided (tributary walk, `rest` of the
water, tidal water removed, confluence cuts, straddling pieces, ambiguous cuts, region clips); its
licensing; and the map.

---

## Data it reads / writes

| Source | Path | Role |
|---|---|---|
| Entries (read) | `data/curated/regulations/entries/catalogue/region-*.json` | the review surface |
| Splits (read) | `data/curated/waters/splits.json` | split labels, notes, what is still to verify |
| Registry (read) | `data/generated/atlas/full/registry.json` | item identity, **`boundaries`**, `variants`, `mus`, `section_ids` |
| Geometry (read) | `data/generated/atlas/full/graph.pkl`, `geometries.pkl`, `waterbody_polys.pkl` | streams, split points, lakes, areas — served as GeoJSON for the map |
| Marks (append) | `data/curated/regulations/review/marks.jsonl` | the one write |

**The one write goes through `backend/writes.py`**: the marks store is backed up first, byte for
byte, into `data/generated/regs/entries_backup/curation-review/<UTC stamp>-mark/…`
(`CURATION_BACKUP_DIR` overrides it; the newest 500 snapshots are kept, `CURATION_BACKUP_KEEP`), then
appended atomically under the store's lock.

**Caches follow the build.** Before every `/api/` request the backend stats the served build's
files and the bundle (`reuse.ensure_fresh`); if any changed (a promote, or a rebuild from a
terminal) every build-derived cache is dropped. The place-namer is keyed on splits.json, so a split
label edited by hand shows on the next request.

## API

`API.md` has the request/response shapes. The one write is `POST /api/marks`.

## Tests

`test_curation_review_api.py` (labels served, marks held against fingerprints and refused, the
human-reviewed state, the splits each entry names), `test_curation_review_safety.py` (one write;
no process; loopback only), `test_curation_review_screen.py` (evidence, progress, the route sweep
that pins `data/curated`), `test_curation_review_queue.py` (the one queue, the run's facts on
screen, a split added mid-review checked through its entries, a wrong mark as a ticket),
`test_curation_review_vintage.py` (one vintage on screen), `test_curation_review_writes.py` (the
mark's backup and all-or-nothing commit; caches), `test_review_*.py` (the pipeline side), and the
BROWSER test `test_curation_review_browser.py` (`-m slow`: the real backend and Vite, each on a free
port of its own, driven by Playwright — `j` moves on, and Enter after clicking an answer records the
mark). The frontend has no unit runner: `npx tsc -p tsconfig.json --noEmit` in `frontend/`.

To run the app against a copy (never the real files): `CURATION_ENTRIES_DIR=<copy>` (and
`CURATION_MARKS=<copy>`) on the backend, and `CURATION_API=http://127.0.0.1:<port>` on `vite` when
the backend is on another port:

```bash
# backend on :8791 against a copy, frontend on :5181 pointed at it
(cd curation-review/backend && CURATION_ENTRIES_DIR=/tmp/cat CURATION_MARKS=/tmp/marks.jsonl \
   CURATION_VITE_PORT=5181 PYTHONPATH="$PWD/../.." ../../.venv/bin/uvicorn app:app --port 8791) &
(cd curation-review/frontend && CURATION_API=http://127.0.0.1:8791 CURATION_VITE_PORT=5181 npx vite)
```

The first requests after a start load the graph and geometry (~20 s; `CURATION_WARM=1` does it at
startup); the loaders are serialised so concurrent first requests do not each load their own copy.
