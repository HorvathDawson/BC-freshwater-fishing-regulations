# Handoff — quota & gear regulation tables

State as of 2026-09-17, branch `redesign/stream-sections`, head `8b95fe0b`.

**Gates (all green, verified unfiltered):**

```
.venv/bin/python -m pytest -q          # no path argument — testpaths covers both suites
  -> 1403 passed, 171 deselected
.venv/bin/python -m pipeline.regs.table.comply         -> COMPLIES, 0 unaccounted
.venv/bin/python -m pipeline.regs.table.method_comply  -> COMPLIES, 0 reaching no table
```

Use `.venv/bin/python` with `PYTHONPATH=.`. The conda python on PATH has no pytest.
`rtk pytest` has once reported a clean suite while tests failed — run it unfiltered.

---

## What was built

The browser used to resolve regulations itself, with four independent precedence
ladders. That is gone. Resolution happens in the pipeline, once, and the page renders
what it is given.

### The model — `pipeline/regs/table/`

- **`authority.py`** — `Authority` (superior / province / region) × `Scope` (region-wide /
  area / water / inherited-by-tributary-walk), **two independent typed axes**. These
  replaced a single `rank` integer that conflated "who wrote it" with "what it binds to".
  That conflation was the root of a whole family of bugs where a region's rule written for
  one named river bound every water in the region. `Source.rank` is derived, never stored;
  `Source.is_base` = region-wide.
- **`ledger.py`** — an `Allowance` is one counter: scope (fish, origin, size class),
  outcome, period, pooled, `Source`, dates. **Closures, releases and size bounds are all
  allowances of zero** — one vocabulary, no special cases. A `Ledger` settles them pairwise
  by the owner's ladder.
- **`build.py`** — `base(rules, water_kind)` is a region's standing table, a pure function
  of (region, water kind); `ledger(...)` overlays a section's overrides. **A water-scoped
  rule cannot enter a base and a stream rule cannot reach a lake, by construction.**
- **`rows.py`** — rows are a derived *view*, not a unit of the model.
- **`oracle.py`** — `may_i_keep(species, length, origin, date, creel)` returns a verdict
  naming the counter that decided it. This is the acceptance test for the whole thing.
- **Gear**: `method.py`, `method_build.py`, `method_deltas.py`, `method_print.py`,
  `method_comply.py`. Gear has three kinds of statement modelled as three — a **permit/ban**
  (one governs), **rig conditions** (never compete; they fold), and **"what you may keep by
  it"**, which *is* the quota `Allowance`. That last one is the only shared abstraction.

### The ladder (the domain owner's words, verbatim)

> Regional always overrides provincial (except full closure), and this water overrides
> regional always (except closures unless they are lifted in this water's regs).

### Two invariants that make debugging tractable

1. **`section table == apply(region base, [overrides reaching this section])`**, with every
   difference attributable to exactly one named override. An unattributable difference is a
   test failure. Gear: 820 deltas over 102 sections, 0 unattributable.
   *This is the thing that stops tail-chasing* — a wrong number points at one delta, not at
   the pipeline.

2. **The checks can fail.** They could not, before:
   - the totality test compared a frozenset against itself (same predicate both sides) — a
     tautology for every possible ledger;
   - `comply` filled 89% of its buckets from the *input record's own fields*, using the same
     tests the generator uses to *skip* a rule, so anything the generator refused to look at
     was accounted for by construction.

   Now every bucket is proved against an **output**, and mutation tests pin it: flipping a
   rule's `type` to `advisory` or adding a stray `water: "lake"` now reports UNACCOUNTED.
   **Never reintroduce a check seeded from the input.** This has laundered failures three
   times in this project's history.

---

## Source of truth

`data/source/fishing_synopsis.pdf` — 88 pages, edition 2025-2027, git-tracked.
Official URL recorded at `pipeline/regs/extraction/extract_synopsis.py:1412`.

**Page map** (each regional quota table is one page; provincial spans 8–12):

| region | 1 | 2 | 3 | 4 | 5 | 6 | 7A | 7B | 8 |
|---|---|---|---|---|---|---|---|---|---|
| page | 15 | 23 | 30 | 36 | 48 | 55 | **64** | 70 | 74 |

Region 7A's page header is a bare "Regional Regulations", not "REGION 7A" — every
heading scan misses it. Region 5's header is doubled-character encoded
("RREEGGIIOONN 55"). Page 15 carries *both* a Region 1 box headed "(excluding Haida
Gwaii)" and a separate "Haida Gwaii Daily Quotas" column.

### ⚠ The repo PDF is NOT byte-identical to what gov.bc.ca serves

Three files, all edition 2025-2027, three checksums: repo copy `6eb14ec7`; the
extractor's `SYNOPSIS_URL` served `930d743a` on 09-16 and `33ba0893` on 09-17 (it moves
day to day); the long-named official file `c76f6851` (stable across both fetches).

**Do not claim byte identity for the repo copy.** What is claimed instead, and tested:
`quota_print.cross_check` proves every quota box in the repo copy reads line for line as
the same box in the regional chapter that *is* byte-identical to gov.bc.ca (10/10 boxes).
`test_the_source_is_the_edition_it_claims` pins path, edition string read from page 1, and
md5 — a swapped PDF fails there.

### Validation rules that must not be relaxed

- **Never compare against a curated `verbatim` field.** A curated verbatim is itself the
  thing under test; diffing against it is a tautology that agrees with itself.
- **A ✓ without a found sentence is a ✗.** This rule caught a false tick where column
  debris had broken the printed sentence while the table side matched.
- The `.txt` extractions in `scratchpad/synopsis/` are **scrambled by rotated map art** —
  use `pdfplumber` layout extraction, not those files.

### Current print agreement

- **Quota: 382/386 lines agree.** The four are one finding — Haida Gwaii "3 Dolly Varden".
- **Gear: 134 lines over 20 standing tables, 0 failures.**
- Both diffs are **executable** — a test fails on a new disagreement *and* on a vanished one.

---

## Open curation defects (data, not code — do not paper over in code)

| # | defect | effect |
|---|---|---|
| 1 | `r5:fraser_river@5-2` extents are `{"op":"whole"}` | binds all 20 Fraser stretches; a Region 5 sturgeon closure **shuts the lower Fraser fishery 304 days/yr** |
| 2 | `r3:fraser_river@3-14.r2` unbounded `downstream_of` | 76 days of a closure Region 2 never wrote, on Fraser runs 0–2 |
| 3 | `r4:kootenay_lake_s_tributaries.r1` + its exclusion filed as `advisory` | tributary walk reaches the Elk/Fording; **false closure** on a printed fishery below Elko Dam |
| 4 | four inverted `band: true` flags — `r7:gwillim_lake.r1`, `r7:lower_blue_lake.r2`, `r7:williston_lake_in_zone_b.r4`, `r2:coquitlam_river.r3` | tells the angler to release exactly what the book lets them keep. **`size.py` is correct — do not change it**; ten other rules use identical wording with the flag right |
| 5 | `z5:white_sturgeon` r1–r3 bind 0 sections | 61 days/yr of silence where the book says catch-and-release |
| 6 | `z2:species_quotas.r1` ("Bass: 20, excluding Mill Lake") and `z2:protected_species` bind 0 sections | no bass number anywhere in Region 2 |
| 7 | `z6:steelhead_stream_closure.r1` self-exempts | worked around, not fixed |
| 8 | `z1:bait_ban_streams.r1` reaches Haida Gwaii | the islands get the all-year ban plus their own seasonal one. Quota side already separates HG correctly; this is gear-only |
| 9 | `zp:set_lining.r2–r4` prose `extent_text` + `scope: section` | land on no stretch |
| 10 | `r4:pend_doreille_river.r1` exempts only the region's barbless rule, not the province's | river reads "exempt" and "barbless required" at once |
| 11 | `r4:goat_river.r3` carries an **exclusion** in `extent_text` | never binds. Only gear rule in that shape |
| 12 | `zp:bait.r3` typed `area:region:2` but names three rivers | rides as a caveat on all Region 2 water |
| 13 | Kootenay Lake Main Body trout/char total | the printed line genuinely does not say whether the water's rainbow 10 sits inside or beside the regional 5 — **a question for the region, not for code** |

**Retracted** (kept on record): "set lining Allowed on 15 Skeena stretches" was a fault in
the gear agent's own reference, not the data — `zp:set_lining.r2–r4` carry
`feature_types: [lake]`, a second spelling of water kind that its kind test did not read.
Both tables now read `water` and `feature_types`. The same blind spot had moved 7 Skeena
quota ledgers.

---

## Open work

1. **`regs-v3.html` still resolves in the browser.** The app page has not been switched to
   the ledger. The artifacts are the only surfaces consuming it. This is the largest
   remaining piece.
2. Groups still named by exclusion — "Any other char" / "Any other trout". The readability
   review ruled this out; member names beneath are a partial fix.
3. Duplicate answers from two authorities print twice (Shuswap wild steelhead:
   `release [Region 3] · release [All of B.C.]`) — dedupe to the closest authority.
4. Weekday and time-of-day rules never decide a `(month, day)`; the client must apply them.
5. Region 7B possession exceptions are verified against curated text only — no 7B chapter
   PDF was fetched (the full synopsis p.70 now covers the quota table).

## Artifacts

- Quota table + per-region base tables + chain of custody:
  https://claude.ai/code/artifact/c16d220a-6eee-4149-9692-49c04862e0a2
- Gear/methods table + 20 standing tables with print diff:
  https://claude.ai/code/artifact/56c5f7d1-fd3e-4109-a44d-44134a28f3ad

## House rules that bit us

- ⛔ **Parser runs are human-only.** Never run `run_parse.sh` or anything dispatching to the
  `claude` CLI — it spends the owner's credits.
- **Never `git add -A`** — a concurrent agent in this repo commits with it and will sweep
  unrelated work into its commits. Stage explicit paths.
- Prefix shell commands with `rtk`. `archive/` and `pipeline/docs/archive/` are prior art.
