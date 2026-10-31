# T332 — DFO salmon quotas in the SAME quota/size model as the game-fish (trout) rules; wording list checked + accepted

Status: IN REVIEW — branch `dfo/t332-shared-quota` (a618808c2, 29ddde218, 616a3ec93), merges after 0.4.0 ships. (owner 2026-10-30: "please tidy them. yes and double check them all. and how they lead to the quota
structure data (which should have same format as trout ones for consistency like same size datatype, quotas etc.)").

## Why
T320 gave DFO salmon its own verdict shape `q` = {d, h, o, mx, w, mo, a, u}. The game-fish rules already have a quota
tree (aggregate + children), `lengths` (ordered size ranges, each with its own take — memory lengths-replaces-size-
polarity), `when`, and gear. Two shapes for one idea ("how many, what size") is the duplication the owner wants gone.

## Work
1. **Wording list:** tidy the split scraps ("f", "fraser river watershed" — fix the clause split, not the entry), check
   all 85 entries one by one against real cells (each entry: meaning correct? reads correct? example cell), then mark
   them `accepted` (owner approved 2026-10-30). Any entry whose meaning is doubtful → listed for the owner, not accepted.
2. **One model:** map every structure meaning to the game-fish types — daily count → the quota node's take; "only N
   over X cm" / "N of which may be over" / "none over" / "max size" → `lengths` ranges; hatchery-marked / wild
   allowance → the same retention qualifiers the regs use (or add ONE shared field both use); monthly → the quota
   period; the region bucket → the aggregate tier. Document the mapping table in the ticket.
3. Convert pipeline (`dfo_salmon/quota.py`, `typed.py`, `deliver/salmon.py`), feeds worker TS port, app (`salmon.ts`,
   `SalmonCard`) to the shared types; the app renders salmon limits with the same presenter as trout limits.
4. Tests: every saved DFO cell reads to the shared model; TS/Python parity; the T320 spot-check five read the same words
   as before; salmon file golden updated.

Depends: T320, T321. Size: L. Ships with the release after 0.4.0.

## Done (2026-10-30, branch `dfo/t332-shared-quota`, not merged — merges after 0.4.0 ships)

### 1. The wording list
**The split, fixed (not the entries):** `wording.clauses` (Python + Worker port) no longer breaks
- inside a lettered section title — Region 6's "F. Fraser River Watershed - There is no fishing for salmon in the portions
  of the Fraser River watershed in Region 6." was three scraps (`f`, `fraser river watershed`, the closure); it is now one
  **scope note** (`kind: note`). It names a watershed SCOPE (the Fraser and its tributaries in Region 6 — section F of the
  cascade, closed outright: `cascade.py`, `entries.py`), never a limit. No other "… watershed" phrase is in a limits cell
  (the others are section titles / the header: "E. Other Mainland Watersheds, except for the Fraser River watershed",
  Region 5B's "(i.e. Region 5 not including the Fraser River watershed)", Region 6's "adult chinook … (other than Fraser
  River Watershed)"); a test holds every registry template naming a watershed to `kind: note`;
- inside a species list ("No fishing for sockeye, pink, chum." was `pink` / `chum` / `sockeye` / `pink or chum salmon` scraps);
- after DFO's comma in "Single, barbless hook in tidal and non-tidal portions of all streams" (`single` scrap);
- before a lower-case word ("1 of which may be more than 77 cm. in length" made an `in length` scrap);
- and a clause's leading dashes are not words ("— FN0372" was `— fn#`, "— —" was `—`).

85 → **78 templates** (14 scraps gone, 7 whole clauses in their place). Zero unknown clauses on all 75 saved pages.

**Each entry checked against its real cells** (all distinct cells per template, fixtures + cache raw + history): meaning and
reader right → **77 `accepted`**. **Doubtful, left `proposed` (owner):**
- `no fishing for chinook from a point n m above fort babine bridge to nichyeskwa creek` (Babine River chinook, archived
  2020 / 2022 pages: "4 per day, only 1 over 65 cm. No fishing for chinook from a point 100 m above Fort Babine bridge to
  Nichyeskwa Creek."). The words close PART of the river; `typed.decode` reads "no fishing" for the whole row, so the
  verdict is **closed** for the whole water (flagged `u` by the unread "100", so no number shows, but the status is
  wrong). Fix belongs in the reader (a part-water closure inside a limits cell), not the registry. Not on today's pages.

Found while checking (DFO's own cells, not registry meanings — the entries are right): Region 6 2020 "Chinook | Skeena
River from Lakelse River mouth … | No fishing for chum." and Region 5A "Coho" / "Sockeye | Quesnel River | No fishing
for chinook — FN0000" — the limits cell names another species than the row's species cell; the row is read for the
species cell's fish. Archived / closed-header pages only.

The registry is now `dfo-wording/2` (the split changed, so a `/1` registry's templates are not this split's): each reader
refuses the other's format and uses its compiled-in copy — a feeds-worker deploy and `deploy/staging/sync-inputs.sh` can
run in either order without false "new wording" alerts.

### 2. One model — the mapping
The typed rule was already the game-fish `CatalogueRule` (take, `lengths` LengthBands, `origin`, `period`, `within`).
What was bespoke was the delivered verdict `q`. It is now a **`TreeTier` of the answers model**
(`pipeline/deliver/answers/model/rows.py` — the trout card's quota-tree node; every salmon tree validates against it in
the tests). ONE shared field added: **`TreeTier.period`** ("monthly" | "annual", only on a size or count tier; absent = a
day's number) — the catalogue has `Period.monthly/annual`, the tree had no clock.

| DFO meaning (example) | rule (CatalogueRule) | delivered (game-fish quota tree) | app (trout presenter) |
|---|---|---|---|
| daily count "4 per day" | `take: 4` | count tier `{tier: count, n: 4, says: "4 a day", species}` | `tierWords` → "**4** a day" |
| hatchery-marked "4 hatchery marked per day" | `origin: hatchery` | the tier's kind `name: "hatchery-marked coho"`, `folded` (the tier's line "4 a day, hatchery-marked only" says it) | tier line only |
| wild allowance "1 of which can be unmarked" | sub-rule `origin: wild, take: 1, within` | a 2nd kind `name: "unmarked (wild) coho", n: 1` (not folded) | `kindWords` → "up to **1** unmarked (wild), the rest hatchery-marked" |
| "only 2 over 50 cm" / "N of which may be over" / "only two over" | sub-rule `lengths: [{min_cm: 50}], take: 2` | size tier `{tier: size, n: 2, over_cm: 50}` | "only **2** over 50 cm" |
| "none over 50 cm" / "maximum size 35 cm" / "80 cm or less" | sub-rule `lengths: [{min_cm: X, take: 0}]` | size tier `n: 0, over_cm: X` + the kind's `sizes {cm: [[0, X]], in: [[0, in]]}` (TreeSizes, the trout kind's size datatype) | "none over 35 cm" |
| monthly "Monthly quota = 1 over 65 cm" | sub-rule `period: monthly, lengths [{min_cm: 65}], take 1` | size tier `n: 1, over_cm: 65, period: "monthly"` | "1 over 65 cm a month" |
| all-salmon row (species "All") | `species: [SA]` | count tier `species: ["SA"]`, "4 a day, all salmon combined here" | tier line |
| not one reading (unread number, new wording, two rules disagreeing) | `review_reason` | no tree; verdict `u: 1` | "Limit: see DFO's words below" |
| region bucket "daily limit … combined is 4" | header note | `bucket.tree` = the TOTAL tier `{tier: total, n: 4, says: "Up to 4 salmon a day in total"}`; `possession`, `t`/`tp`, `min_cm`, `ids` beside it | `tierWords` + " · 8 in possession" |
| header annual / monthly "annual limit for chinook over 50 cm is 10" | header note | the total's children: size tier `n: 10, over_cm: 50, period: "annual", species: [CH]` | (shown with the species' notes, as before) |
| water bucket (Squamish aggregate coho) | header note on a water | OWN tier `{tier: own, whose: "Squamish River", n: 1, species: [CO]}` | `tierWords` |

The wording is the store's (`says`, like every trout tier), the same words as T320 everywhere; the app composes none
(SALMON_WORDS.quota keeps only possession, the water-bucket sentence and the separator).

### 3. Converted (the bespoke `q` removed everywhere)
- Pipeline: `quota.species_tree` / `walk` / `words` / `bucket` / `over_bucket` (FORMAT `dfo-quota/2`); `deliver/salmon.py`
  verdict `tree` / `u` (file **`salmon/3`**, version byte 3); `live.py` feed **`dfo-feed/2`**, `_upgrade`: a run carrying a
  `dfo-feed/1` index drops `q` from carried verdicts and re-reads each region's bucket — never the old shape in a new feed.
- Worker: `src/dfo/quota.ts` (`speciesTree`, `walk`, `words`, `bucket`), `salmon.ts` verdict, `live.ts` FORMAT + `upgrade`,
  `DFO_REV` bumped (cached regions re-type once).
- App: core `salmon.ts` (`SalmonQuota {tree, u}`, `SalmonBucket {tree, …}`, `quotaLine` = tierWords / kindWords over the
  tree, `quotaWords` its text, `salmon/3` decoder), `salmonFeed.ts` (`dfo-feed/2`), data `feed/salmon.ts`;
  `SalmonCard` draws the quota, the bucket and the water bucket with `LineText` from the presenter's runs.
- Contract: `release/contract.py` salmon/3 → `pack.generated.ts`; `answers.generated.{ts,json}` (`period`).

### 4. Format / version impact (what the release needs)
- **Release:** the `salmon` file is `salmon/3`; an older app's decoder refuses it (version byte) — it does not mis-read.
  Publish the first release built with this code with **`minAppVersion` = the app version that carries T332** (as paint/5
  did): older apps keep their data and say "update the app".
- **Live feed:** the Worker publishes `dfo-feed/2` at the same `feeds/dfo/index.json`. An older app refuses it
  (`parseSalmonFeed` checks the format) and keeps its LAST CAPTURE (shown with that capture's time) — it never mis-reads,
  but it gets no live DFO change until updated. A T332 app refuses a captured `dfo-feed/1` and answers from its release
  file until it fetches a `dfo-feed/2`.
- **Order:** ship the app + release (with minAppVersion) first, then deploy the feeds worker; the wording registry sync
  is safe in either order (`dfo-wording/2`).
- Feed size: the trees add ~22 kB to the ~74 kB index (now ~95 kB; budget raised to 100 kB in `test_dfo_live`).

### 5. Tests
- `pipeline/tests/test_dfo_quota.py`: the five spot-checked places (Chilliwack/Vedder, Stave, Kispiox, Stamp, Quesnel 5A
  closed by its header) say T320's words; **every open cell of the tracked pages** says the words T320 gave it
  (`fixtures/dfo_salmon/t320_words.json`, recorded from T320's `q` through the app's old `quotaWords` before the change);
  each tree validates as `TreeTier`; shapes (hatchery folded, wild kind, monthly `period`, size datatype); bucket total
  tier + annual / monthly children; `over_bucket` mutation. One-off check over the whole corpus (75 pages, 329 open
  verdicts): 0 word differences, every tree a valid `TreeTier`.
- `test_dfo_wording.py`: the split makes no scraps (7 cells), no scrap is an entry, all accepted but the one doubtful,
  watershed = note.
- Worker `test/dfo.quota.test.ts`: the TS port over the same t320_words cells (TS == Python == T320 words), shapes,
  disagreeing rules → `u`; `dfo.parity.test.ts` (feed byte-parity, all 6 scenarios incl. a `dfo-feed/1` prev state);
  `dfo.wording.test.ts` (+ a `/1` R2 copy is refused).
- Core `salmon.test.ts` (trees; the trout presenter's runs), `salmonFeed.test.ts` (a `dfo-feed/1` is refused),
  `SalmonCard.test.tsx` (LineText runs, numbers as `n:big`); golden `salmonFeed.golden.ts` regenerated.
- Runs (unfiltered): worker `npx vitest run --root .` 18 files / 173 tests pass; pipeline DFO / salmon suites pass
  (except T321's 2 known data-driven failures in `test_dfo_salmon.py` and atlas/bundle-less errors in this worktree);
  app `tsc -b` clean; app vitest: the remaining failures are pre-existing on the base (checked on a clean checkout:
  check-colours, check-tickets, data-vs-ui, back-layers, one-parts-list, back.test) or read the main checkout's live
  release (conformance `*.release.test.ts`).

## Owner
- The one doubtful wording entry (Fort Babine part-water closure) — decide: a reader fix (part-water closure inside a
  limits cell) or accept as-is (archived only).
- Ship: publish the next release with `minAppVersion` = the T332 app version; then deploy the feeds worker; run
  `deploy/staging/sync-inputs.sh` (registry `/2`).
- Note: two ticket files carry the number T332 (this one and `T332-ios-walkthrough-card-hidden-over-sheet.md`); the
  ticket guard flags it — the iOS one needs renumbering.
