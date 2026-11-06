# T366 — DFO live refresh that is clean to run (region 2 FN1179 first)

Status: DONE (2026-11-05) — items 1–5 built and run. The feeds-worker half (row digest) ships WITH the next app
deploy, never ahead (owner 2026-11-03: "make it so that this command works cleaner in future too"; batch 3 of
RELEASE-PLAN-0.5.0.md).

Why: T361 showed a refresh can silently move owner marks to "rule changed" (the typed rows were a season old, then
re-emitted from a newer page). Region 2 now needs one: staging's live fetch saw FN1179 (Fraser, mainstem upstream of
the CPR Bridge at Mission: chum 2/day Nov 3–30), which no saved page has (newest region 2 copy 10-28, modified 10-16).

## Work
1. **Preview before writing:** `live run --regions N --dry-run` fetches and parses, writes nothing, and prints what
   would change: new / dropped / reworded places, rows changed per place (old → new, with the FN number), and every
   owner mark that would read "rule changed" or "binding moved". The real run prints the same summary.
2. **One vintage, stamped:** the typed file records the sha256 + dateModified of the raw page it came from; the review
   app and the queue warn (banner) when typed/ is older than the raw page, or the raw page is older than N days in
   season. No more reviewing last season's rows (T361's cause).
3. **Archive the fetched page:** every live fetch saves a dated copy into `cache/dfo_salmon/history/` (as the Wayback
   seeding does), so the wording archive dates the new wording and a later "historic or new?" question is a lookup.
4. **Owner reviews PLACES only (owner 2026-11-03):** "only location ones we need to really really review. we can have the
   digest say the other rule change with text and how it was interpreted ... (still want a urgent notification if the
   rule couldnt be parsed using the table structure)". So:
   - a mark is staled only when its PLACE or BINDING moved (new / dropped / reworded location, fingerprint or geography
     change). A rule-row change never stales a mark: the location answer stands (T361's six close this way; fix
     `test_marks_never_orphaned` to match);
   - every rule-row change goes into the DFO digest: place, old row → new row (verbatim, with the FN number) and how it
     was read (species, dates, limit, gear), for a quick skim; anything wrong is fixed after;
   - URGENT notice (stays, T321 path) when a row cannot be read from the table structure, or when a row's location
     matches no curated DFO place (that one also goes to the review queue: it is a place question).
   Marks are never edited.
5. **Run it:** region 2 refresh (FN1179), then any other region whose live page is newer. The owner re-reviews only
   the places the preview lists as new or moved; FN1179's rows land in the digest, UNLESS the worker cannot match the notice's location to a curated DFO place (owner 2026-11-03): an unmatched location goes to the review queue as a place to curate, with an urgent notice. No credits; network only.

Depends: none (T361 found the cause). Size: M.

## Done (2026-11-03, items 1–4 + the item-5 dry run)
- **1 Preview** `live run --regions N --dry-run` (`live.peek_fetcher`: `fetch.fetch_page`, nothing written — no cache,
  manifest, history, feed, typed rows, notices). `regs/dfo_salmon/refresh.py` compares the page with the typed rows in
  use: PLACES new / reworded / dropped / revived / section moved (`entries.reconcile`), ROWS changed per place (old → new
  verbatim, FN numbers, how each new row was read), MARKS that would move (every owner DFO mark of the region through
  `marks.stale_reasons` with the new rows, plus marks on a dropped/moved place), URGENT rows. The real run prints the same.
- **2 One vintage** `locations.emit_html` writes `page: {sha256, date_modified}` + `notes` (header) into rules/; `feed.build`
  carries both into typed/. The real `live run` re-emits locations/ rules/ typed/ from the page it read (`live.emit_region`).
  `deliver.salmon.load_headers` reads the typed file's `notes`; an old typed file falls back to the dated cached page, and
  NEITHER is now a `SalmonError` (T357 point 1). `refresh.vintage` (typed older than the saved page; saved page not fetched
  for 7 days, Apr–Dec) → `/api/dfo/progress` `vintage`, the review app's startup log, and `live queue` / the real run print it.
- **3 Archive** `fetch.archive`: every live fetch saved to `cache/dfo_salmon/history/region<slug>_<stamp>.html`, one copy
  per page (skipped when the newest copy is the same page).
- **4 Places only** `marks.moved` holds a DFO mark against text + binding only (`marks._place_only`; the stamped meaning is
  kept, not compared). T361's six hold; `test_marks_never_orphaned` green (the rows case now pins "no stale").
  Row changes → one digest notice per page (`dfo-rows:<slug>:<sha12>`, `urgent: False`). Urgent (`urgent: True`): held
  page, unplaced place (drift / new / new_water), new wording, unread row (`dfo-unread:…`), header/text changes.
- Tests: `test_dfo_live.py` (+6), `test_marks_never_orphaned.py`, `test_review_dfo.py`.

### Item 5 dry run (2026-11-03, live pages)
Region 2: typed page 2026-04-01 → live 2026-11-02. PLACES: reworded ×2 (Fraser "mainstem waters upstream of the CPR Bridge
at Mission" — likely `2:fraser-river:mainstem-waters-upstream-of-the-cp`, which reads as dropped; and "mainstem waters
downstream of the CPR Bridge at Mission to the Highway 1 Bridge at Hope" — likely the dormant
`2:fraser-river:mainstem-waters-from-the-downstrea`, 0.90), revived ×1 (Nicomen/Dewdney Slough). URGENT: both reworded
Fraser places match no curated place — the upstream one "All | Apr 1 until further notice | No fishing for salmon", the
Mission→Hope one **"Chum | Nov 3 to Nov 30 | 2 per day — FN1179"** → review queue. ROWS (digest): 15 places — chum
openings FN1122 (Oct 17–Nov 30, 2/day: Alouette ×2, Chehalis, Harrison ×2, Nicomen, Serpentine, Chilliwack/Vedder, Stave),
FN1167 chum non-retention (Ashlu, Cheakamus, Mamquam, Squamish), Capilano chinook split at Sep 5 (FN0960), Coquitlam
chinook non-retention → FN0961/FN1008. MARKS that would move: none.
Other regions newer than their typed rows: **1** (10-05 → 11-03: Cowichan chum FN1196, 2 places) and **6** (08-28 →
09-02: Kispiox coho FN0897/FN0951, 1 place); no place changes, no marks move. 3, 4, 5a, 5b, 7, 8: same page.

### Left (item 5, main session) — all done (2026-11-05)
1. DONE 11-03 (abed46e5f: rc 0, no marks moved). `python -m pipeline.regs.dfo_salmon.live run --regions 1 2 6` (network; writes cache, history, feed, typed).
   Regions 2 and 6 MUST be refreshed before the next bundle: their typed rows have no header and no cached page of their
   date, which `load_headers` now refuses.
2. DONE 11-03 (9d5d52553: entry file committed, the Fraser rewordings folded as variants, owner approved). `python -m pipeline.regs.dfo_salmon.live queue --regions 2`, commit the entry files; the owner confirms the two Fraser
   rewordings in DFO mode (note DFO's new wording says "downstream of … Mission to … Hope" for the reach UPSTREAM of
   Mission — confirm it is the Mission→Hope place).
3. DONE 11-05 (see Outcome). Review-app frontend banner for `/api/dfo/progress` `vintage` (backend only here); the staging worker's DFO job
   (app/workers/feeds) does not split digest vs urgent row notices yet.

## Item 5 run (main session, 2026-11-03)
`live run --regions 1 2 6` (rc 0, no marks moved; 2 urgent: the two reworded Fraser places), then `live queue --regions
2` (2 variants folded). `entries/dfo_salmon/region-2.json` left UNCOMMITTED for the owner's DFO-mode check (OWNER-TODO).

## Outcome (2026-11-05, item 5 point 3)
- **Review app, DFO mode:** `DfoVintageBanner` (curation-review/frontend) reads `/api/dfo/progress` `vintage` and says it
  in the notice dock: typed rows OLDER than the saved page ("you would be reviewing last vintage's rows" — red, cannot be
  hidden, × shrinks it to a pill), a saved page not fetched for a week in season (amber), the check failing (red); each
  note's text, then the commands: `live run --regions … --dry-run`, `live run --regions …`, `live queue --regions …`,
  reload. A failed progress load is shown (was swallowed); a failed vintage check is logged with its traceback.
- **Feeds worker:** `src/dfo/rows.ts` ports `refresh.row_changes` / `how_read` / `rows_text`; `runDfo` (robust) compares
  each typed page with the region's last typed page (priv `rows/region-<slug>.json`, the baseline) and adds ONE
  `dfo-rows:<slug>:<sha12>` notice, `urgent: false` (place, was → now verbatim, FN numbers, how each new row was read).
  `dfoEvents` carries `urgent`; `events.deliver`: a DFO / in-season event with `urgent: false` waits for the daily digest
  (`NOTIFY_DIGEST=daily`); every other DFO notice stays urgent. The DFO email section renders rows as Was / Now / read
  as, with "nothing to re-review" and the local `--dry-run` command. No baseline yet → that page becomes it (logged, no
  digest); an unreadable baseline → an URGENT `dfo-rows-baseline` notice ("NOT compared"), rewritten from the page.
- Tests: worker vitest `dfo.worker` (+2, priv keys + rows baseline), `events` (+1: digest vs urgent, rendered body),
  `email`, `dfo.*` — 106 passed (parity/corpus suites are `describe.skip` as before); `test_review_dfo.py`,
  `test_curation_review_safety.py`. Frontend typechecked (`tsc --noEmit`); the banner NOT rendered in a browser.
- **Left:** deploy the feeds worker with the next app release (main session). The first run after deploy writes each
  region's baseline, so row digests start from the following page change.
