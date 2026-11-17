# T759 — A DFO rule we cannot match is an ISSUE on its river: the colour stays, the line and a banner say so

Status: IN PROGRESS (owner decided 2026-11-15 evening; the feeds half (worker + Python, `feeds/t759-issues`, which holds `data/t759-missing-place-check`) merged 2026-11-16 on `merge/t759`, see "Merge" at the end; left (Claude): the app half (`app/t759-stripe`) merge, the Android benches, then the staging worker deploy)
not merged; left: worker + app halves (the other chat), vectors lines, merge)

Source: main chat 2026-11-15, after T758. Trace with real data: `handoff/PLAN/curation-review/feed-missing-location-2026-11-15.md`.
Decision: `handoff/PLAN/OWNER-TODO.md`, "2026-11-15 evening" rounds 1, 2, 3, 5 and 6 (verbatim there; they are the
authority over this page).

## The defect (unchanged)
The app composes each section's chain of DFO places (narrowest first) with today's feed; a place the feed lacks drops
out and the next, wider place answers (`app/packages/core/src/salmonFeed.ts:279, 288`). In 68 of the release's 435
chains, losing the narrowest place reads some fish and day looser. Worst case: the Campbell River "Maple Street to the
cement block" closure reworded, the stretch reads "4 hatchery-marked" instead of closed.

## The model (owner 2026-11-15)
- **No "not known" status and no "check" reading for a missing place.** Whatever still applies on a stretch gives its
  status and colour.
- **Not an issue, nothing shown:** a place DFO removed; a row replaced by one we match EXACTLY (a fingerprint the
  curated places already hold). The remaining (wider) rule answers. Accepted as fine (round 3).
- **An ISSUE on that river** (rounds 3, 5, 6), kinds:
  - `unmatched` — a NEW wording on the river we cannot match to a drawn place: no curated wording (`why: new`); its
    close match is a place another row of today's page already holds (`near_place_taken`, e.g. the Campbell sibling);
    or the wording joins a curated place that is not drawn (`undrawn`: no extent, not tidal, not a confirmed variant —
    what `live queue` seeds). Its rules are held (or sit on the undrawn place).
  - `drift` — a CLOSE match (top candidate of `entries.reconcile`, same water, score >= 0.5, the place free today): the
    rule APPLIES on that place ("same place, wording drifted"), and it is an issue until the owner confirms it in the
    review app. Also a row that joins a variant only PROPOSED (`duplicate_of` set, `duplicate_confirmed` not true).
  - `closed_area_unbound` — a closed area today's row names that no curated wording names (a `#closed:` piece id
    outside `curated_pieces`): DFO reworded a closed area inside a place, so the bound piece id left the feed and a new
    one binds nothing. (Raised by the other chat; same rule.)
- **What the app shows for an issue:** (1) the status colour everywhere stays what still applies (a drift's rule
  applies); (2) the river's stream line gets a distinct "issue" look, a variation like the bait-ban one, same colour;
  (3) EVERY stretch of that river shows a banner in the salmon section quoting the row's own words (`area` + `rows`),
  until it clears.
- **How an issue clears:** the owner links the row to a place (unmatched: places it; `undrawn`: draws it) or confirms
  the drift (a `variant_of` mark: the write-through sets the seeded wording's `duplicate_confirmed` true,
  `regs/review/proposals.py`) in the review app; the place files change; the next feed run matches exactly and the
  issue is gone. A piece: the owner places the reworded closed area (T573's confirm), the next release binds it.
- **Who:** Python feed — main chat (this branch). Worker + app — the other chat.

### How "drift confirmed" is detected (the feed reads only the curated place files)
Today a reworded wording reaches the place files only through `live queue` -> `entries seed` (a NEW record holding the
new fingerprint, no extent) -> `entries group` (proposes `duplicate_of`, `duplicate_confirmed: None`) -> the owner's
DFO mark (`variant_of`: `duplicate_confirmed: true`; `not_variant_of`: contested, `false`). Fingerprints are appended to
an existing place only for page defaults (`entries.py:424`). So a row's wording is CONFIRMED when its fingerprint is on
a curated place that is not a variant, or is a variant with `duplicate_confirmed: true` (`live.confirmed`); and it is
an issue-free match when that place is also drawn (`live.drawn`). Before `queue`, the row has no fingerprint match at
all (drift or unmatched). After `queue`, before the owner: a proposed variant -> `drift`; a seeded record with no
variant link -> `unmatched`/`undrawn`. Running `queue` therefore never clears an issue by itself.
Tested: `test_t759_a_close_reword_applies_its_rule_and_is_a_drift_issue_until_confirmed` (proposed -> drift, confirmed
-> none, fingerprint on the place itself -> none).

## What this does NOT protect against (residual risk)
- **Our reader misreads the page so a row DFO still prints disappears from our parse.** No unmatched row, no drift: it
  reads as a removal, the wider rule answers, nothing is shown. The owner is told once (`gone` notice: "if the row is
  still printed, the page was misread"). A big misread trips the structural hold (> 25 % unbound or count ±20 %:
  `entries.py:53-55`, region held at the last feed), and `untangle.verify()` refuses a page it cannot untangle. One
  row lost is under both. How often: NOT MEASURED (no per-row misread count exists); it needs a parser slip that drops
  exactly one row cleanly. This is the trace's case (a) when misread — accepted by the owner's rule; what catches it is
  the gone notice (and the owner's DFO review of the page).
- **A close match that is the wrong place.** `_candidates` ranks by text and landmarks alone; "upstream of" and
  "downstream of" the same bridge score 0.92 (`entries.group`'s docstring). Such a drift applies the rule on the
  wrong stretch until the owner refutes it; the banner quotes the row on every stretch meanwhile.
- **A region not read this run** (not asked, held, fetch failed) carries no issues (`issues.unread` says why); its
  answers are the last feed's, labelled stale/held as today. While a region is held, its banners are not shown.
- **A place id renamed between the release and the feed, or a release newer than the worker's entry copies** (trace,
  third way): the release names an id the feed never had; falls through silently. `live check-release` lists it when
  run by hand. Not covered here.
- Trace cases mapped: (a) DFO drops the row -> no issue (accepted); misread -> see above. (b1) close reword -> `drift`,
  rule applies, banner. (b2) far reword -> `unmatched` (`new`), the old place goes, wider rule + banner. (b3) reword
  toward a sibling -> `unmatched` (`near_place_taken`), wider rule + banner. (c) fetch fails / (d) row unparseable /
  structural -> unchanged (last feed / "check" for an unread row, T676). Third ways -> not covered.

## First design: SUPERSEDED (commit 438b9e3f5, removed from the branch)
The "check" rule (`REWORDED_VERDICT`, `MISSING_VERDICT`, rules (i) unplaced-water and (ii) stricter-answer,
`stricter_where`, `_looser`, `missing_places`), the per-region `holds` with `last`, the top-level `known`, and the
release `chains` input to `live.run` / `main` (`--release` is check-release's only again) are all gone. Kept from it:
`gone` (the worker's case a: listed and told once, nothing shown) and `_reindex` (shared by `_carry`).

## Python half (branch `data/t759-missing-place-check`)
`pipeline/regs/dfo_salmon/live.py`: `close_matches` (drift -> its free top candidate, applied in `join`,
`words_by_location`, `closures.live_pieces`), `gone_places`, `issues_of`, `confirmed`, `drawn`, `curated_pieces`,
`SHAPE`; `unplaced_notice` says the banner. `closures.py`: `live_pieces` (piece id -> rule, the row, the area;
`live_piece_rules` reads it). Tests: `pipeline/tests/test_dfo_live.py` `test_t759_*` on doctored fixture pages.
Notices: an `unmatched` / applied `drift` row's issue IS its unplaced notice (`dfo-new|drift|new_water:<slug>:<fp>`,
now ending with the banner sentence); a proposed-variant `drift` and a `closed_area_unbound` get `dfo-issue:<slug>:<key>`;
`undrawn` is not told again (told when it was unplaced; it is in DFO review). Each once (`notify.send_due`).

### Sweep 2026-11-15 (today's 9 live pages fetched read-only, curated places of the main checkout, release
r-f749475d0962): **8 issues on 6 waters, all `unmatched`/`undrawn`; 0 new wordings, 0 drifts, 0 closed areas.**
5a Quesnel Lake (Horsefly Bay line), 6 Babine Lake (`~3`, 400 m mouths), 6 Kasiks River (3 places: below the Upper
Pool up/down, above Hwy 16), 8 Mabel Lake (northerly line), 8 Osoyoos Lake (`~2`, north basin), 8 Shuswap River (`~2`,
middle). Every one is a place the release already lists in `unmapped` (its chain's `u`): the card already says "parts
of this water are not mapped". So with the app rule below, **day-one banners: 0**.

## For the worker and app (the other chat)

### `issues` (top-level in index.json; example)
```json
"issues": {
  "waters": {
    "1:campbell-river": {"r": "1", "name": "Campbell River", "items": ["gnis:…"], "list": [
      {"kind": "unmatched", "key": "unmatched:<fp>", "fingerprint": "<fp>", "why": "near_place_taken",
       "place": "1:campbell-river:downstream-of-the-confluence-with~2", "water": "Campbell River",
       "area": "from the end of Maple Street downstream to the confluence with the Quinsam River.",
       "rows": ["All | Aug 1 to Oct 31 | No fishing"]},
      {"kind": "drift", "key": "drift:<fp>", "fingerprint": "<fp>", "place": "1:campbell-river:from-a-fishing-boundary-sign-at-th",
       "location_id": "<the proposed variant, when it joined one>", "water": "Campbell River", "area": "…", "rows": ["…"]},
      {"kind": "closed_area_unbound", "key": "closed_area_unbound:6:babine-lake#closed:<12 hex>", "fingerprint": "<fp>",
       "piece": "6:babine-lake#closed:<12 hex>", "water": "Babine Lake", "area": "Pierre Ck.",
       "rows": ["No fishing for salmon — within a 400 m radius of the mouth of Pierre Ck."]}]}},
  "no_water": [{"kind": "unmatched", "why": "new_water", "...": "a water no entry knows: no section can show it"}],
  "unread": {"3": "held", "6": "stale", "7": "not asked"}
}
```
Sorted: waters by id, each `list` by `key`. `why` only on `unmatched`; `place` on `drift` and `near_place_taken`;
`location_id` on a proposed-variant drift and `undrawn`; `piece` on `closed_area_unbound`. No issue changes `locs`.

**Every stretch of that river:** the sections of the issue's `items` (the water's registry items, the same field
`feedUnplaced` rows carry and `salmon.ts:771` filters by) through the app's item -> sections index. Not the chains: a
water's chains name only drawn places.

**App rule for `undrawn`:** when the release already lists `location_id` in `unmapped` (its chain's `u`), the card's
"parts not mapped" line already says it: no banner, no line look. Otherwise (a place seeded after the release) the
banner.

### Minimum feed shape (contract)
`index.json` top level `"dfo": {"pieces": 1}` — the same value the worker's `/status.json` serves (integer; >= 1 means
the feed emits closed-area piece locations). General rule: a release may require a NAMED minimum feed shape; an app
holding a release that binds a closed-area piece, given a feed without `dfo.pieces >= 1` (an old worker put back after
the upload gate passed), stops using the live feed for salmon and shows the release's own calendar, said the way a stale
feed is said, reported once (`reportFailure`). Python: `live.SHAPE`, test `test_t759_todays_pages_carry_the_feed_shape…`.

### Vectors (`pipeline/tools/emit/dfo_vectors.py`, changing on the T758 branch — NOT edited here)
In its `live` output (T758), beside the per-region fields: drop the first design's `known` / `holds` lines and add
```python
"issues": got["index"]["issues"],
"dfo": got["index"]["dfo"],
"gone": {s: r.get("gone", []) for s, r in got["index"]["regions"].items()},
```
then regenerate `python-vectors.generated.json` (the other chat). Worker port of the rule: `close_matches` (top
candidate only, free = no exact row today and no earlier drift took it, page order), the join applying it, the issue
kinds and `why`s as `issues_of`, `confirmed` / `drawn` on the entry record.

### Superseded for the other chat
- **T757 app half** ("a release-bound piece the feed lacks reads 'not known, check the notice'", `pieceNotKnown`):
  superseded. A bound piece the feed lacks is a removal (the place answers, nothing shown) or, when DFO reworded it,
  a `closed_area_unbound` issue (banner). The only gate left is the minimum feed shape above.
- **T573 worker hand-off spec**, "not known, check the DFO notice" for a missing bound piece: no longer applies, same
  reason.
- **Worker `REWORDED_VERDICT` "check" + `holds`** for a drift's top candidate (`live.ts:570-582`): replaced by the
  close match applying the row's rule + a `drift` issue. The worker's `ROW_HELD_VERDICT` for an unparseable row
  (T676) is unchanged.


## Added 2026-11-16 00:31 (app chat): the river line's stripe is built (owner's pick C), not merged
- Branch `app/t759-stripe` @ 6c24f94a6 (from `app/t759-issues` 1289d8c20; worktree only, not pushed). Sonnet lane.
  The prototype branch `app/t759-line-looks` is superseded and must never be merged.
- Built: two runtime layers (a paper dash under an ink dash, pattern [3, 2]) down the middle of every section of a
  river with a banner, drawn only in the Salmon view (opacity 0 in every other view), same zoom fade as the bait-ban
  edges, both adapters through the shared style code; tokens `color.issue.stripe`, `color.issue.casing`,
  `dash.issue.stripe`; legend entry "Check rule" (`MAP_KEY.issue`; only wording that fits the two-row legend at
  390 dp); a square "!" badge on the folded Salmon row, label "A DFO rule to check on this water"; a closed-all-year
  Salmon row with a banner now folds open to it.
- Colour: the closed colour fails (contrast 1.00 on a closed line in all three themes; under 3:1 on the open and
  release lines), so neutral ink on paper; `tools/issue-stripe.test.ts` (18 tests) holds it on every Salmon line
  colour, the basemap grounds and 17 greys for satellite, under protan, deutan and tritan.
- Not proven: the native adapter never rendered (shared style code + a native style JSON test only); the lane's
  `check:fast` could not finish for a missing `.venv` link; re-run by the app chat with the link at 00:30: rc 0. No emulator.
- Owner: pictures sent 00:31, To-Do row `t759-stripe-final` (legend words, strength on the dark blue line).
- Left: his two confirms; an emulator look on Android; then it merges WITH the rest of T759's app half and the feeds
  half (`feeds/t759-issues`), benches and a message to the main chat first.

## Owner's answers on the final stripe, 2026-11-16 11:51 (question dialog in the main chat, pictures on screen)
- The legend entry: "\"Check rule\" is fine (Recommended)".
- Is the stripe strong enough on the dark blue "Keep salmon" line: "Make it wider".
To do (app chat, branch `app/t759-stripe` 6c24f94a6): widen the dashes on every Salmon line colour (not only the dark
blue: one stripe, one width), re-run the contrast and colour-blind tests, draw it on the Android emulator (it has only
been drawn on the web), send him the new pictures, then the T759 merge (app half + `feeds/t759-issues`).

## The wider stripe is built, 2026-11-16 12:27 (app chat; branch `app/t759-stripe` 75c966f66, not merged)
Owner: "Make it wider". Scaling both layers up (ink 0.4 and casing 0.85 of the line's width, times 1.5 or 2) was
tried and rejected: the pale casing covered the line. Cause found by the lane: MapLibre measures dashes in units of
floor(line-width), so two layers of different widths drift out of phase. Now ONE width for both tones (0.6 of the
line's width) and the tones lie along the line: dark 3, pale 2, then a gap of 3 where the line's own colour shows at
full width (`dash.issue.stripe` [3, 5] over the new `dash.issue.casing` [5, 3]; equal sums = always in phase). Five
stripe and legend test files pass (36), `check:fast` passes. The before / after picture (light and dark) was sent to
the owner in the app chat at 12:27. Note for readers of the pictures: the lower Campbell is a wide river drawn as a
tinted area, pale in light and dark green in dark theme, before and after; that is not the stripe.
Left: his word on the picture, the Android emulator look, then the merge (app half + `feeds/t759-issues`).

## Owner, 2026-11-16 ~16:55 (app chat dialog, verbatim label)
The wider stripe (picture `stripe-before-after.jpg`, sent 12:27, again 16:55): "Wider is right (Recommended)". The
wider stripe ships as built on this branch.
