# T669 — One "strictest of both regions" for salmon and freshwater straddlers

Status: DONE — 91398f801, 28fdf30fa; shipped in 0.8.4 (cut e769f710a, data r-fec3fab4c5aa; 0.9.0 audit 2026-11-22, every hash is an ancestor of origin/main and of e769f710a). (Earlier status: IN PROGRESS — item 1 done (91398f801); item 2 DROPPED by the owner 2026-11-20; item 3 built on lane/l9-strictest (28fdf30fa); left (Claude): main chat review + merge; ships with the next data release (staging upload re-stages the pair table))

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T669 | One "strictest of both regions" for salmon and freshwater straddlers | DONE | 91398f801, 28fdf30fa | — |

Source: owner 2026-11-11 (T668): "The strictest of the rules is the same logic the fresh water regulations use I
believe; we should double check that and if possible unify that logic." The side-by-side is in
`handoff/PLAN/app-integration/T668-audit.md`, section "Freshwater (Z4 / T183) vs salmon straddlers".

## Today
- Shared: region membership (`registry.regions.section_regions`). Count and size caps are compared with the
  freshwater primitives: `deliver.salmon._caps` / `_both_bind` call `ladder.stricter` and `rules.statement`.
- Not shared: salmon still ranks per-day VERDICTS (`salmon.STRICT`: closed > release > check > open). The freshwater
  side runs `read.effective_rules` step 6 over RULES (`closure_grade`, `release_origins`, `keeps`). The DFO feed
  (worker → app) carries only verdicts and their quota trees, so the feed path cannot hand rules to the freshwater
  reader.
- Differences that are on purpose (keep them unless the owner says otherwise): a DFO region with nothing to say is
  closed (owner 2026-10-27, closed unless DFO opens it); the DFO header's daylight line (`h`).
- Not covered by either side: a pooled count (aggregate tier) and hatchery/wild kinds as caps (salmon shows both
  regions' words); comparing two rules' hour windows.

## Work
1. One table of cases (closure vs open, release vs open, two counts, two size caps, pooled, hatchery/wild, gear,
   hours) that BOTH the freshwater reader and `salmon.strictest` run, each case's expected winner stated once.
2. Move the closure / release ranking onto `closure_grade` / `release_origins` by keeping the answering rule's statement
   beside each DFO verdict (a verdict field, in both the release and the feed). This is a feed format change: the worker
   and the app move with it.
3. Pooled and kind caps as freshwater caps (the aggregate tier is `total` with `whose`), so a mix shows numbers, not
   only words.

## Blocks full unification
The feed composes chains from verdict indices in the app (`withSalmonFeed`), with no rules. Any rule-level comparison
needs its inputs in the feed format (worker + app + `live.compose` together).

Depends: T668. Size: M. Owner: no.

## Item 1 done 2026-11-12 (91398f801)
`pipeline/tests/test_strictest_cases.py`: eleven cases, each outcome stated once; the freshwater step-6 reading
(`ladder.stricter`, two regions' identical statements once) and `salmon.strictest` / `both_gear` / `tighter_hours` must
both reach it. Three differ on purpose, each with its reason in the table: a pooled count (salmon shows both words,
item 3), hours (only the salmon header compares them), a region with nothing to say (DFO: closed, `NOT_LISTED`).
Mutation: a freshwater reader that never displaces fails four cases. Test only: no output can move.

## Owner answer 2026-11-20 01:56

Dropped: item 2. Not doing it. Item 3 stays. owner 2026-11-20 01:56 (lane-plan batch), verbatim: "Make sure their tickets are  removed/completed/no doing labelled".

Status line before this answer: IN PROGRESS — item 1 done (91398f801); left (Claude): items 2 and 3 (feed format change: worker + app + live.compose together); the shared comparison landed in T668 (dfb4ea7c1)

Owner 2026-11-20 11:11 (dialog), after the Qualcho Creek before/after pictures (lane L9, `lane/l9-strictest`): item 3 **"Ship it (Recommended)"**.
Then (owner, same dialog round): coho "only 1 over 50" looked wrong (5B's own line says 2): it is Region 6's cap, the stricter. Owner: **"Name the region (Recommended)"**: a mixed line names each number's region, e.g. "… MU 5-6 to 5-11 combined (Region 5B) · only 1 over 50 cm (Region 6)". Built on lane/l9-strictest before merge.

## Item 2 DROPPED (owner 2026-11-20)
The rule statement on each DFO verdict (a feed format change) is not done. Item 3 needed no feed format bump.

## Item 3 built 2026-11-20 (lane/l9-strictest, 28fdf30fa)
`deliver.salmon`: `_caps` now reads the aggregate tier (`total` + `whose`), a hatchery-marked kind (read as a release
of unmarked fish), an unmarked share and a monthly cap. `_both_bind` keeps a pool apart from a plain count: a plain count
never displaces a pool (the pool covers more waters: Z4's "a narrower cap never displaces a wider", applied to waters);
a pool at or under the plain count displaces it. `_moot` drops a size cap that another cap already covers. `_tree_of`
builds the caps that bind into the tree `quota._one` gives a DFO row with those numbers. `stricter_opening` therefore
returns one region's opening, or (a mix) `{t: both regions' words, tree}`. It returns `u` (words, no number) only when a
number is unread or the caps do not make one tree (two pools). RULINGS Z4a is amended. `test_strictest_cases` now builds
its trees with `quota._one` and has hatchery / unmarked / pool cases. The only case that still differs on purpose is "a
pooled count over a lower plain count": freshwater keeps the plain count only, salmon keeps both, the pool over the
count. No live card has that case today. All 111 trees in the live release's openings read back to themselves.
The app needs no change: it takes the Python answer from the pair table and draws any tree. An app test shows a
merged tree from the table is drawn as numbers.

**Proof.** Side deliver via heavy.sh to `data/generated/side/l9-strictest/out`, release r-3c76ec6cf0d4. Answers diff
against the live r-6258ffded676: 265 sections changed in all. Only ONE two-region chain changed (chain 300, Region 5B
region section beside Region 6 E/A: Qualcho Creek part 2 and 18 unnamed wetlands, 19 sections):
- Chinook, all year: before "Limit: see DFO's words below" (4 per day, only 1 over 50 cm; 4 per day, only 1 over 65
  cm; "Region 5B and Region 6 together"). After: Region 5B's own opening, "Up to 4 chinook a day in total, all waters of
  Management Units 5-6 to 5-11 combined · only 1 over 50 cm" (1 over 50 cm already covers 1 over 65 cm).
- Coho, Jan 1 to Oct 31: before "Limit: see DFO's words below" (4 per day, only 2 over 50 cm; 4 per day, only 1 over 50
  cm). After: "Up to 4 coho a day in total, all waters of Management Units 5-6 to 5-11 combined · only 1 over 50 cm"
  (Region 5B's pool with Region 6's size cap, "together").
The other 246 sections are on 24 one-region chains. On those, only the verdict's DFO words `t` differ ("…, hatchery
marked only"). That comes from T623 F13 (11d7722c6) and today's typed feed, not from this change: a one-region chain
never reaches `strictest`. Part-name wording (T780) is release-wide. The pair table on the live release goes from 2
pairs with both regions' words to 1 with one region's opening and 1 with one quota.
Shots (web, 390 px, Jan 15, release only, no live feed): `T669-shots/before-qualcho-creek-jan15.png`,
`T669-shots/after-qualcho-creek-jan15.png`.
Under the live feed the app reads the pair table. `deploy/staging/upload.sh` re-stages it (sync-inputs `dfo-pairs`)
with the release, so the feed path changes on that upload.

## Owner 2026-11-20: "Ship it" + "Name the region"
The owner asked why coho reads "only 1 over 50 cm" when Region 5B says 2: that cap is Region 6's. His answer:
"Name the region". On a MIXED line (numbers taken from both regions) each number now names the region it came from:
"Up to 4 coho a day in total, all waters of Management Units 5-6 to 5-11 combined (Region 5B) · only 1 over 50 cm
(Region 6)". A line taken whole from one region stays as it was (chinook on Qualcho reads Region 5B's own).
- How it works: `deliver.salmon.region_label` gives each group's region from its places' page slugs (a `book:` place
  names none). `strictest(answers, regions)` and `stricter_opening(a, b, ra, rb)` tag each cap with its region.
  `_name_regions` adds " (Region …)" to the words (`says`) of each tier or kind line in a tree mixed from more than one
  region. A count that is hatchery-marked only because of the other region names both regions. `_caps` reads the
  labels back, so a third region folds in correctly.
- Who passes the regions: `chain_calendar` (the release), `live.compose` (the feed reference) and
  `salmon_pairs.pairs` all pass them. If the same two openings in another region pair would name other regions, the
  table holds the numbers with no names (the table is keyed by content), never a wrong name.
- The app draws the stored words as they are: no app code change and no format bump.
- Tests: `test_t669_name_the_region_on_a_mixed_line` (the `_name_regions` no-op mutant fails it) and
  `test_t669_the_release_and_the_live_feed_name_the_same_regions`. The app test now draws a labeled merged tree.
  RULINGS Z4a amended.
- Picture: the Jan 15 after shot is re-taken (`T669-shots/after-qualcho-creek-jan15.png`, side release r-6b6f7a2b4777, which differs from r-3c76ec6cf0d4 only on chain 300).

## Review leftovers (lane LF, 2026-11-20)
- 5dfb14add — "Region 5B · region": Region 4's and 5B's page-default places carry the page scope "region" as their
  `section`. `salmon_who.places_named` now drops a page scope (`cascade.is_page_scope`), and the app's `placeFrom` skips
  a literal "region" (releases already on phones). Tests: `test_a_page_default_place_names_no_section`, the core
  `placeFrom` test (both fail without their half). Measure: the live release names 2 such places, on 127 chains;
  they now read "Region 4" / "Region 5B".
- 5ff9d60f4 — labels agree: when the same two openings meet in two region pairs, `salmon_pairs` stored an unnamed
  merge while `chain_calendar` and `live.compose` named the regions. The table now also holds `named`
  ([a, b, region of a, region of b, answer], one per region pair, each what `live.compose` composes there); `pairs`
  keeps the unnamed merge for apps before 0.8.4. The feeds worker carries `named`; the app asks it first, keyed by its
  groups' regions (`regionLabel`, the Python's `region_label`). Additive: no `dfo-pairs` format bump (a bump would make
  0.8.3 drop the whole feed). Tests: `test_t669_the_pair_table_names_the_regions_of_each_region_pair` (fails with the
  old table), the worker's "named answers ride into the feed", the app's "each chain takes its own region pair's named
  answer" (fails without the lookup or without the labels). Measure: the live table is byte-identical (one region
  pair, 5B and 6; 0 `named`).
