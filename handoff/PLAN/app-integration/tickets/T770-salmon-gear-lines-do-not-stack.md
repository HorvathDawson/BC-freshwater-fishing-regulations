# T770 — Salmon gear lines of a wider place vanish when a narrower place has a gear line that day

Status: DONE — 4e0feaca9, 58debf8a7, b7449a66b, c20de86c1, abb01d6c2. A chain's salmon gear lines are every place's, narrowest first (pipeline, app, worker); eulachon is its own closure line (owner 2026-11-16).

Closed by the 2026-11-20 audit: the owner's yes recorded (question 13 "Show both (Recommended)"; eulachon "As a closure line (Recommended)"). Built in the pipeline (4e0feaca9, c20de86c1, abb01d6c2), the app (58debf8a7) and the worker (b7449a66b). Merged via 02aea5be3 and e389f9ce8. The app/worker `o` line was drawn under T796 (a6bbaf951, 58fa0593c). Shipped from the 0.8.0 data release on.

Source: second-look fix lane A, 2026-11-15 night (`handoff/PLAN/curation-review/second-look-fixes-2026-11-15.md`,
"New findings" 1).

## What is wrong
`gear_on` (salmon.py, line ~311) returns the gear lines of the NARROWEST place in the chain that has any gear line
that day, and stops. A whole-river gear rule that holds all year is hidden on the days a narrower stretch has a gear
line of its own. Example: Little Qualicum, Sep to Nov: the stretch's "fly fishing only" shows and the river's
all-year hook rule does not. Both hold on the water.

## What to build (after the owner's yes)
- Gear lines of every place in the chain that hold that day are shown, narrowest first, equal lines once.
- A line a narrower place replaces (same kind of gear rule, different value) is not shown twice: decide and write down
  what "replaces" means before coding (bait ban vs bait allowed; hook count) or show both and let the words stand.
- Diff every answer key's gear lines before and after; the diff is the review list.

## Owner question (on the morning list)
Should a river-wide gear rule keep showing on a stretch that has its own gear rule that day? Recommended: yes.

## Owner, 2026-11-16 (question dialog; verbatim in `handoff/PLAN/OWNER-TODO.md`, "Answers by batch")
Question 13: "Show both (Recommended)". Build it; the diff of every card's gear lines is his review list. The eulachon line (Region 6-wide reminder) must never be displaced by a bait rule.

## The rule, written before the code (lane/salmon-rules-1116, 2026-11-16)
- A chain's gear lines on a day are EVERY place's lines that hold that day, narrowest place first, each place's lines
  in its own order (a place reads its own lines as before: dated rows, else its "until further notice" ones).
- "Equal lines once": the same words (with their dates) printed by two places show once, at the narrowest place.
- "A narrower line replaces a wider one": ONLY when it is the same line. A different value of the same kind of gear
  rule (a bait ban beside a bait permission, one hook rule beside another) shows BOTH, narrowest first: both are DFO's
  printed rows and the angler must meet both (owner: "Show both"). Measured on the release r-46e4355a7277 chains with its typed rows: no two places of one
  chain set the same gear slot to different values today, so a structural "replace" would change nothing; keeping it
  by the words keeps the live feed's composition (the worker's location years, the app's `composeChain`) a plain
  concatenation with nothing to re-decide.
- A row about another fish ("No fishing for eulachon", Region 6 B(ii)) is not a gear rule: it is read apart from the
  gear rows of its place (a bait row of the same place never decides whether it holds), and like every line it is never
  displaced by a narrower place's line. The page-wide "Reminder: There is no fishing for eulachon in Region 6." is a
  region header note (`6:other:…`, kind `other`, never folded or dropped): `notes_for` / core `salmonNotes` already
  show it on every page whose chains are Region 6's, so it is carried at the page's width by the header, and the
  B(ii) row's line by every chain that holds B(ii).

## Lane result (lane/salmon-rules-1116, 2026-11-16)
- `deliver.salmon.place_gear` (one place's lines; another fish's row read apart from the gear rows) and `gear_on`
  (every place's, narrowest first, the same words once); `live.compose` merges every place's year the same way.
- Tests: `pipeline/tests/test_t770_gear_lines_stack.py` (5; mutations: the old first-place rule fails 4, reading the
  eulachon row together with the gear rows fails 1, `compose` keeping "first" fails the parity test).
- Review list (salmon step rebuilt from r-46e4355a7277's built inputs, before -> after; no line removed anywhere):
  13 chains, 3,745 sections. Eulachon line now shown: 9 chains, 3,606 sections (Kitsumkalum above and below the
  canyon signs, below the railway bridge, Kitsumkalum Lake and Redsand Lake: Jan 1 to Jun 15; Lakelse three places,
  Kloiya: all year). Hook line now shown: Little Qualicum, 3 chains, 8 sections, Sep 1 to Nov 30 ("single barbless
  hook less than 15mm" beside "Fly-fishing only"). Bait line now shown: Morice, signs below Gosnell Creek to Lamprey
  Creek, 131 sections, Sep 1 to 30 ("No natural bait allowed" beside "Fly-fishing only").
- App chat (live feed only; the release's `g` is already right offline): core `salmonFeed.ts` `composeChainOn` must
  merge, per group and day, EVERY place's gear list (`[...new Set(places.flatMap(gear list that day))]`, narrowest
  first; a new list through `intern(t.gears, …)`) instead of the first place's; the worker's `dfo/salmon.ts` `gearOn`
  (one place) must take `heldOn` of the gear rows and of the other-fish rows apart (`place_gear`). The saved pages'
  vectors do not move (`dfo_vectors --check` clean).

## 2026-11-16 16:23: owner asked "Eulachon is a bait or a limit?"
Answered (old main chat): neither, it is a closure carried in the gear / bait lines slot. Open for the end batch: show
and classify it as a closure (card and `answers_diff` grouping) instead of a gear line. The candidate's gear lines
differ on 3,745 sections (`answers_diff`); the read-only worker on the candidate's extra differences reconciles that
with "13 chains".

## Owner, 2026-11-16 19:09: eulachon as a closure
"As a closure line (Recommended)": eulachon shows as its own closure line (closed all year in Region 6), apart from
the gear and bait lines; data writes it as a closure, the app draws it.

## Owner, 2026-11-16 19:09 (verbatim in OWNER-TODO): eulachon "As a closure line (Recommended)"
Built (lane/salmon-hardcases-1116): a DFO row CLOSING another fish ("All | No fishing for eulachon", Region 6 B(ii))
is no gear line; it is the chain's closure line, Python's words "Eulachon closed all year: No fishing for eulachon"
(its dates when not all year). FIELD: chain `o` = runs of an index into payload `gears` (-1 none), absent on a chain
with none; `live.location_calendar` / `live.compose` carry it the same way (every place's line, narrowest first, the
same words once: never displaced). The Region 6 header reminder note is unchanged. Tests: `test_t770_gear_lines_stack.py`
(closure line whole, never displaced, `o` apart from `g` and the live composition), `test_t229_cascade_order.py`.
Vectors regenerated (the eulachon locator's line moves from `g` to `o`). FOR THE APP CHAT: draw `o` as the card's
closure line(s) apart from the gear and bait lines; the worker's `dfo/salmon.ts` takes another fish's closure out of
`gearOn` into an `o` year (twin of `place_closures`), and `composeChain` merges `o` like `g`.
Salmon step rebuilt on the scratch bundle (r-4179ef17d31a): 30 chains carry `o`, 15,664 sections (131 waters, the
Skeena / Kitsumkalum side) lose "No fishing for eulachon (Apr 1–Mar 31)" from the gear lines and gain the closure line.
