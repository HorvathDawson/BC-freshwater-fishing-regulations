# T770 — Salmon gear lines of a wider place vanish when a narrower place has a gear line that day

Status: OPEN (pipeline, `pipeline/deliver/salmon.py`; needs the owner's yes on the rule before it is built).

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
