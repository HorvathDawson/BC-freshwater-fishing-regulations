# T761 — DFO "rest-of" places: their own bounds, the listed areas, and DFO's "areas AND TIMES"

Status: IN REVIEW (built, code d65d4e15d; data sweep marks 2787 to 2794 landed; release and the owner's re-ticks owed; leftovers T775)

Source: owner, 2026-11-15 22:12 and 22:14 (OWNER-TODO "2026-11-15 night — owner answers"): "this iss handled by rest of
option in extents" (the Skeena sockeye pair) and "all items wih same structure as that shoul duse rest of extent type
too"; his earlier "Rest-of extents: fix both (Recommended)" (round 4). Findings: `final-marks-second-look-2026-11-15.md`
(Region 1 rows on Somass / Stamp / Little Qualicum; Region 6 Skeena pair), `second-look-fixes-2026-11-15.md`, the
Skeena cascade review part 1 (scratch `cascade/part1.md`).

## What is wrong today
A DFO place with extent op `rest` is "the whole water less every other placed place on it"
(`pipeline/regs/dfo_salmon/entries.py` `to_reach_input`, lines ~949-994; `pipeline/atlas/reach/build.py` `_build_rest`):
1. It has no bounds of its own. "upstream of CNR bridge at Terrace to a point above the confluence with the Babine
   River, excluding the areas below" cannot be a rest-of: against the Skeena's 13 siblings it binds nothing.
2. An explicit sibling list is ignored for DFO; every other placed place on the water is subtracted.
3. A DORMANT (retired) sibling still takes its water out (Nanaimo x2, Somass x4, Stamp x8).
4. One sibling that cannot be drawn makes the whole rest `complement_unknown` (Nanaimo, until its cut is built).
5. "AREAS AND TIMES": a listed stretch is cut out for the whole year even when its rule holds a few months. Somass
   "Except in those areas and times listed below" binds 0 of 6 sections (the whole-river bait ban Jun 1-Aug 24 is a
   sibling), Stamp 0 of 10, Little Qualicum 5 of 8: the chinook and coho limits show nowhere on those stretches on the
   days the listed stretch has no rule of its own.

## What to build
- **Model.** A DFO `rest` extent may carry (a) its own bounds: the stretch or water it is the rest OF (the same end
  points a `between` / `upstream_of` / `downstream_of` / `whole` takes, with its item and tributary scope); (b) a list of
  the places it leaves out ("the areas below"), honoured; with no list it is every other LIVE place of the water inside
  the bounds. The mark model, `fixes_of_extents` / `EXPRESSIBLE` and the write-through carry both.
- **Reach.** The rest binds its bounds less the sections its LIVE, DRAWN listed places bind. A dormant place takes
  nothing. A listed place that does not bind is skipped and named in a diagnostic and on the card (never
  `complement_unknown` for the whole rest). `POLICY_VERSION` bump with its changelog line.
- **Chain (areas and times).** On the sections of its listed places inside its bounds, the rest-of place stands in the
  chain AFTER them: a listed stretch answers on the days it has a rule for that fish, the rest-of rule shows on the
  other days (`pipeline/deliver/salmon.py`: `covering`, `chain_order`, `rides_on_others`). It never sorts before a place
  it leaves out, whatever their sizes (a listed stretch with side streams can be larger than the rest).
- **Review app.** The editor's "rest of" form shows and saves the bounds and the listed places; the map shows the rest
  (bounds less the listed places); the place card says which places answer first. Route table regenerated if a route
  changes. Worked in a worktree, never under the owner's running app.
- **Audit** (`pipeline/regs/review/audit.py` section D): follows the new rules (dormant sibling no longer a finding).
- **Data, after the code lands** (marks saved unsure for the owner's re-tick, never edits of his lines): every
  leftover-worded place uses rest-of: the Skeena "excluding the areas below" place (bounds CNR bridge to gauge 08EB005;
  listed: the mainstem-only in-season openings, the Kispiox four-sign zone, the Kitwanga mouth stretch) and its dormant
  twin if it has its own extent; Somass / Stamp / Little Qualicum (back to rest-of tonight); Nanaimo; and a sweep of
  every place whose words say except / excluding / unless … below (Nitinat "downstream of Parker Creek, except in the
  areas and times described below", Dean "except for the canyon as listed below", …) with a table of what was changed
  and what was left and why. A place whose excepted areas are closed-area pieces (Thompson, Shuswap, Stave) is NOT
  this structure (AGENTS rule 68).

## Done when
- Tests on built data (anchors rule) pin: bounds; listed places honoured; a dormant listed place takes nothing; an
  undrawable one is skipped with its note; the chain puts the rest after its listed places on their sections and the
  seasonal case reads the rest's rule off-season (the Little Qualicum shape) and the listed rule in season; mutation
  checks for each.
- On the side build: Somass, Stamp, Little Qualicum, Nanaimo and the Skeena pair resolve as the words say; the sockeye
  answer between the CNR bridge and the gauge is the in-season openings on their days and closed otherwise, by
  structure, not by id order.
- Full pipeline CI tier green in the main checkout; `check-tickets` green; AGENTS rule 13 / 68 and `pipeline/docs/MAP.md`
  lines updated.

## Not in this ticket
Gear lines stacking across places and ordering by own-river size (cascade review findings, to be ticketed with the
cascade report); one-bank places (T754); the lost-closure banner (T759).
