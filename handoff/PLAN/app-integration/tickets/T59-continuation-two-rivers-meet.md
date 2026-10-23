# T59 — One river system where two rivers meet (Atnarko/Bella Coola); Fraser exit into Annacis Channel

Status: DONE —code) — c5b1e3d0 (pipeline), app part 87ba267c, 7e3cd448 (already on the base); reviewed (counts reproduced: 18 rows, +1 Atnarko → Bella Coola, 33 confluence candidates, 469 exit changes / 234 none; Q1 note corrected). Left: `python -m pipeline.deliver` then
`python -m pipeline.deliver release` (merger; no atlas / reach / tiles rerun); after it the two `needs_bundle` pins
below pass. Owner review of the exit list (Q1) and the fallback (Q2) below, both with defaults.

Owner 2026-10-22: "think of things like this as one system; bella coola atnarko is another example."

- The app already treats a `water_continue` row as one system (cross replaces the back step, 87ba267c). Atnarko →
  Bella Coola has no such row: the Bella Coola starts where the Atnarko and Talchako meet, and `continuation.py`
  refuses a two-rivers-meet point (the Somass/Thompson rule).
- Fix in the pipeline: where two rivers meet, pick the one a synopsis entry names together with the receiving river
  ("Atnarko/Bella Coola Rivers", "Chilliwack/Vedder Rivers") as the continuation, still requiring `item_down` and the
  stub gate at the receiving river's top. The other stays a tributary. Fallback only when length/order agree.
- Defect found on the way: Fraser River's `water_exit` points into Annacis Channel. Find why and fix.

Acceptance: water_continue gains Atnarko → Bella Coola (and no false pairs: list every new row for review);
Fraser exit is the sea/estuary, not a side channel. Needs deliver.

## Built (c5b1e3d0)

**Continuation** (`pipeline/deliver/bundle/continuation.py`). `_goes_on` now answers YES / CONFLUENCE / NO.
CONFLUENCE = every gate holds (item_down, A's main line begins there, B's top piece, STUB_M, the FWA-code reversal
check) except that another inflow joins within `CONFLUENCE_M`, or B's own side channel rejoins B's top piece farther
than `OWN_M` (1000 m) below A's mouth. A CONFLUENCE link is kept only when one water row's heading names A and B as one
(`heading_pairs`: the heading's name split on "/", each segment's which-water words equal to the registry name's, both
in the entry's `matched`), and no other river meeting there is paired with B (`settle`). The pairing is read in
`write`, once `entry` exists (`book_pairs`); `links` (graph loaded) returns the candidates with the flag.
- Why OWN_M: the Bella Coola's top piece is 14 km long (48 628 → 62 600 m) and three of its own braid pieces rejoin it
  7 km and more below the Atnarko; the old rule ("any piece of B lower down the top piece") refused it outright. Within
  1000 m it is still "B goes on as itself" (the Cottonwood, 448 m), paired or not; farther, only the book decides — so
  no unpaired link changes.
- Book pairings in the live bundle: exactly two — Atnarko/Bella Coola (r5…atnarko_bella_coola…) and Chilliwack/Vedder.
- **Every new `water_continue` row (live bundle + graph, measured 2026-10-22): ONE —
  Atnarko River (gnis:17209) part 3 → Bella Coola River (gnis:11611) part 0.** Nothing removed; 18 rows. The 32 other
  confluence candidates (the Sproat and Stamp → Somass, Puntledge and Tsolum → Courtenay, Fontas → Fort Nelson,
  Poquette → Quesnel, Nakina → Taku, Nahlin and Sheslay → Inklin, 13 creeks → Brunette River, the Talchako → Bella
  Coola, …) stay refused: no heading pairs them.

**Fraser exit** (`pipeline/deliver/bundle/topology.py`). Why: `item_down` follows EVERY edge out of EVERY section, so
the Fraser "flows into" 19 waters — 18 side channels/sloughs that leave and rejoin it, and Moose Lake — and the exit
picked the one nearest its mouth part: Annacis Channel. The same flaw named a creek's own HEADWATER lake as its exit
whenever the creek's one part touched both (Burman River → Burman Lake, Nahatlatch River → Frances Lake). Rule now
(`exit_receivers`): a stream's exit is never a water that itself flows into it — a stream whose mouth (lowest piece of
its main line, walked down the graph: `mouth_downs`, computed in build.py while the graph is loaded) reaches it,
directly or down a chain (the two Jeperson Side Channels), or a lake whose `item_down` names it. A water's own mouth
reaching a candidate keeps it whatever the candidate says back. Lakes keep every receiver (a creek through a lake is
both inflow and outlet). Nothing left → no `water_exit` row (as the Bella Coola already had).
- **469 exits change**, every one listed in `handoff/PLAN/app-data/t59/exit-changes.tsv` (old receiver, new): 234 lose
  their exit (coastal or out-of-province streams that named their own headwater lake: Fraser, Squamish, Campbell,
  Capilano, Kettle → US, Peace → Alberta, Stikine → Alaska …); 235 move to the real receiver (Nahatlatch → Fraser,
  Penticton Creek → Okanagan Lake, North Thompson → Thompson, Crooked River → McLeod Lake, Pack River → Williston …).
  Each change drops a receiver that provably flows into the water, so a remaining pick is the old nearest-part rule.

## Tests
- `pipeline/tests/test_water_continue.py` 22 + 1 `needs_bundle`; `pipeline/tests/test_part_geo.py` 9 + 4
  `needs_bundle`. Against the live bundle: 33 pass, 2 fail by design until deliver reruns —
  `test_the_chilliwack_continues_as_the_vedder_in_the_bundle` (expects the Atnarko row, 18 rows) and
  `test_the_fraser_reaches_the_sea_and_its_side_channels_flow_into_it` (Fraser none, Annacis → Fraser, Nahatlatch →
  Fraser, Atnarko → Bella Coola, Burman none).
- Mutation pins: accepting every confluence fails 5 tests; accepting a heading that pairs both rivers fails
  `test_at_a_confluence_…`; `exit_receivers` returning all candidates fails `test_a_water_never_exits_into_…`.
- App: no change (a `water_continue` row makes the Atnarko's ▼ "Downstream into Bella Coola River" and cross in place).

## Owner questions (defaults built)
- **Q1** — the 469 exit changes (`handoff/PLAN/app-data/t59/exit-changes.tsv`) ship with the deliver? Default: yes,
  they all drop a water that flows INTO the water; spot-check the list. (Review 2026-10-22, live bundle + graph:
  Noons Creek's drop is right — Dallas Creek, an added stream, has its mouth on Noons Creek and Noons reaches the sea;
  Alec Chief Creek does NOT change (it keeps Toozaza Creek, not in the list) — the earlier note was wrong. Of the 234
  "none" rows, 205 mouths end on a piece with no edge out (the sea, or the atlas edge: Lightning Creek gnis:3526 runs
  out of BC at measure 19 274 toward Ross Lake), 29 leave through a braid/sea piece first; the "(none: …)" label in the
  list means "no named water below the mouth".)
- **Q2** — the fallback "where no heading pairs them, the river whose FWA code joins nearest the top, only when its
  upstream length / Strahler order is also larger": not built. Default: no; the book pairing covers both owner examples
  and cannot invent a pair. Building it needs a review list like the one above.
