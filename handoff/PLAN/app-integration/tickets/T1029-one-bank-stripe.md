# T1029 — One-bank DFO places: the one-side stripe on the Salmon map (T754 option B, the rest)

Status: OPEN — owner answered Q1/Q4–Q6 2026-11-24; v2 looks (A–D) shot for his pick (lane L18, 0.9.1), default built = B; Q5's binding change measured + proposed, not built (needs geometry in the reach step); DRAFT PR, not merged before his pick.

Source: T754 (owner approved option B 2026-11-15 round 4; question 2 answered 2026-11-22 23:45, "Other bank: the water's
other rules"). T754 built the words and the verdict (5953ef057, eeb19b4f3): a placed one-bank place stands beside the water's other
places (`u`, `p[lid].side`), the stretch paints by the other bank's answer and the card shows the one-side caveat. What
option B also had is left here.

## Work
1. Bundle: per section of a placed one-bank stretch, which side of the DRAWN line the bank is (+1 right / -1 left of the
   exported line's direction), computed against the exact exported geometry, never assumed (T754 §3 B, §4: the Kitimat's
   west bank is the right bank looking downstream; FWA lines most likely run upstream).
2. App map: two runtime layers over `stream` (`line-offset` ± half width + 1 px) in the bank place's own colour that day
   (closed for the Kitimat), opacity by feature-state. Pictures for the owner first (light + dark, Kitimat at the signs).
3. Owner questions still open from T754: Q4 (a stripe along the route line inside a polygon reach), Q5 (the Kitimat
   bank place binds 12 unnamed tributary sections from its "(including tributaries)" heading, T807: should a one-bank
   place take tributaries? Tributaries: no bank caveat (review fix of #61); owner may revisit), Q6 (the synopsis `kitimat_river.r1`
   `side: west` gets the stripe too?).

Depends: T754. Size: M. Owner: yes (looks, Q4–Q6).

Review of #61 (accepted as is): the folded salmon row on the Kitimat stretch says "open" with no hint of the closed west
bank until the caveat is opened; this ticket's stripe is the planned cue.

## Prototype and pictures for Q4–Q6 (lane L10, 0.9.1 overnight; draft PR, NOT to be merged)
What was built, just far enough to shoot honestly:
- `pipeline/deliver/bank_side.py` (+ `pipeline/tests/test_bank_side.py`, 3 mutations caught): per section, which side of
  the DRAWN line a compass bank is (+1 right / -1 left of the line as written in the tiles' `stream` layer file; no answer
  where the line runs within 30° of the word's axis or its pieces disagree). On the live 0.9.0 layer files:
  the Kitimat at the signs (section 1234098) is written d/s sign → u/s sign, i.e. UPSTREAM (NE): its west bank is LEFT
  of the drawn line (-1), 100% of its length (the right bank looking downstream, as T754 §4 said). The Nakina place's
  section 1218332: north bank = left (-1). The 12 tributary sections: 9 left, 2 right, 1 no answer (1009611 runs E–W).
- App (`paint.ts` `BANK_LAYERS` / `bankStripePaint`, two runtime layers in `runtime-style.ts` over `stream`): a 3 px stripe
  with a fixed `line-offset` (± half the stream's width + 2 px), opacity by feature-state `bk`, colour by feature-state
  `bks` = the token the line itself would wear (`salmon.closed`, `status.closed`). NOT wired: nothing in the app sets
  `bk`/`bks` yet — the picture harness sets them from the values above (in the product: the bundle's per-section side +
  the bank place's own salmon day). Shown in the Salmon and Standard colourings only (the adapter follows the mode): a
  visible layer of its own on the plain map is one more stream bucket per tile (`lab.test.ts`). The day is Aug 15 2026: the Kitimat bank place is "No fishing for salmon Apr 1 to
  Mar 31", the river at the signs is open (keep salmon) by its "downstream of Highway 37 bridge" place.
- Shots: web (dev Metro over the live 0.9.0 release and atlas, read-only), phone 412 px, light and dark, browse map.
  Not run on the emulator: MapLibre Native's `line-offset` is not checked (the spec validator takes the layers for both).

**Q1 — does the stripe read? (`T1029-shots/q1-kitimat-today-vs-stripe-{light,dark}.png`, z12 / z14 / z16)**
At z14–16 a thin closed-red edge on the west bank, the river green (open) beside it. At z12 the 3 px stripe is as wide
as the line and the stretch reads closed at a glance. Recommendation: the stripe from z13 only (a layer minzoom).

**Q4 — a river drawn as a polygon: where does the stripe go? (`q4-polygon-reach-{light,dark}.png`)**
The Kitimat and the Nakina bank stretches are LINES (no polygon reach at either), so this decides only future bank
places. Shown on the Kildala's wide reach 1846283 (an illustration; no bank place there), z14 and z15.
Options: (a) along the route line inside the polygon — the stripe sits mid-channel, and the route is written in 10
pieces, so it also runs along the cross-channel connector pieces (red ticks across the water); (b) along the polygon's
own bank edge — reads right, but needs new geometry (the outline split where the route meets it, computed in the
pipeline and shipped as its own layer: a tile build), the picture's edge is computed in the harness; (c) no stripe on a
polygon reach, the card's one-side caveat only. Recommendation: **(c)** now (no bank place is on a polygon), (b) if one
ever is.

**Q5 — the 12 unnamed tributary sections the Kitimat bank place binds from "(including tributaries)": stripe or not?
(`q5-tributaries-{light,dark}.png`, z13.4 and z15)**
With: each creek gets a "west bank" stripe, but "west" means nothing on a creek: it lands on whichever side faces west
section by section, so it flips at section joins, an east–west creek gets it on its north or south edge, and one
section (1009611) has no answer at all. Without (today, the review fix of #61): the creeks answer by their other
places; their card says "Unnamed stream · Region 6 — SALMON ✓ Open today", no bank caveat. Recommendation: **without**.

**Q6 — the synopsis `kitimat_river.r1` (`side: west`, "No Fishing on the west half of river between fishing boundary
signs") gets the stripe too? (`q6-synopsis-west-half-{light,dark}.png`, Standard map, z14 / z16)**
Without: the stretch is orange (own rules), its card carries "⚠ On one side of the channel only · No fishing". With:
the same stripe in the Standard map's closed colour on the same west edge (the same section, the same side).
Recommendation: **yes** — one model for both sources (T754 §5); it changes what `guide.py` says about `side` (never
coloured), so it is his call.

**Nakina (`nakina-{light,dark}.png`)** — it differs: the place is dormant, and on Aug 15 the whole Nakina is closed to
salmon, so a closed stripe beside a closed line shows nothing (the case T754's 11-22 note foresaw). No question.

## Owner answers (2026-11-24 morning, verbatim)
- The stripe itself (shown today vs stripe at z12/14/16 on the Kitimat at the hatchery signs): "Should make it more
  visible right now it isn't ?" — the pictured stripe is too faint; build it, more visible.
- Q4, a one-bank stretch drawn as a polygon: "Along the bank edge".
- Q5, the 12 side creeks the Kitimat bank place binds from "(including tributaries)": "No stripe on creeks and only
  tributaries on the one bank side get included".
- Q6, the rule book's own west-half rule (`kitimat_river.r1`, `side: west`): "Yes, stripe it too (Recommended)".

## v2 looks for his pick (lane L18, 0.9.1; `T1029-shots/v2-*`, phone 412 px, Salmon map, Aug 15, Kitimat at the signs)
Each image: today on top, the option below, z11 / z12 / z14 / z16, light and dark. The harness (scratch, not in the repo)
sets feature-state `bk` (side of the drawn line, -1 = left = the Kitimat's west bank), `bks` (the bank's answer token,
`salmon.closed`) and `bko` (the other bank's = the line's own answer, `salmon.open`) and adds the layers at runtime; every
expression is plain MapLibre style JSON.
- **A** `v2-a-*`: on the stretch the line is drawn WIDER (T = max(1.5 × the stream's width, 6 px)) and split down the
  middle: the west half the closed colour, the east half the line's own. Reads "one bank closed" from z14; at z11–12 the
  stretch reads mostly red (river closed).
- **B** `v2-b-*` (RECOMMENDED, built as the default): A + the app's own closed cue (`color.cue.closed` /
  `dash.cue.closed`, the paper dashes of `stream-closed`) down the middle of the closed half only. Reads "one bank closed"
  from z14 (z13 borderline); below that a red-and-white tick. The dashes are the colour-blind cue the closed line already
  has (T377), so it reads "closed" without the red.
- **C** `v2-c-*`: A + a boxed "West bank closed" label (the boundary marks' box, `point-marks.ts`) at the stretch's middle,
  anchored on the bank's side of the screen (the map never rotates, G1, so screen-left is west), from z12. Says it in
  words; MapLibre places `line-center` once per tile piece of the line, so a section crossing a tile edge gets two
  labels (z14 here).
- **D** `v2-d-*`: the river line untouched, a stripe OUTSIDE it on the bank's edge as wide as the line (≥ 4 px). Reads
  "one bank closed" from z14; at z11–12 a wider red-and-green line.
Low zooms: no option reads "one bank" below z13 — the stretch is 746 m, a few px at z11–12. Suggested: show the cue from
z13 (layer minzoom) and let the card's caveat carry it below; his call.

Native feasibility (checked against the pinned MapLibre Native, `pin.env` MLN_COMMIT 3a6b955,
`src/mln/style/layers/line_layer_properties.hpp`): `line-offset`, `line-width`, `line-gap-width`, `line-color`,
`line-opacity` are `DataDrivenPaintProperty`; `line-dasharray` is `CrossFadedPaintProperty` (zoom-constant per layer,
never data-driven) — so every dash is its own layer with a fixed dash and per-feature opacity (as `stream-closed` and the
issue stripe already are), and no `line-offset` reads feature-state (one fixed layer per side, the side picked by
opacity). A, B, D: line layers only. C: a symbol layer on the atlas `stream` source-layer whose filter (`["in", ["id"],
…]`) and text/anchor (`match` on `["id"]`) are set from the bundle at run time — layout and filters cannot read
feature-state on either engine — with the marks' box image the phone already registers (`Map.native.tsx` `markBoxName`).
Not run on a device or emulator (another lane has it).

## Q4 — along the bank edge (`v2-q4-bank-edge-*`, Kildala 1846283, an illustration: no bank place is on a polygon)
How the edge is found from the exported geometry (`lake` layer polygon + its `stream` route): the polygon's OUTER ring,
split where the route's two ends meet it; of the two arcs, the one lying on the bank's side of the drawn route (+1/-1 as
`bank_side`, measured per vertex: 100% / 0% here); minus where the ring touches another water polygon (none here); minus
the END CAPS (from each end, the outline's segments within 35° of square to the route: 11 m and 45 m here, the outline
crossing the channel). Written with the water on its right, so the band sits inside the water (`line-offset` +half its
width). Ambiguous (dotted ink, never striped): an ISLAND's outline (1 here, 374 m: a bank of a channel, not of the river's
west side), the end caps, a BRAID (a side channel drawn as its own line outside the polygon: not on the polygon's edge,
so not striped; whether it is "the west bank" is the place's words, not geometry), and a polygon in several pieces (the
largest only, here 1). In the product this is NEW GEOMETRY: the pipeline writes the edge as its own tile layer (the app
takes geometry from tiles only): a tile build.

## Q6 — the rule book's west half (`v2-q6-*`, Standard map, option B)
`kitimat_river.r1` is on the same section (1234098) and side (west = left of the drawn line): the west half in the
Standard closed colour + dashes, the other half the line's own colour (`status.own`).

## Q5 — only the tributaries on the bank's side (MEASURED; the rule PROPOSED, not built)
Measured on the exported `stream` layer (live 0.9.0 tiles, read-only), `q5` script in the lane's scratch: the Kitimat bank
place binds 13 sections: the stretch 360885824:4295 (746 m drawn, written d/s sign → u/s sign) and 12 tributary
sections. Exactly ONE line touches the stretch besides the river itself: 360639090:0 (1009609), joining on the LEFT of
the drawn line = the WEST bank, 214 m above the d/s sign (532 m below the u/s sign). Every other one joins through it:

| section (dense) | joins | side | fate |
|---|---|---|---|
| 360639090:0 (1009609) | the stretch, 214 m above the d/s sign | west | kept |
| 360800012:0 (1138262), 360725454:0 (1074714), 360823141:0 (1160627) | 1009609 | west (via) | kept |
| 360403626:0 (859361), 360590557:0 (975982), 360693120:0 (1049532) | 1160627 | west (via) | kept |
| 360306221:0 (805403), 360365166:0 (837614), 360396880:0 (855509) | 1049532 | west (via) | kept |
| 360639090:1519 (1009610), 360639090:1602 (1009611) | their own blue line 360639090, through sewage-lagoon routes (`under_lake`) to 1009609 | west (via) | kept |

**Kept 12, dropped 0, undetermined 0.** (Last night's "1 of 12 gets no answer", 1009611, was the side of the creek's OWN
line facing west — it runs E–W; by where it joins it has an answer.) Edge cases: a tributary joining inside the stretch
near a sign — none (the only one is 214 m in); one joining through a side channel or lake — 1009610/1009611 (through the
lagoons, same blue line); the Nakina bank place binds its own section only (`360874795:15041`, no tributaries): nothing
changes there.

What the reader sees: TODAY (`deliver.salmon`, review fix of #61) a bank place's tributary sections are not its own: the
12 creeks answer by their other places ("Unnamed stream · Region 6 — SALMON ✓ Open today", no caveat). His answer says
the west-side ones "get included": read literally, the 12 creeks would take the bank place's own rule, whole width, no
stripe and no caveat — "No fishing for salmon" all year, i.e. the 12 go from open to CLOSED. That is an answer change on
12 creeks (a data release); confirm it is what "included" means before it is built (owner question).

Proposed RULINGS E28 amendment (not yet in RULINGS.md; the owner's words): "(Owner 2026-11-24, T1029 Q5: 'No stripe on
creeks and only tributaries on the one bank side get included'.) A one-bank place whose words take tributaries ('including
tributaries') walks only the tributaries that JOIN ITS RIVER ON THAT BANK, measured where each joins against the river's
drawn line (a tributary of a tributary takes the side its own joins on); they take the place's rule across their whole
width, with no stripe and no bank caveat. A tributary whose side cannot be determined is reported with a typed reason
(`tributary_side_undetermined`) and stays as today; one on the other bank leaves the place (`tributary_other_bank`)."

Where it would go: `pipeline.atlas.reach.dfo_salmon.resolve_all`, after `build_reach` for a place with
`locations.bank_of(...)`; and `deliver.salmon`'s bank branch (the kept tributaries into `l`, not dropped). NOT built:
the reach step has no geometry (its graph is topology; `extent.py` / `build.py` use none), so the side test needs the
atlas's section lines read into the reach run (a new input to `build_reach`'s caller) — not a contained change, and for
today's only bank place with tributaries it moves nothing (12 kept, 0 dropped). `classify.py` is untouched (no
POLICY_VERSION bump).
