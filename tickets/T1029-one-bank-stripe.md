# T1029 — One-bank DFO places: the one-side stripe on the Salmon map (T754 option B, the rest)

Status: IN PROGRESS — draft PR #112, independent review done (output correct; 1 HIGH + 3 MED + LOW); its fix round is built (fb32ed0b9, 9c561e10a, 60ed7424e, 94ba4543c; the owner's layer pick A: 39eb40e28). Built: look B from z13 on the Salmon map (`bk`) and the book's west half on the Standard map (Q6, `bs` → `bb`, from the salmon head alone); the 12 creeks closed, computed from the extent by FWA's side. Gates: atlas parity IDENTICAL, reach "no binding changed", bundle: exactly the 12 creeks differ, salmon head 101,518 B gz (gate 110,000); after the CRS fix the side atlas re-stamped: Kitimat "R-" → "RL", its 12 creeks unchanged. Owner: layering answered "A: cues above the stripe" (built, 39eb40e28); the undetermined-side creek answered 2026-11-27 "keep it closed" + a warning (built: the reach run and the release checks warn on each, `feat/T1029-one-bank-creek-warning`). Left: Q4 (designed), the device walk, the 0.9.1 data release (its atlas rebuild re-stamps the banks) and an app build.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1029 | One-bank DFO places: the one-side stripe on the Salmon map (T754 option B, the rest; owner Q4–Q6) | IN PROGRESS — draft PR #112 (look B from z13; creeks closed by FWA side, computed from extents; Q6 the book's half on Standard, from the salmon head); review done, fix round built (CRS of the bank stamp, W46 refusal, tests); owner's picks built (layers A; undetermined creek kept closed + warned); Q4 (designed), device walk, 0.10.0 data release left | T754 | T754 (owner 2026-11-15 / 2026-11-22 / 2026-11-24) |

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
- **B** `v2-b-*` (RECOMMENDED, built as the draft default, 238235297: `paint.ts` `bankHalfPaint` / `bankCuePaint`, four
  runtime layers `stream-bank-{r,l}` + `stream-bank-cue-{r,l}`, `tools/bank-cue.test.ts`, 4 mutations caught): A + the app's own closed cue (`color.cue.closed` /
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

## Owner answers (2026-11-24 ~12:50, verbatim)
1. Which look: "B: split + closed dashes (Recommended)" — from zoom 13 up, the card carrying it below that.
2. What "included" means for the 12 side creeks: "Not sure, show me".

## Q5 show-me (`T1029-shots/v3-q5-creeks-show-me.png`, light, Salmon map, Aug 15) — text to paste to the owner
**What is printed.** DFO, Region 6, under the heading "Kitimat River (including tributaries)": "on west bank between
signs at Kitimat hatchery outfall." All, Apr 1 to Mar 31: No fishing for salmon. And "downstream of Highway #37 bridge."
Coho Apr 1 to Oct 31: 4 per day, only 2 over 50 cm; Chum and Pink Apr 1 to Aug 31: 2 per day; Chinook Aug 1 to Dec 31:
No fishing for chinook. The B.C. synopsis (p.51, `kitimat_river.r1`): "No Fishing on the west half of river between
fishing boundary signs near Kitimat Hatchery outfall".

**The 12 creeks.** One creek joins the closed stretch, on the west bank, 214 m above the downstream sign; the other 11
all drain into it (two through the sewage-lagoon ponds). All are unnamed streams.

**Reading 1 (today): the creeks keep today's answer.** On Aug 15 all 12 read SALMON Open: coho 4 a day (only 2 over
50 cm), pink 2 a day, chum 2 a day; chinook and sockeye closed — from DFO's "Kitimat River (including tributaries),
downstream of Highway #37 bridge" row, with the B.C. synopsis. No bank caveat. Nothing changes.

**Reading 2: the creeks closed like the west bank.** All 12 change: an angler on any of them reads SALMON Closed, "No
fishing for salmon", Apr 1 to Mar 31 (all year), from the west-bank row. Still no stripe and no bank caveat on a creek.

**My reading (not fact):** DFO's "(including tributaries)" is the heading over the whole Kitimat table; the west-bank
row is a small closure at the hatchery outfall on the river's own bank. It most plausibly means the river's west bank
there, not every creek draining into that 746 m of bank: Reading 1.

(How "today" was read: the web app over the live 0.9.0 release and atlas, each section tapped on Aug 15 and its card
read; all 12 creeks are one salmon chain, so they answer alike. One tap, creek 975982, landed beside its line; its chain
is the same.)

## Build of look B (lane L18, 0.9.1, after the owner's pick) — what is built, what is left
Built and tested (0cc340500; no data release needed for these):
- The app's four runtime layers (`stream-bank-{r,l}` halves, `stream-bank-cue-{r,l}` closed dashes, one fixed dash per
  layer as MapLibre Native needs) from zoom 13 up (`BANK_MINZOOM`, owner 2026-11-24); `tools/bank-cue.test.ts`
  (evaluates the runtime style; mutation-proved). Nothing sets `bk`/`bks`/`bko` yet, so no frame draws a cue.
- `pipeline/deliver/bank_side.py` production-grade: deterministic (sorted, file bytes only), every section asked for
  ends with a side (+1/-1) or a TYPED reason (`Why`: along_axis, pieces_disagree, no_line, written_twice,
  polygon_reach) — never a guess; `report()` gives what ships and what the build must print. On the live 0.9.0 layers:
  Kitimat 1234098 → -1 (west = left of the drawn line); Kildala 1846283 → `polygon_reach` (the report the brief asked
  for: a one-bank place landing on a polygon reach is seen, not skipped). `test_bank_side.py`, 5 mutations caught.

Left (the plan; needs a scratch bundle build, i.e. heavy.sh, held 2026-11-24 for the 0.9.0 production build):
1. Bundle (`deliver/bundle/build.py`, right after `_salmon.build`; the tiles dir is already read there for access and
   undrawn): for each `p[lid]` with `side`, its sections = those whose chain's `u` holds `lid`; `sides_from_layer(
   <tiles>/layers/stream.geojsonl, {sid: side}, <tiles>/layers/lake.geojsonl)`; ship `p[lid]["bk"] = {sid: ±1}` and
   print `report()`'s reasons (a polygon reach included). Additive: the 0.9.0 app's parser checks only `p[lid].side`
   (`release/0.9.0` `core/src/salmon.ts` L308) and ignores other keys; no `bk` → no cue (new app + old data). Size: the
   salmon head is 101,480 B gz in the 0.9.0 release against the 110,000 B gate; `bk` adds ~21 B per bank place (2 today).
2. Rule book `side` (Q6, "Yes, stripe it too"): the same per-section side for a synopsis rule with `side` (the regs
   bundle's beside rows, `f.side`), so the Standard/Trout colouring sets `bks = status.<the rule's own answer>`.
3. App: `packages/data` decodes `p[lid].bk`; the salmon colouring path (`map/src/colouring.ts`, `web/useChannels`,
   `native/useColouring`) sets per bank section `bk` = the sign, `bks = salmon.<category of the bank place's own chain
   that day>`, `bko = salmon.<the section's own category>`; cleared on day / mode change like `sp`. Creeks: none (a bank
   place's tributaries carry no `bk`; owner: "No stripe on creeks").
4. Style source (rule 28): every stream overlay today (`stream-closed`, the issue stripe, gear marks) is a runtime layer
   in `runtime-style.ts`; `layers.source.json`'s DSL has no construct for an offset half-line by feature-state. Putting
   look B there needs a style-builder extension — a question for the coordinator (keep the runtime-layer form, as
   `stream-closed`, or extend the builder).
5. Proof: scratch bundle (heavy), `app_reads` / `pack --check` / size gates, old app + new data and new app + old data,
   web shots of the REAL wired build `built-b-*` (light + dark, z12/13/14/16).

Q4 (polygon reach, "Along the bank edge"): DESIGNED, NOT BUILT — the edge rule and the Kildala picture above
(`v2-q4-bank-edge-*`); needs its own tile layer (a tile build). Until then `bank_side` reports `polygon_reach`.

Device walk (add to the device list): on the pinned MapLibre Native (pin.env 3a6b955), a one-bank stretch at z12
(nothing), z13, z14, z16 in Salmon and Standard: the two halves offset to their sides (`line-offset` from a zoom curve),
the closed dashes on the bank's half only (one fixed `line-dasharray` per layer), light + dark.

## Owner answer on the 12 creeks (2026-11-24 ~12:15 MST, verbatim)
"i think closed we need a clean way to have this calculated from extents so it fits into structure nicely"
= Reading 2: the creeks draining into the closed bank read salmon Closed from the west-bank row; no stripe, no bank
caveat on a creek ("No stripe on creeks and only tributaries on the one bank side get included" stands). It must be
COMPUTED from the extent in the normal structure, never a hand list and never a Kitimat special case.

### The clean way, measured (lane L18) — and what it costs (NOT built: needs atlas + reach + bundle runs, heavy hold)
What already is: the REACH run binds the 12 creeks to the bank place through the ordinary tributary walk (the place's
heading "(including tributaries)", T807) — `reaches/full/dfo_salmon.jsonl.gz` row
`6:kitimat-river:on-west-bank-between-signs-at-kiti` holds 13 sections. They are DROPPED only at delivery:
`deliver.salmon`'s one-bank branch keeps the river's own sections (beside, `u`) and discards the walk's (review of #61),
so the creeks answer by their other places (Open).

The bank side of a tributary needs NO geometry at reach time: FWA's `LEFT_RIGHT_TRIBUTARY` (the side a stream enters
its parent, looking downstream) is in the source streams layer and not yet carried into the atlas. Measured: 360639090
(the one creek joining the stretch) = RIGHT; the Kitimat flows SW at the signs, so right looking downstream = WEST —
the same answer `bank_side`'s geometry gives. (The others' values are relative to THEIR parents, not the Kitimat: they
take their root's side.)

Plan (each step general for every one-bank extent; a typed reason where it cannot say, never a guess):
1. Atlas: carry `LEFT_RIGHT_TRIBUTARY` per blue line into the graph (a field on the stored graph: an atlas rebuild,
   `build --atlas` beside the live one, then `build_parity`).
2. Reach (`dfo_salmon.resolve_all` → the walk's roots): a place with a bank keeps the tributary systems whose ROOT joins
   its own stem inside the extent on that bank; the bank word → "left/right looking downstream" from the stem stretch's
   flow direction (the atlas knows mouth→source per line, `cutting.line_coords_2d`); roots with `NONE`/unknown side →
   `tributary_side_undetermined`, kept as today and reported; the other bank → `tributary_other_bank`, out of the place.
   A reach rerun; `binding_diff` before/after must show the Kitimat place unchanged (all 12 on the west bank) and
   nothing else moving. A second case: none in the data today (the Nakina place has no tributaries; the book's
   `kitimat_river.r1` has `includes_tributaries: false`) — a synthetic test pins the other-bank and undetermined paths.
3. Deliver (`deliver.salmon` one-bank branch): the place's TRIBUTARY sections ride in their chains' `l` as any place
   (whole width: Closed for salmon all year, attributed to the west-bank row), no `bk`, no caveat; its own river's
   sections stay beside (`u`) with `bk`. A bundle build; the salmon diff shows exactly the 12 creeks going Open →
   Closed. RULINGS E28 amended with his two answers.
Cost: one atlas build beside the live one + parity, one reach run, one bundle/release build, then the 0.9.1 data
release. Step 3 alone (no side filter) would already close the 12 today, but would not honour "only tributaries on the
one bank side" for a future place — so it waits for 1–2.


## Built (lane L18, 2026-11-24 afternoon) — commits dc347175c, 8101dcd3d, 9eee990f3, a9b6a735b
- Atlas: every stream piece carries FWA's `LEFT_RIGHT_TRIBUTARY` (`StreamNode.left_right`; FWA's LEFT / RIGHT / NONE /
  UNKNOWN, refused otherwise) and `drawn_banks` (which side of its own line each compass word is, `common.bank_side`,
  one rule shared with the bundle, proved equal on 300 random lines).
- Reach (`reach.dfo_salmon.one_bank_tributaries`): a one-bank place keeps only the tributary systems whose root joins
  its stretch on that bank; the other bank → `tributary_other_bank` (out of the place), unreadable →
  `tributary_side_undetermined` (kept as before, reported); diagnostic `one_bank_tributaries`. Synthetic tests (other
  bank, a braid to both banks, FWA NONE, a stretch piece with no drawn answer, a pond on the way), 5 mutations caught.
- Delivery: the walk's tributaries ride their chains' `l` as any place (whole width, no caveat, no stripe); a chain is
  keyed by `speaks` too (the place's own card chain keeps its `n`). The bundle ships `p[lid].bk = {section: ±1}` for each
  section a bank place stands beside (`deliver.bank_side.stamp_payload`; a section beside two bank places stops the
  build; no side → reported, no cue).
- App: core decodes and checks `bk`; a `bank` colouring channel labels each such section "L|R" + the bank place's own
  chain (`bk`, fixed per release); the halves read the salmon day through the style (`slice` / `match`, no `in` scan);
  from z13; nothing on Standard yet. `tools/bank-cue.test.ts`, `salmonBank.test.ts` and three pinned app tests updated.

## The heavy gates (side builds in the lane's worktree, never the live data)
- Atlas (`pipeline.atlas.build --full`, 43 min): `build_parity` IDENTICAL (21,298 items, 4,612,858 sections, 11,393
  boundaries); `section_handles.txt` byte-identical; graph: 1,958,585 nodes / 1,548,408 edges, 0 nodes differing beyond
  the two new fields. 1,237,216 stream pieces stamped; 1,236,869 carry FWA's side. Kitimat stretch `drawn_banks` "R-"
  (west = left of the drawn line), creek 360639090:0 = RIGHT (west looking downstream).
- Reach (18 min): `binding_diff` "no binding changed"; all 369 DFO place rows identical; the new diagnostic: the Kitimat
  bank place keeps all 12 tributaries (west bank), none other-bank, none undetermined.
- Bundle (12 min): salmon per section vs the live bundle: exactly 12 sections differ — the 12 creeks, now with the
  west-bank place first in `l`: closed for every species on Aug 15 (was coho 4 / pink 2 / chum 2 open). The stretch
  unchanged (beside, `u`); `p` changed only by `bk` on the Kitimat (1234098: -1) and the Nakina (1218332: -1) places;
  the unmapped list identical once chains are resolved. Salmon head: +35 B gz (≈101,515 of the 110,000 gate).
- Web shots of the REAL wiring, `built-b-{light,dark}.png` (z12/13/14/16): the same app over the 0.9.0 release (no `bk`:
  draws as today — new app + old data) and over that release with the bundle's `p[lid].bk` in its salmon head (the only
  change; the creeks' closure arrives with the full data release, not shown). Old app + new data: the 0.9.0 parser checks
  only `p[lid].side` and never reads `bk`.

## RULINGS E28 amendment: in RULINGS.md E28 since 2026-11-28 (the owner's 2026-11-24 and 2026-11-27 words)

Proposed AGENTS rule 49 amendment, STILL PROPOSED and NOT APPLIED (E28 landed 2026-11-28 without it; AGENTS.md
unchanged): after "…not placed, it is an ordinary undrawn place, never widened." add
"Its TRIBUTARIES (a place whose words take them) are not beside: the ones joining its stretch on its bank ride their
chains' `l` like any place, across their whole width, with no `bk` and no caveat (T1029, owner 2026-11-24)."

## Q6 built (lane L18, 2026-11-24 evening) — commits 76b8ff39e, 1c2338823, 1ba481823
- Bundle: a synopsis rule with `side` ships its sections' side of the drawn line in the salmon head, `bs = {section: ±1}`
  (`deliver.bank_side`; today one: `kitimat_river.r1` → 1234098: -1). Core checks it (a non-empty map, sides ±1 only).
- App: the Standard map's `half` channel labels each such section `bb` = "L"/"R" (`bookHalves`); the profile paint draws
  that half `status.closed` + the closed cue, the other half the line's own colour, from z13 (look B, as Salmon).
- Cold start (1ba481823): the halves are read from the salmon HEAD alone (`WaterSource.salmonHead` = the loader's
  `salmon([])`, the head cached once and shared with a later Salmon read; `useBookHalves` → `MapProps.halves`, web and
  native channels). Before it, a fresh Standard map drew the half only after a visit to Salmon (the web reads the slices on
  the first Salmon press). Test: `useMapColouring.test.tsx` "the book's closed halves on a cold Standard map".
- Proof (`T1029-shots/built-q6-{light,dark}.png`, z13/14/16, Aug 15): the same app, a fresh browser profile per run, Salmon
  never opened; BEFORE over the 0.9.0 release, AFTER over it with the side bundle's salmon head, every `salmon-<r>` slice
  REFUSED at the network in AFTER (the web's background prefetch asks for them; none arrived): 1234098 wears `bb = L` and
  its west half draws closed. Note: the web's background prefetch (T253) fetches the salmon slices on a cold Standard map
  anyway; that is the cache warm-up, not a read for the map.
- Review of 1ba481823 (lane L18b): one path for web and native (BrowseHost → MapScreen → Map → both channel hooks); the
  head is decoded once per loader and through `withLiveSalmon` keeps `bs` (`...P`); `bookHalves` is cached per payload, so
  the map is handed the same object and nothing relabels per render; a failed read is reported by `useAsync`. No change.

## Review #112 (independent, 2026-11-24 night) and the fix round (lane L18b)
Output correct (the 12 creeks verified independently; nothing else moves). Findings and what was done:
- HIGH (fb32ed0b9): `stamp_drawn_banks` measured the atlas's EPSG:3005 METRES with the degrees rule (cos(lat) scaling).
  Re-stamping the side atlas (1,237,216 stream pieces, 53 s): north/south opposite on 20.0% and missing on 48.5% before
  the fix; east/west opposite 0.1%. Now every line is projected to EPSG:4326 in one batch, and both degree rules refuse a
  line in metres. Test through `stamp_drawn_banks` on 3005 lines (the Kitimat, a 59° N line whose east answer needs the
  per-degree scale); 4 mutations caught (no projection, reversed, unscaled, guard removed). Today's answers do not move:
  the Kitimat's word is "west" (the first character, unchanged "R"), the Nakina's north place has no tributaries.
  Re-proved on the re-stamped side graph: Kitimat 1234098 "R-" → "RL", the west-bank place keeps all 12 creeks (none
  other-bank, none undetermined) = the side reach run. NEEDS THE ATLAS RE-STAMPED: the 0.9.1 build rebuilds the atlas,
  which stamps with the fix; that build must show 1234098 `drawn_banks` "RL", the reach's `dfo_one_bank` in report.json
  `{places: 1, kept: 12, tributary_other_bank: 0, tributary_side_undetermined: 0}`, and the salmon diff = the 12 creeks.
- MED 3, W46 (9c561e10a, 94ba4543c: the check reads no node kind, T121): a graph.pkl without the bank fields read "" on every node and every tributary was kept silently:
  `require_bank_fields` now refuses it by name (`atlas_lacks_bank_fields`), only where a one-bank place has tributaries.
  The reach run prints every tributary kept with no side read (`!!   dfo <place> … KEPT with no side read`) and writes the
  tally to report.json (`dfo_one_bank`). Keeping one stays as built, a PROPOSAL (owner question 1 below). The CLI's print /
  report.json lines are not covered by a test (the tally function is).
- MED 4 (9c561e10a, 60ed7424e): the `resolve_all` wiring (`row["sections"] = sorted(kept)`) is tested through
  `resolve_all`; the phone's channel set is pinned to the web's (`one-colouring-engine.test.ts`; removing the phone's
  `bank` or `half` effect is now red).
- MED 2, a look: shown as owner question 2 (`review-cues-ab-{light,dark}.png`); he picked A, built (39eb40e28, below).
- LOW (9c561e10a, 60ed7424e): `keep = stem & bound`; `root_of` dropped; the bundle refuses a `bk` place whose own chain in
  `unmapped` is not exactly one (the app's decode refusal, now caught at build); the stream and lake layers are read ONCE
  for all one-bank places (was once per place; the book's half rules still read them once more: 2 passes, was 3).

### Owner questions from the review (for the coordinator to ask)
1. ANSWERED (owner 2026-11-27 ~23:55 MST, verbatim): "keep it closed (for trib rules i assume) but one thing to note we
   should add a thing that warns us if this is ever a thing". The ask: a tributary whose side cannot be read (FWA NONE /
   UNKNOWN, or joining on both banks), kept (closed) or left out? Reading (planning chat): stays closed as built, plus a
   build WARNING when one first appears. Built: `dfo_salmon.one_bank_unread_lines`, printed by the reach run and listed
   by the release checks (`release.one_bank_unread`; summary, `one_bank_unread.txt`, ledger; never a stop). No binding
   moves; today's reach run (2026-11-25): 1 place, 12 kept, 0 unread, so it is silent.
2. ANSWERED (owner 2026-11-25, shown `review-cues-ab-{light,dark}.png`, multiple choice, verbatim): "A: cues above the
   stripe". Built (39eb40e28): the closed cue (T377) and the issue stripe (T759) draw after the four bank layers, so a
   fully closed river reads closed across both halves without its colour; the gear edge (a wider line UNDER the stream)
   is drawn again over a bank stretch as two thin edges just outside it (`stream-gear-{fly,bait}-bank`,
   `line-gap-width` = the stretch's width, `paint.ts` `gearBankPaint`), on the edge's own sections and days. One style
   for web and native (`runtimeStyle` + the adapter contract). `tools/bank-cue.test.ts` pins the order and the edge
   (4 mutations caught, the bank layers last among them). Pictures of the real build: `T1029-shots/built-a-{light,dark}.png`
   (rows: before, A as shot, A built; Standard and Salmon on Aug 15, Salmon on Nov 15 when the whole river is closed;
   z14/z16). Not run on MapLibre Native (`line-gap-width` is data-driven there; the device walk).

## Left
Q4 polygon reaches: designed, not built (above). The device walk (above). The 0.9.1 data release carries `bk`, `bs` and
the 12 creeks' closure, from an atlas the fixed stamp re-stamps (above: what it must show); an app build carries the
drawing. Owner question 1 above: answered 2026-11-27 (kept closed, warned; built).

## Q4 — LEFT (coordinator 2026-11-24: do not build now; build when the first case appears)
Owner's answer: "Along the bank edge". The measured rule (`v2-q4-bank-edge-*`, Kildala 1846283): the polygon's OUTER
ring, split where the route's two ends meet it; the arc on the bank's side of the drawn route (+1/-1 as `bank_side`);
minus where it touches another water polygon; minus the END CAPS (from each end, segments within 35° of square to the
route); islands (inner rings) never striped; written with the water on its right so the band sits inside the water.
Cost: a new `bank_edge` tile layer (the tile export) + a tile build + one runtime layer in the app. No one-bank place sits
on a polygon reach in today's data; the build prints `polygon_reach` (a typed reason, visible in the bundle log) when one
appears — then build it.

## Owner

- [x] 2026-11-25 — A creek of a one-bank place whose side cannot be read: keep it closed (as built, a proposal) or leave it out? → "keep it closed (for trib rules i assume) but one thing to note we should add a thing that warns us if this is ever a thing" (2026-11-27)
- [x] 2026-11-25 — One-bank stretch hid the line's own cues → "A: cues above the stripe" (2026-11-25).
