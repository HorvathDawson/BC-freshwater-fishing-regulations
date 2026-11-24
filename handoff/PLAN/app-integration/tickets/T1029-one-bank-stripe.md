# T1029 — One-bank DFO places: the one-side stripe on the Salmon map (T754 option B, the rest)

Status: OPEN — after 0.9.0; pictures for the owner's Q4–Q6 in the DRAFT prototype PR (branch `feat/T1029-one-bank-stripe`, lane L10 0.9.1, not to be merged); needs his answers.

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
  the bank place's own salmon day). The day is Aug 15 2026: the Kitimat bank place is "No fishing for salmon Apr 1 to
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
