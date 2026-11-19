# T875 — The water Map tab's ▼ / ▲ steps stay put, part to part and water to water (phone and desktop)

Status: PARTIAL — fixed by 1c0911f91 + 93efeda60 (review fixes) (lane app/t875-parts-steady, off app/integrate-081-web); left (Claude): the merge into
app/integrate-081-web.

Owner 2026-11-18 ~20:33, verbatim: "The ui shifts here when moving upstream in desktop too".

His two phone shots (Android, dark), the water page's Map tab: the Vedder River, "PART 1 OF 1", "Lowest part" and
"Upstream into Chilliwack River"; after pressing Upstream into Chilliwack River, a "Show all 4 parts" row appears ABOVE
"Map: rules for Thu 19 Nov / PART 1 OF 4 · CHILLIWACK RIVER" and the steps move down under his finger. Desktop too.

## Cause (measured, `app/tools/water-map-steps-layout.test.ts` on the unfixed code)

- **Desktop (the T752 split):** the text column hangs from the top, and above the steps sat the parts row ("Show all N
  parts", only on a water of many parts), the day, the where line and the name. Vedder → Chilliwack moved the steps
  46 px down (244 → 290 at 1280): the parts row.
- **Phone:** the bar stands on the foot of the screen, so on a fresh open the steps did not move; but a step into
  another water keeps the bar's height (T731 `useBarFloor`), and the bar's content hung from its top, so the floor's
  spare room went UNDER the content: the Vedder reached from the Chilliwack drew its steps 46 px higher (568 vs 614 at
  390), and ▲ back into the Chilliwack dropped them 46 px (the owner's shots). The map's foot also moved 46 px between a
  one-part and a many-part water (the map resized).
- The name was already held to two lines (T214) and the exit's place held (T214); a one-line step pair was 1 px shorter
  than a wrapping one.

## Fix

The steps are measured from the edge the bar is anchored to, with only fixed-height rows between them and it:
- **Desktop:** the steps head the text column (the coordinator's preferred option: the steps above the variable text).
  Everything whose height a part decides — the parts row, the where line, the name, an open parts list — is below them.
- **Phone:** the steps keep their place above "Show regulations" and the exit's place (both fixed), and the bar's
  content stands on its foot (`justifyContent: "flex-end"`), so the T731 floor's room goes above it. A water of one part
  holds the parts row's place with a quiet line, "This river is one part" (`WATER_MAP_WORDS.partsOne`, composed in core),
  so the bar — and the map above it — keep their size from water to water.
- ▼ / ▲ are always two lines tall at the reader's text size (`stepHeight`, 53 px at 1×; their words capped at 2×).

Independent review (2026-11-18), fixed in 93efeda60:
1. (medium) On the phone the exit's place was held only on a water WITH an exit (T214 `exitHeld`), so a step into a
   water with none (the Cowichan, a headwater) dropped the steps 52 px. It is now held on every water on the phone.
2. (low) The step height assumed 1× text: at 150 % a one-line pair was shorter than a wrapping one. It now follows the
   text size (`useCappedScale`); the exit line is capped at 2× so one line always fits its 44 px place.
Known, not fixed: at 200 % text a name or where line that wraps takes room from the map on the phone (above the steps,
which stay put).

Why not the steps directly under the map on the phone too: there the map takes whatever the bar leaves, so the map's
foot moves whenever the bar's height does; steps at the bar's top would move with every row that changes below them
(an open parts list, a water with no exit). On the phone the steady edge is the foot of the screen, so the steps keep
their low place and every row under them is fixed. The desktop's steady edge is the top of the column, so there they
lead.

Test bed: the `water-nav` scenario's testIDs are unchanged (`water-map-step-up`, `water-map-parts-toggle`; the one-part
line is `water-map-parts-one`, the step row `water-map-steps`).

## Verified

- `app/tools/water-map-steps-layout.test.ts` (new): the real Shell on the fixture at 390 and 1280, laid out in headless
  Chrome; the Vedder (cut to one part, as the live release has it), the Chilliwack's longest-named part (two lines, cut)
  and its shortest and a middle part, the Cowichan (no exit), each at 1×, 150 % and 200 % text, and on the phone the
  Vedder reached by a step (floored at the Chilliwack's height, and at a taller floor). The steps' top is one pixel in
  every case at each width and text size (1×: 390 → 613, 1280 → 156), and on the phone at 1× the map's foot too. It
  fails 7 ways on the unfixed code and 6 ways on 1c0911f91.
- Pictures (scratchpad of the 2026-11-18 app chat): `t875-390.png`, `t875-1280.png` (before / after, a red line at the
  first water's steps).
