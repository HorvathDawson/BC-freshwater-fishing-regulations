# T203 — app UI: jumpy "Flows into" chip, search lands on the map, amber highlight

Status: OPEN

Source: owner 2026-10-25, using the app.

## Problems
1. **"Flows into Vedder River ›" appears where the navigation button was** on the map, after the layout shifts; the
   tap meant for navigation lands on the chip and opens the tab instead. Fix the jump (reserve the space / no layout
   shift) so a control never moves under the finger; the chip must not take the navigation button's place.
2. **Search → a water with several sections is ambiguous** (e.g. Cayoosh Creek): it is not clear which section you
   land on. From search, ALWAYS open the Map tab with the whole water shown so the user picks the section there —
   whether search was opened from Conditions or Regs.
3. **Map highlight on amber is poor**: the selected-part highlight does not read on amber (no-access, T165) water. Pick
   a highlight that holds on every water colour incl. amber and cvd (cf. Q123 dim-the-others / halo shots in
   `app-integration/T151-shots/`).

4. **Kanaka Creek part 1 ("112 Avenue bridge → Cliff Park Falls"): the falls' label sits on the bridge.** The splits
   are right in the atlas (blk 356352191: bridge at 7,196.9 m, falls at 8,206.6 m, 1 km apart), so the end labels are
   placed wrong in the app (swapped ends, or labelled at the gauge 08MH076 boundary 20.7 m below the bridge — that
   sliver goes away with T179's gauge merge). Find which and fix the end-label placement. Owner: Chilliwack's labels
   looked right last checked — diff what Kanaka does differently (a reference that works).
5. **Non-adjacent stretches in one part** (Kanaka part 2 = below the bridge + above the falls): by design a part groups
   every stretch with the same rules ("Two stretches: …"). Owner 2026-10-25: the map treats non-adjacent stretches as
   SEPARATE stretches (each selectable on its own, its own label); only adjacent ones stay one stretch.

## Done when
Each fixed with a test (layout-shift / tap target, search → map route, highlight contrast check on amber + cvd) and
owner phone check.
