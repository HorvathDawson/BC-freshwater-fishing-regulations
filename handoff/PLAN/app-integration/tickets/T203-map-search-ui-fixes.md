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

## Done when
Each fixed with a test (layout-shift / tap target, search → map route, highlight contrast check on amber + cvd) and
owner phone check.
