# T203 — app UI: jumpy "Flows into" chip, search lands on the map, amber highlight

Status: PARTIAL

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

## Outcome (2026-10-25, items 1-3 built; 4-5 need release data)
- **1 (built)**: the "Flows into" chip is now the LAST control of the map's bar, below "Show regulations": the steps and
  that button never move when the chip appears (only at the mouth part). Test: `WaterMapView.test.tsx` (DOM order, and the
  button under the steps row with or without the chip).
- **2 (built)**: `opener.searched(item)` ALWAYS opens the water's Map, the whole water shown (route `openWater` /
  frame `whole`, only on a Map view, dropped by a part step or a view change; `useWaterMap(..., wholeFirst)`), from
  Conditions or Regs alike. Tests: `openWater.test.ts`, `WaterMapView.test.tsx`, `Search.shell.test.tsx` (updated).
- **3 (built)**: the water Map's selected part ("dim" cue) now also carries a half-width halo in `color.highlight`
  (`PART_HALO_PX` 1.25 px a side, no width floor), so it reads on amber. Tests: `parity.test.ts` (casing/ring shown by
  `part`), `tools/water-status-cvd.test.ts` (halo vs every status incl. amber, all CVD models; no-access grey is dotted: 7).
  Not checked by eye on a phone.
- **4, 5 (NOT built)**: the app cannot do them from the release. Diagnosis (bundle.sqlite, Kanaka ord 455): the part graph
  makes ONE `part_mark` per PAIR of parts (`topology.py`, "middle of their nearest points"), but part 2 (below the bridge +
  above the falls) meets part 1 at TWO places; the bundle holds one mark, "Cliff Park Falls", and none for the bridge.
  Chilliwack works because its parts are a chain, each pair meeting once. The release carries no section adjacency
  (`section_span` / `section_touch` are not in it), and sid order is not adjacency (Chilliwack's part sids interleave with
  other parts'), so stretches cannot be derived app-side. Needed first (pipeline, a release rebuild): one mark per
  meeting point, and a per-part stretch list (`part_stretch`: sids grouped by adjacency); then the app lights one stretch
  at a time and labels each stretch's ends. Follow-up: T203b (not yet filed).

## Follow-up (2026-10-25)
`app/tools/request-walk.mjs` still waits for `regs-header` after a search result (search now lands on the Map) and its
unnamed-stream tap opens "" — update the walk for the T203 landing, then re-pin it on the split release
(`node app/tools/request-walk.mjs --url http://localhost:39218 --pin`; T182). Until then `release-budget`'s
"live release is split, pin is not" check fails — the only red in `pnpm -C app check`.
