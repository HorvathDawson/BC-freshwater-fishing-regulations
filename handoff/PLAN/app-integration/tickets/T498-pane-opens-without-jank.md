# T498 — The rules / conditions pane opens in one piece

Status: IN REVIEW — b9103af33, 47299ccad (+ testbed c23affc58); owner sees it on the next APK. Owner 2026-11-08 (phone, 0.6.9). Part of T495 (smooth map).

Tapping a stream opens the pane in a janky, flickery way: the page shows, then its content loads in. Ideal: build the
content first, then show the finished pane.

## Do
- Reproduce on the emulator; record what renders frame by frame (sheet open vs content arrival vs layout jumps).
- Open the sheet only once its first content is ready (or with a stable skeleton of the final size), one load path for
  every way in (rule: one load path per screen). No silent fallbacks.

## Done (2026-11-08)
- **Cause** (emulator, testbed `pane-open`, cold map tap on Norrish Creek): 5 screens in 250 ms — map; frame with only
  the name; page without its Salmon card; with it; page pushed down by the access notice, and the view strip losing
  its Map view (the frame learnt it was solo after it was shown).
- **Fix**: `shell/paneReveal.ts` — a Regulations pane opened over the browse screen is built hidden at its final size
  while the browse screen stays frozen as it was, then shown in one frame once `WaterScreen` is drawn whole (Salmon card
  and access notices in) or after 1.2 s (its own waiting state). A map tap asks "solo?" before opening
  (`openWater.ts`). Wrappers `collapsable={false}` (Fabric re-parented the map: black/grey frames).
- **After**: map → finished pane, two screens (cold and warm). Shots: `shots/T498-before.png`, `shots/T498-after.png`.
- **Owner call**: held for the Regulations view only. Conditions / Map views carry a live map; building one under the
  held map crashed MapLibre's render thread once (SIGSEGV) before the collapsable fix. The Conditions-map open measured
  2 visible screens already. Extending the hold to them needs another emulator pass — say if wanted.
