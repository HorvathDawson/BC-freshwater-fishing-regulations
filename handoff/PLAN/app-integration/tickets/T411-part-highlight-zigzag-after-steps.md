# T411 — The highlighted part still zigzags after stepping between parts (water map, phone)

Status: DONE 2026-11-05 in code (3350c391d; 0.6.1 batch; not on a phone yet). Reproduced and fixed on the emulator (test bed
`part-steps`): FAIL before (Part 2 after the steps: sections 316504 and 316506 drawn by the copy AND the atlas,
copy_until 11 at camera z10.90), PASS after (none drawn twice). Owner look on the next APK.

Reported: owner 2026-11-05 on 0.6.0 (About says 0.6.0), so the T399 fix (adfbe330e) is on the phone and the
zigzag remains on the step path. The T399 emulator check only opened Part 1 directly; this path was never checked.

Source: owner, with a screenshot (`T411-shots/coquitlam-part2.png`): "The weird highlight in sections is still an
issue. To reproduce go to Coquitlam river then hit downstream to bottom then go upstream one time and you will see it."

**Repro:** open Coquitlam River → Map tab. Press Downstream until the lowest part (Part 1 of 3). Then press Upstream
once (Part 2 of 3, Lougheed Highway Bridge → Coquitlam Watershed boundary). The pink casing and the yellow line run on
two different paths along the whole part, like T399's zigzag.

**Likely lead (unverified):** T399 (adfbe330e) gave each copied section its own ceiling (`copy_until` = the tile zoom
its line was first kept at, `water-copy.ts`). Stepping goes through other zooms. The part's line may be kept from the
zoom where Part 1 was framed (shallower), then drawn at Part 2's deeper zoom. Or the highlight casing and the line come
from different sources (copy vs atlas) at that zoom. The emulator check in T399 only looked at Part 1 opened directly,
never after steps.

**Work:**
1. Reproduce on the emulator with these exact steps (test bed: add the step path to a scenario). Log each copied
   feature's `copy_until` and the camera zoom at Part 2.
2. Fix it so the casing and the line always come from the same geometry whatever path led to the part (one load path
   per screen: opened directly, by steps, or from a tap must look the same).
3. A test for the step path (`water-copy.test.ts`) and a test-bed scenario that fails before the fix.

Links: T399 (first fix), T175 (water copy), T345 (Coquitlam step paths).
Size: S–M.

## Done 2026-11-05

**Cause (measured, test bed `[copy]`/`[copydraw]` log: `T411-shots/before-copy-log.txt`).** Map opens on Part 2 at
z9.9: the copy keeps the river at z9 (copy_until 9). ▼ to Part 1 (z11.6): that pass sees Part 2's sections 316504 and
316506 with a WIDER box than z9's simplified line, so `take` replaced them and their ceiling rose to 11; their pieces
also came from tiles of TWO zooms in one pass (z11 and z7 — the source still held z7 tiles). ▲ to Part 2 (z10.9, tile
z10): the copy (copy_until 11) drew that z11 + z7 line under the atlas's own z10 line — the owner's straight second path
is the z7 piece. A settle over tiles the map already holds fires no full render (T348), so no pass ever corrected it
(on the emulator no full render came after any step at all). The ticket's lead was half right: not one ceiling, a
ceiling RAISED by a deeper pass, plus mixed-zoom pieces under one label.

**Fix (`water-copy.ts`, `Map.native.tsx`).** A kept section's ceiling only comes DOWN: the shallowest tile zoom any pass
found it at (a deeper pass may replace the geometry, never raise the ceiling), and also the shallowest zoom a piece
itself was cut at, read off its vertices' tile grid (`pieceGridZoom`, the atlas's 2,048 grid; believed only when chance
cannot explain it, n vertices × d zooms ≥ 4). So the copy draws a section only where the atlas does not, whatever path
led there. The phone's settle fallback (T348's `SETTLE_ASK_MS`) now runs the copy's pass too, so a step back to held
tiles still updates it.

**Tests.** `water-copy.test.ts` (T411: the step path Part 2 → Part 1 → Part 2, Part 1 first then up, a mixed-zoom pass,
the short-piece rule) fails on the old code, passes now. Test bed `part-steps` (`app/tools/testbed/scenarios/part-steps.mjs`,
`STEPS_VIA=tap` for the map-tap way in): FAIL before / PASS after. The probe lines are test-bed only (`camProbe.copy`).

**Shots.** `before-part2-after-steps.png` (two paths, as the owner's), `after-part2-after-steps.png` (one),
`before-after-part2.png` (side by side), `after-part1.png`; logs `before-copy-log.txt`, `after-copy-log.txt`.
