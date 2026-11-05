# T411 — The highlighted part still zigzags after stepping between parts (water map, phone)

Status: OPEN — owner 2026-11-05 on 0.6.0 (About says 0.6.0), so the T399 fix (adfbe330e) is on the phone and the
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
