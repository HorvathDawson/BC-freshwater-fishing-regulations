# T400 — Camera moves feel choppy on the phone (part steps, open a water, Whole river)

Status: DONE 2026-11-05 — adfbe330e, d91f300f6 (owner feel check on the next APK)

**Cause (main):** the binding's `Camera.fitBounds` defaults to `easing: "fly"` (`@maplibre/maplibre-react-native`
`Camera.tsx`: `fitBounds … easing = "fly"`); every framing went through `fitBounds` with 450 ms — a fly arc (out, then
back in) squeezed into 450 ms, through zooms whose tiles were not loaded. **Also:** the water copy's ceiling was a
style prop on the phone, so the first settle after a move swapped the whole style (T399 removed it).

**Done:** `adapters/native.ts` passes `easing: "ease"` to `fitBounds` and `easeTo` (the web eases already). Test in
`adapter-contract.render.test.tsx`.

**Left (measure on the test bed, then decide):** a search arrival with `refine: true` (BrowseHost) still re-fits in up to
a few sequential rounds after the first move settles (`refineFit`), and the copy's `probeZoom` steps in one zoom per
settle — each is its own camera move. Option: measure the water box first and move once. Owner feel check on the APK.

## Measured (2026-11-05, test bed `feel`, run t312-c): the "extra re-fit rounds" do not happen
Each arrival: search → open the result → back; `[move]` lines tagged with the moving map's height.
| water | page open: camera moves | at rest after | back on the map |
|---|---|---|---|
| Fraser River | browse map fit ×2 (under the page), page map fit ×1 | 3.8 s | 0 moves |
| Coquitlam River | browse fit ×2, page fit ×1, copy probe ×1 | 4.5 s | 0 moves |
| Cultus Lake | browse fit ×2 | 0.5 s | 0 moves |
| Norrish Creek | browse fit ×2 | 0.5 s | 0 moves |
- **No refine round** on any of the four (refine is only for a water placed by evidence, not its outline), and the
  return to the map makes no move at all: it was framed under the page. The water copy's zoom probe stepped once
  (Coquitlam's page map). Every move eases (adfbe330e).
- Seen, not changed: the browse map is fitted twice to the same box under the opening page (the search focus, then the
  arrival key) — invisible, one wasted ease. Left as is.
