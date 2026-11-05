# T400 — Camera moves feel choppy on the phone (part steps, open a water, Whole river)

Status: PARTIAL 2026-11-04 (owner, 0.5.0 Play internal).

**Cause (main):** the binding's `Camera.fitBounds` defaults to `easing: "fly"` (`@maplibre/maplibre-react-native`
`Camera.tsx`: `fitBounds … easing = "fly"`); every framing went through `fitBounds` with 450 ms — a fly arc (out, then
back in) squeezed into 450 ms, through zooms whose tiles were not loaded. **Also:** the water copy's ceiling was a
style prop on the phone, so the first settle after a move swapped the whole style (T399 removed it).

**Done:** `adapters/native.ts` passes `easing: "ease"` to `fitBounds` and `easeTo` (the web eases already). Test in
`adapter-contract.render.test.tsx`.

**Left (measure on the test bed, then decide):** a search arrival with `refine: true` (BrowseHost) still re-fits in up to
a few sequential rounds after the first move settles (`refineFit`), and the copy's `probeZoom` steps in one zoom per
settle — each is its own camera move. Option: measure the water box first and move once. Owner feel check on the APK.
