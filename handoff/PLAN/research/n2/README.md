# N2 evidence

Android emulator (AVD `n1_pixel`, Vulkan, release APK, airplane mode), release `r-6960fe8201e3` installed under
`files/release/` with tiles `d768d6e135e62b99.c9f9604f12bf` and the live feed pushed by adb. Emulator, not a phone.

| file | shows |
|---|---|
| `android-review-00-launch-offline-scalebar.jpg` | offline launch, status colours by feature-state, the metric scale bar bottom-right |
| `android-review-01-style-swaps.jpg` | Map → Conditions → Map: each a style swap; standing, then status, re-applied |
| `android-review-02-tap-sid-lights-its-reach.jpg` | Spots › Pick the water: the tapped section's id lights exactly the reach through the 24 dp box |
| `android-review-03-tap-opens-fraser-no-black-hole.jpg` | the same tap on the Map tab opens the Fraser; no black hole where the map was |
| `android-review-04-conditions-feed-standing.jpg` | Conditions with the feed: standing colours, gauge dots, the pill-less label with its halo |
| `android-review-05-conditions-tap-opens-section.jpg` | a Conditions tap on the Chilliwack opens its section |
| `android-review-06-two-finger-attempts.jpg` | injected two-finger gestures: no rotation, no pitch — and no pinch either, so not proof |
| `android-emu-0*.jpg` | the first N2 run (before review): binding scale bar top-left in miles |
| `web-n2-*.jpg`, `web-base-*.jpg` | the web walk (`web-walk/`), this branch and main on the same clicks |

Found on the device during review and fixed: `scaleBar={false}` left a black hole over the next screen when the map
unmounted (bisected; the binding's bar is now on and parked off screen); `setFeatureState` before the first full render
crashed with SIGSEGV in `MapRenderer::actor()` on a remounted map (now gated on `onDidFinishRenderingMapFully`).
