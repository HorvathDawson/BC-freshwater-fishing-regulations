# T857 — Revisit the Rust port of MapLibre Native (mln-rs) as the phones' map engine (future)

Status: OPEN — FUTURE (not before it has phone SDKs; app chat).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T857 | Revisit the Rust port of MapLibre Native (mln-rs) for the phones once it has phone SDKs (owner; future) | OPEN — FUTURE | app chat | owner 2026-11-18 |

Owner 2026-11-18 ~14:55, verbatim: "also should we use the rust port of map libre? seems really good ??", then on the answer
(not yet): "add as a future ticket please."

State on 2026-11-18 (github.com/louwers/mln-rs): created 2026-11-03 by Bart Louwers (a MapLibre Native maintainer), 10 stars,
in MapLibre's onboarding discussion (maplibre/maplibre#557). A Rust rewrite of the rendering core: desktop demo, browser demo
(WebGPU / WebGL2), a C API crate (`mln_c`) and Android / Apple build targets, but no Android or iOS map SDK (view, gestures,
offline, annotations) and no React Native binding like `@maplibre/maplibre-react-native`. It already took the owner's
feature-state fix (louwers/mln-rs#88, a port of maplibre-native#4919), which the app ships through the T420 patch.

Revisit when any of these holds: it becomes an official MapLibre project; it ships an Android/iOS map SDK; a React Native
binding exists. Then: compare against the patched MapLibre Native on the Intata / Fraser benches (frame times, memory,
APK size), check feature-state, offline packs and the style features we use. The web stays on MapLibre GL JS either way.
