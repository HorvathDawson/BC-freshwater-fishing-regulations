# T409 — Water pages: the next Lighthouse wins (later)

Status: OPEN — later (owner 2026-11-05: "any other easy lighthouse wins? could be a new ticket for later").

Source: T405 outcome on staging (fc215b52): water pages Perf 77–88, LCP 2.1–2.5 s, CLS 0.002, A11y 100; region hubs
97–100. What is left is the app's boot (TBT 350–520 ms) and bytes before first paint. Measure each item alone
(Lighthouse mobile, vedder / cowichan / skeena, 3 runs median) and keep only what moves the score.

## Candidates, cheapest first (none changes how the app behaves)
1. **Fonts:** 6 font files load before the first paint. Fewer weights on the prerendered page, subset to Latin,
   `font-display: swap`, preload only the one the largest text uses.
2. **Release verify in the browser's own code:** sha256 + ungzip run in JS today; WebCrypto `digest` and
   `DecompressionStream` do both off the main thread → less TBT. Same checks, same refusals (no silent fallback).
3. **Code splitting:** load the map / Conditions / rule-book code after the pane is interactive, not in the first
   bundle (the `later` chunk exists since T282; move more into it).
4. **sql.js start:** instantiate the WASM after the first paint (it is not needed to show the prerendered pane).
5. **Cache headers:** long immutable cache on hashed JS/CSS/fonts/thumbnails (repeat visits; Lighthouse "efficient
   cache policy").
6. **Third-party / injected scripts:** confirm nothing Cloudflare injects on prod (staging check found none after T196).

Not on the list (owner said no, or it changes behaviour): booting the app only at the first touch.

## Acceptance
Each item measured before/after; water pages ≥ 90 if reachable; hubs stay ≥ 95; T269 page-vs-tap pairs unchanged.

Links: T405, T196, T269, T282. Size: S–M per item. Owner: no.
