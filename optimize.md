Item 4 (Core Web Vitals / bundle) — how we'd do it
Your main JS chunk is ~1.9 MB (600 KB gzipped). LCP/INP are ranking factors, so trimming it helps. The approach, roughly in order of payoff:

Measure first — run Lighthouse (or PageSpeed Insights on the deployed URL) to get real LCP/CLS/INP numbers, and npx vite-bundle-visualizer to see what's actually big. No point optimizing blind.
Lazy-load heavy, non-critical deps — pdfjs-dist + pdf-lib (bathymetry PDFs) and likely maplibre-gl plugins are prime candidates for dynamic import() so they're not in the initial bundle. I already code-split chart.js out via the gauge tab; same technique.
manualChunks in vite.config — split vendor libs (maplibre, pmtiles, pdf) into separate chunks so the app shell loads first and the browser caches vendors across deploys.
Defer the map init until after first paint if possible, and make sure the inline splash → LCP element is fast (it already is).
Preload the critical font/tiles, ensure images use loading="lazy".
It's a measure-then-cut loop rather than a single change, and it risks touching load-bearing map code — so I'd want to do it as its own focused pass with Lighthouse numbers before/after, not bundled with the SEO work.

Want me to run the bundle visualizer + a Lighthouse pass and come back with a concrete, prioritized list of what to split (and do the safe dynamic-import wins)? Or leave item 4 for now — the content fixes in 1–3 are the bigger SEO lever anyway.