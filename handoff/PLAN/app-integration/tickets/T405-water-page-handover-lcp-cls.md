# T405 — Water pages fail the Lighthouse gate when the app takes over the prerendered pane

Status: OPEN.

Source: T196 gate on staging 2026-11-05 (release r-5a5999c94588, staging version 965c01af, lighthouse 12.8.2 mobile,
simulated 4G). Hubs and About score Performance 100 / A11y 100 / LCP 1.3-1.4 s / CLS 0. `/water/vedder-river`:
**Performance 45, A11y 97, LCP 38.9 s, CLS 0.154, TBT 750 ms.**
- LCP element is a span the APP renders ("only if you fish for white sturgeon…"), 99 % render delay: the prerendered pane
  (T269) is replaced by the app's own pane once the bundle has run and read the release, and the replacement's text
  becomes the new largest paint. The app bundle costs 3.7 s of main thread under the 4x CPU throttle.
- CLS 0.154: the pane container (0.052) and the `keep-tab` block (most of the rest) move at the hand-over.
- A11y: `aria-dialog-name`, `label-content-name-mismatch` (app components, same on the tapped pane).
## Work
1. Make the hand-over keep the prerendered nodes' boxes: the app's first render of the pane has the same size and place
   as the prerendered one (no shift), and either hydrates the prerendered DOM or takes over without painting a larger
   text node than the prerender already painted.
2. Fix the two A11y findings in the pane components.
3. Re-run the T196 gate on 3 water pages (command in handoff/PLAN/seo/MEASURE.md, against staging).
## Acceptance
Lighthouse mobile on 3 water pages: Performance >= 95 (or the owner lowers the water-page target), LCP <= 1.8 s,
CLS <= 0.05, A11y >= 95; the T269 page-vs-tap pixel pairs still match.

Depends: T269 (water pages are the app). Size: M. Owner: no (unless the target moves). Default: keep the gate's
targets; until fixed the water pages still serve full prerendered content to crawlers (no JS needed), so indexing is
not blocked — only the Core Web Vitals score.
