# T530 — Share a gauge from the Conditions tab (the button T487 promised)

Status: IN REVIEW — phone look on the next APK

Owner 2026-11-09: "there is no button in the conditions tab to share the conditions gauge cards that we made before … make
sure it is cleanly done".

## Today
T487/T511 built the public gauge pages (`/gauge/<station>/<slug>`, `app/tools/seo/gauges.ts`) and their preview cards
(`/gauge/<station>/card.png`, drawn by the feeds worker). T487 decided "shared from the Conditions tab's gauge", but no
share control exists in the app (ConditionsPanel / GaugeTrace).

## Do
- One share control on the gauge in Conditions (the gauge card's header, where the station is named), same look and words
  as the water page's existing share (top bar). One shared helper for the link and the share sheet: no second copy.
- The link is the gauge page URL for the release's site origin (staging vs prod from config, never hard-coded); the slug is
  the one the site builds (one function in core, read by the SEO build and the app, so the two cannot drift). A station
  with no page (no water with a page) shows no button — and a test proves the app's list matches `/gauge/stations.json`'s
  rule.
- Native: the OS share sheet (title + URL; the card is the link's preview). Web: Web Share API, else copy the link with a
  "Link copied" confirmation. A failure is said and reported (no silent fallback, owner 11-03).
- Logic in a hook (`useGaugeShare`), not the component (AGENTS 25); the button is a `ui-native` component used by phone and
  desktop layouts.

## Done when
- The button is on every gauge that has a page, on phone and desktop, light and dark (shots).
- Tap → share sheet with the right URL on the emulator; the shared link's preview shows the gauge card (staging).
- Tests: URL/slug parity with the SEO build, no button without a page, failure is shown + reported. `pnpm check` exits 0.

## Done (2026-11-09)
- `@app/core` `gaugePage.ts`: `slugOf`, `gaugePath`, `gaugeUrl`, `gaugeHasPage` — the ONE slug/path rule; `tools/seo/gauges.ts` and `slug.ts` now import it.
- `@app/ui` `useGaugeShare` (+ `useSendLink`, the one press shared with the water header's `useShare`): sheet / `navigator.share` / "Link copied"; failures said and `reportFailure`d; dismissed silent.
- `ui-native` `GaugeShare` on the gauge card's title row (same icon + words style as the water header); the Shell hands the platform share down by `SharePortContext`. No button without a platform share, a water with a page, or a sourced gauge name.
- Origin is `SITE_ORIGIN` (one public site for every build, T404), like the water share: no per-environment host to drift.
- Tests: `ui/gaugeShare.test.ts`, `tools/seo/gauges.share.test.ts` (parity with the page list), `ui-native/GaugeShare.test.tsx`.
- Shots (web, real staging data, Vedder/Chilliwack at Vedder Crossing): `shots/T530-phone-light.png`, `-phone-dark`, `-desktop-light`, `-desktop-dark`.
- Left for the phone: share sheet on the emulator/APK and the preview card on a staging link.
