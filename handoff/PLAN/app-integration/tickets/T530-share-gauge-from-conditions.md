# T530 — Share a gauge from the Conditions tab (the button T487 promised)

Status: OPEN

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
