# T621 — The map's web and native halves agree

Status: IN REVIEW (d59ea56 G4, 1ed2bf7 G19, 9f3052f G15, 7d3f88a gate; G5 and G21 NOT DONE, below)

Source: T608 part B (owner 2026-11-10: "audit the app web vs native more"),
`handoff/PLAN/app-integration/T608B-findings.md`: 28 findings G1–G28, each with file:line of both copies, the state
that shows it, and the fix. Rows marked P there are plausible, not fully traced: confirm before fixing, close with a
reason if not real.

## Work
Findings whose files are under `app/packages/map`: G4 (wrong answer, P), G5, G15, G19, G21 and any other G row there.
Gate: a test that both map halves (`Map.web.tsx`, `Map.native.tsx`) honour every `MapProps` key, or list the key as
web-only / native-only by design with the reason.

## Acceptance
- Each finding taken here no longer reproduces and has a test, or is closed with a reason; the gate named above is in.

Depends: —. Size: M. Owner: looks at the shots.

## Result (2026-11-10)
- G4 DONE (d59ea56): a press asks 12 px; only when the box holds two waters are the 2 px and 6 px rings asked, nearest wins
  (`pickSection`, both adapters). Not proven that the engines ordered hits differently; this makes it not matter.
- G19 DONE (1ed2bf7): a tap on a titled pin shows its words in a card on the phone (`titledPin` shared with the web popup);
  the phone's pin key now carries the title.
- G15 DONE (9f3052f): `zoomControls` puts + / - on a bare phone map (`zoomStep`), top-right, under `insetTop`.
- Gate DONE (7d3f88a, `map-props-gate.test.ts`): web-only by design `onRegion`, `onImageryError`.
- G5 NOT DONE: the route overlay reads its lines with a JS pmtiles range fetch (`atlasTrace`); a phone's atlas is the
  installed archive, which that fetch cannot read. Needs a native tile reader (or the water-copy route through
  `querySourceFeatures`, which only sees loaded tiles), checked on a device. Listed in the gate as an open gap.
- G21 NOT DONE: the web's `within` filter carries the 61 KB mask into 12 label layers (about 0.7 MB more style on every
  style swap) and an unsupported `within` in the patched native build would fail the whole style. Needs an emulator look first.
- Not exercised on a device: the G15 buttons and the G19 card (unit and render tests over the shared code only).
