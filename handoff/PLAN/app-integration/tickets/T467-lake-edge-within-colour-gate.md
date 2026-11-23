# T467 — Lakes: a visible shoreline inside the colour-blind gate

Status: IN REVIEW — owner: OWNER-TODO "Check (you): T467 shaded shoreline as built" (shots T467-shots/); code 2a4633ecc, PR open.

T457's lighter fill (0.4 light / 0.75 dark) failed `tools/status-colours-distinct.test.ts`: closed vs own and no access
vs base were too close for colour-blind readers once washed out. 0.5 and 0.55 failed too. So the fill stayed solid,
and only the 1.5× shoreline shipped. In a colour mode that shoreline is the SAME colour as the fill (`follow`), so it
does not show.

## Work
1. Proposal that keeps the fill (and the gate) as it is: draw the shoreline as a SHADE of the fill colour (darker in
   light mode, lighter in dark; one token step) so the lake reads as outline + fill. Check the gate (lines vs fills),
   and that the shade never meets another status colour.
2. Or: a lighter fill only where the fill answers no question (Plain mode), with the outline carrying the colour.
3. Shots (light / dark, regs and Flow modes, Stave/Alouette like the owner's screenshot) to the owner; the look is the
   owner's call.
## Owner question, open (2026-11-06): should the colour-blind check apply only to the colour-blind theme?
Owner: "color blind checks only need to be on color blind mode tho?" Today `tools/status-colours-distinct.test.ts`
checks light, dark AND cvd, each as the WORST of normal vision and protan / deutan / tritan (Viénot, Machado). The 0.4
lake fill failed only under the CVD simulations (whether light/dark pass under normal vision alone is not yet measured).
- **For scoping it to the cvd theme:** that theme exists for exactly this; light/dark could be tuned for looks.
- **Against:** about 1 in 12 men have some red-green colour blindness, and many never switch themes. Closed vs open is the
  app's most important signal. Light/dark would still be checked under normal vision (that check is what caught the
  owner's own T47 complaint).
- **Claude's recommendation:** keep the CVD check on all themes for the status colours, and get the look from Work 1
  (the shoreline as a shade of the fill), which passes either way.
- **If the owner chooses the cvd-only scope:** light/dark are checked under normal vision only, the cvd theme keeps the
  full check (its own `opacity.lake` 0.9). Re-test the 0.4 / 0.75 fill and ship it if normal vision passes. The gate's
  header records the decision. That is a scope change by the owner, not a loosened guard.

Related: T457, T47 (status colours), T165, T158. Size: S.

## Default taken 2026-11-12 (not an owner acceptance)
The owner question (should the colour-blind check apply only to the colour-blind theme?) has no answer on record, so the
default stands: **the check stays on all themes** (Claude's recommendation above; nothing is loosened, the gate is
unchanged). It is on the owner's to-do page as a default taken: the owner acts only if they disagree. The ticket stays
OPEN for Work 1 (the shoreline as a shade of the fill: a look, with before/after shots for the owner to pick), which is
a map-style design change, not a one-answer fix.

## Owner pick 2026-11-22 23:35 (dialog with Stave Lake pictures: today vs shaded band)
"Yes, the shaded band (Recommended)" — build it, with the colour-blind gate check, Flow mode and emulator shots.

## Built (2026-11-23, lane L18, branch feat/T467-lake-shade-band)
- **The look** the owner picked (`T467-shots/stave-{light,dark}-shade.png`, the pictures of round 2): every colour a
  lake's shoreline FOLLOWS from its fill is drawn a shade off it — light 40% toward black, dark 45% toward white — in every
  followed mode: the status day (Map tab), Flow (`standing`, the Conditions tab: an ungauged lake now has a grey band),
  stocked, surveyed, runs and salmon. Plain keeps the outline's own colour; the fills and their opacity are untouched.
- **How (AGENTS 28/29):** `layers.source.json` `lake.outline.shade` names a themeable number token, `shade.lake.edge`
  (tokens.json + themes); `pnpm style:build` writes `{token, shade}` on each followed colour (`build-style.mjs`, which
  refuses a shade on an edge that does not follow, a non-number token or a value outside [-1, 1]); `colorExpression`
  resolves it with `shadeHex` (throws on a colour it cannot shade). One path for web and native (both paint through it).
- **The colour-blind gate** (`tools/status-colours-distinct.test.ts`, "(T467)"): measured, the shade costs separation
  for a colour-blind reader (a uniform shade moves every status the same way: light own's shoreline darkens toward
  closed — 4.4 as tokens for a deuteranope at 40% —, dark closed's lightens toward the no-access grey). The fills, which
  carry the status, keep every T47 floor. New rows, pinned at what the look reaches (rounded down to 0.5), with mutation
  pins (no shade: the band vanishes into the fill; a deeper light shade: own's shoreline goes the way of closed; a shaded
  cvd theme): `band` the shoreline shows on its own fill (light 22, dark 11, normal vision); `edges` two lakes'
  shorelines as thin lines (light 8.5, dark 4.5, worst vision); `other` a shoreline against every other status line
  (light 5.5, dark 4.5). **Default taken: the colour-blind theme keeps its solid edge (shade 0)** — that theme exists for
  exactly the readers the shade costs. The owner may want the band there too, or a smaller shade in light/dark.
- **Over satellite** (T940's set, merged 2026-11-23) the shoreline is NOT shaded: there the lake's fill fades and its
  outline is the status line itself, in the satellite set's own colour (`onImageryTokens` sets the shade to 0;
  `tools/satellite-status.test.ts` caught the clash on the merge).
- **Tests:** `lake-shade.test.ts` (shadeHex; every followed mode shaded, plain and cvd not; fails with the layer's shade
  removed), the gate above, and the pins that wrote the edge as the fill's own colour updated to the shade
  (`lake-outline`, `profile-style`, `colouring`, `finfish-paint`; the frozen `midzoom` fixture's light/dark `lake__edge`
  colours carried by hand, hex for hex). `vitest related` on the change: 2,626 passed.
- **Shots** (`T467-shots/`): web, Stave + Alouette lakes, light / dark / colour-blind, Map tab (`regs`) and Conditions
  (`flow`), today vs shade (`stave-web-*`); Android emulator (`stave-android-*`, today = the kept test-bed APK of
  2026-11-22, shade = this code): light / dark / cvd and light Flow. The Android pairs are framed a little differently
  (the old APK does not read `fit=`). On the phone at a ~5 km frame the dark band is subtle; it shows clearly in light.
