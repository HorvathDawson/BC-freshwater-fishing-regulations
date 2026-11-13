# T467 — Lakes: a visible shoreline inside the colour-blind gate

Status: OPEN (owner 2026-11-06 on 0.6.8: "the lakes still look solid").

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
