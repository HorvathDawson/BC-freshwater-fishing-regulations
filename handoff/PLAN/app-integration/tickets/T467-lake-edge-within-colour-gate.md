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
Related: T457, T47 (status colours), T165, T158. Size: S.
