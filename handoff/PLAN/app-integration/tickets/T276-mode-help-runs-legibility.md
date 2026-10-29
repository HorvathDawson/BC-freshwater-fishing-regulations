# T276 — Mode notes by the mode switch, Runs tab legible on phones, first-open tutorials for Runs and Conditions

Status: OPEN (owner 2026-10-29).

Source: owner: the DFO line ("Salmon rules from Fisheries and Oceans Canada (DFO), pages dated up to …. Waters DFO does not
list are closed to salmon.") "should maybe go near the button used to change mode instead of in legend at bottom"; "the run
tab is very hard to read on mobile feels small. the slider should be bigger and the spawner info [legend: 'Spawners counted
… 10 100 1k 10k 100k … No counts …'] should just be part of tutorial on open instead same with conditions it should have a
tutorial on how to read it too."

## Work
1. Salmon mode's DFO source line moves next to the mode switch (small, same words), out of the bottom legend.
2. Runs tab on phones: larger slider (thumb + track, ≥44 px touch target), larger text; the spawner scale legend leaves the
   bottom block.
3. First-open tutorials, using the existing coach-mark tour component (T248): Runs mode (what the colours/scale mean,
   "no counts are shown as such, never guessed", tap a stream) and the Conditions tab (how to read it). Shown once, re-openable
   from Help.
4. "Run timing could not be loaded" on local: serve /runtiming/ from the local tile server like /feeds/ (owed since T261).

## Acceptance
Phone screenshots: Salmon with the DFO line by the switch; Runs before/after; both tutorials; map rect unchanged across modes
(T275 holds).

Depends: T275, T248. Size: S–M. Owner: no.
