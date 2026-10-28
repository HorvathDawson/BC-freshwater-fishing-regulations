# T222 — Stave spawning channels: add the Ruskin / Northrop channels so Stave r1/r2 can bind

Status: OPEN

Source: owner 2026-10-27 "needs a split" list (decisions.md): "Stave at Ruskin Dam (and channels): should already exist —
find the existing splits and bind." Applied in 540995d4 for the z2 marking rule; Stave r1/r2 stayed undrawn because the
Ruskin and Northrop spawning channels are FWA side channels (blks 355993381-4, 355997566-7) with NO sections in the atlas.
Moved off OWNER-TODO 2026-10-28 (stale-item sweep): no owner call is needed, only build work.

## Work
1. Add the channels as added streams (pipeline/atlas/waters/added_streams), name them from the book / FWA.
2. Side atlas build, then bind Stave r1/r2 to the channels (never below-dam binding: that widens to the mainstem).
3. Binding diff before promote (only Stave r1/r2 should move).

## Acceptance
Stave r1/r2 bind the channels' sections; binding diff shows no other rule moved; the review tool no longer lists them
as undrawn.

Depends: next atlas rebuild window. Size: S–M. Owner: no. Default: —
