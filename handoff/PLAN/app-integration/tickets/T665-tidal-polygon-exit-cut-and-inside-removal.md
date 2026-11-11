# T665 — Tidal: cut every creek where it leaves the tidal polygon; remove lines wholly inside it

Status: OPEN

Source: owner 2026-11-11, on the T660 follow-up (A–J): "every item in the 103 can be removed. And the plan in general
looks solid just need to figure out a clean way to implement it." Next atlas rebuild (with T664 and the nine waiting DFO
cuts); NOT build 27. Left alone until build 27 is out.

Measured in `T660-tidal-tributaries-not-cut-at-boundary.md` ("Owner follow-up 2026-11-11 (A–J)"),
`handoff/PLAN/app-integration/T660-owner-followup.json`, `handoff/PLAN/app-integration/T660-inside-named.json`, maps in
`handoff/PLAN/app-integration/T660-shots/`.

## Owner rulings
1. A creek that joins water tidal under the curated limits ALWAYS gets a cut; only the piece inside tidal water goes
   tidal. No 50% skip, never all-or-nothing ("Removing the entire section is bad").
2. Every line wholly inside the tidal polygon is removed: the 103 unnamed AND all 103 named (206 lines, 66.4 km).
3. Blue line over tidal shading is fine where there is no reason to remove it. The 191 freshwater receivers are as
   wanted (the polygon is not the regulation boundary). T662 dropped.
4. Short slivers (under 5 m) can be removed when unnamed.
5. Serpentine, Cowichan, Copper Creek: already `mouth`; nothing to change.

## Work (design first: the owner wants a clean way to implement it)
- Cut point = where the line leaves the DFO tidal polygon (lane's recommendation over the crossing top): of the 161
  tidal cuts 55 move < 25 m, 101 move up (+14.0 km tidal), 5 move down (−0.2 km); +12 cuts on creeks with no crossing run
  (Fraser delta, Steveston 356242813); 1 of the 25 skipped runs changes (356070273). One cut per line, at the highest
  exit (3 lines re-enter the polygon).
- The 371 cuts into freshwater receivers make nothing tidal: propose dropping them (owner has not ruled).
- Removal: proposed as an atlas sidecar `inside_tidal.json` read by `reach.outside` and the tiles. 26 named lines carry a
  water rule: show what each rule binds after removal before promoting. Not included: 15 lines ≥ 95% inside (15.3 km).
- Add the shore rule to RULINGS.md. Binding diff + maps to the owner before promote.

## Open with the owner
- Chambers Creek (360880075): the owner did not follow the explanation; explain again with the map, no change proposed.

Depends: T660. Size: L.
