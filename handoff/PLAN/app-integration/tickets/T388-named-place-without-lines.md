# T388 — A named water's link place never fetches `lines`

Status: DONE in code (dfe887d6a; owner 2026-11-04); live with the release rebuild + a re-cut of the request-walk pin.

Source: owner 2026-11-04, after the 0.5.0 walk re-cut (08b263ee7): the web's address bar (T369, `systemBack.web.ts` →
`@app/ui` place.ts `placeOf` / `linkSearch` / `resolveStartLink`) read the `lines` file (1.2 MB gz, every stream section
in B.C.) for any named water of several parts or stretches, and for every TAPPED frame — so tapping a named water fetched
all of `lines` just to write the address (`request-walk.pin.json`: the named tap, the save-data tap, the search open).

## Owner decision (2026-11-04)
- NAMED waters: the place comes from a new small table in the WATER file, `part_line` — each part's own stretch of
  stream as runs `(ord, part_ix, stretch, blk, m0, m1)` (blue line key + metres, the units of `lines` / `StreamPlace`),
  keyed by `ord` so it rides in the `water-<region>` slice the page already fetches. Additive to water/1: a release
  without it uses the old `lines` path (documented as the pre-T388 release path).
  - Writing: a named frame (tapped or not) → the middle of its part's (or stretch's) runs. One part, one stretch: no
    place (T369). `here=` is separate and unchanged.
  - Opening `regs=<item>` + blk + m: the part of THAT item whose run holds (blk, m), and its stretch; none → the
    "place not on this map" notice (T369 D9), never a guess.
- UNNAMED reaches: unchanged — `lines`, fetched only when one is on screen or a link to one is opened.
- Merging stays: a link stores one point; opening it rebuilds that release's part / reach around it.

## Work (done)
- Pipeline `release/part_line.py`: runs from the bundle's `part_section` + `part_stretch` + `section_measure` (read once
  for `lines` too); touching sections of one part + stretch on one blue line merge; lakes / polygons / zero-length
  sections give none. ONLY waters whose links carry a place (2+ parts, or a part of 2+ stretches): measured on the
  2026-11-04 bundle 9,661 runs / 79 kB gz for 682 waters (every named water would be 53,639 / 451 kB). Written into
  `water` (`build.py` `water_extra`), cut into `water-<r>` by ord (`slices.WATER_SLICE_TABLES`, additive like
  `water_continue`); `contract.py` + `pack.generated.ts`; the `water` size gate 2.5 → 2.65 MB (2,409,579 + ~79 kB was
  ~11 kB under 2.5 MB).
- App `@app/data` `RegsReads.partLines(ord)` (null: a release cut before the table; read off `sqlite_master` AFTER a
  keyed read of the water, so the schema is never asked of `water-shared`); `focus.ts` TABLES `water.part_line` by ord.
- `@app/ui` place.ts: a named place = the middle of the part's (stretch's) LONGEST run (the main stem, not a side
  channel); a named link resolves on its own water's runs (`resolvePartPlace`), a tapped one reopens the tap on the
  part's (stretch's) midpoint section at its `here=`; a tapped one-part stream (no place) reopens the same way
  (`ownPart`). A named link whose water this release does not hold: the notice.

## Acceptance
- pipeline `test_part_line.py`: runs for a 3-part fixture (merge, side channel, gap, stretch, lake part, zero length),
  only placed waters, the slice holds its waters' runs, an older water's slices have none; mutation pins on each.
- app `place.test.ts` (T388 suite): every `lines` read THROWS and is counted — named writing (part, tapped, stretch,
  one part), opening across a re-split, the side channel, a tapped round trip, a place no run of THAT water holds
  (incl. one `lines` would open as an unnamed reach — the mutation pin); a release without `part_line` reads `lines`;
  an unnamed reach still does. `reads.test.ts` partLines; `focus.test.ts` routing.
- `tools/release-budget.test.ts` "T388": the named moments' pin has no `lines` — FAILS until the pin is re-cut on a
  release built with `part_line` (`node tools/request-walk.mjs --url http://localhost:<port> --pin`); the unnamed tap
  still fetches it.

## Left (not this lane)
- Deliver re-run (bundle unchanged; the release step writes `part_line`), then re-cut `request-walk.pin.json`.

Depends: none. Size: M. Owner: no.
