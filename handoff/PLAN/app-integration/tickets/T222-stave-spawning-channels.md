# T222 — Stave spawning channels: add the Ruskin / Northrop channels so Stave r1/r2 can bind

Status: DONE — live r-0fa7927b16e5 (2026-10-28), atlas 10cbcb72e9f907cf, tiles 10cbcb72e9f907cf.edc12ab96087

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

## Outcome (2026-10-28; e8ff6db1, 0f40f008, c47572af, 2ea061c4, f010a2b1)
Owner 2026-10-28 approved both bindings (decisions.md).
- Ruskin: the east-bank FWA side channels 355993381-4 are named "Ruskin Spawning Channel" in name_variants.json (source
  regulation; OSM "Ruskin Channel" at their head, the east-side channel built 1978) -> item `blk:355993381`, 4 sections.
  The name protects them from the braid prune (they had been pruned as anonymous braids).
- Northrop: not in FWA at all (OSM draws it on the west bank near the dam; no FWA line matches). Added from OpenStreetMap
  as an added stream: `data/source/municipal/osm_streams.geojson`, added-streams source `osm` (label OpenStreetMap,
  `clean._map_osm`), DEM-oriented (mouth at the south end), receiver FWA 356193720 -> item
  `wsc:100-046202-028692-080600` "Northrop Spawning Channel" (variant Northrop Channel), 1 section. OSM's 50 m
  "Northrop Creek" is dropped by the general short-tributary prune. The regenerated added_streams.json is byte-identical
  apart from that one stream.
- Stave r1 -> whole `blk:355993381`, r2 -> whole `wsc:100-046202-028692-080600`; the row's `matched` lists both channel
  items (its whole-water scope had clipped r1/r2 to nothing: `empty_after_scope`).
- Found on the way (T224, fixed at the cause, 2ea061c4): mined names were graph names, so they changed the braid prune
  (409 braid pieces kept), ditch membership (the Quinsam diversion headpond joined the Quinsam River) and slough folds
  (lake 329292348 became the OSM "Silverstream Slough" stream). They are now registry labels only (`registry/mined.py`).
- binding_diff (both reach runs at HEAD, old atlas vs new): Stave r1 +4, r2 +1; 92 zone/region rules + 1 licensing
  record take the 5 new sections; 0 existing sections gained or lost a rule. Atlas parity: +5 sections, 0 gone.
- App (:39218): both channels drawn closed; tapping either shows "No fishing for any game fish" (p.26).
  Shots: T222-shots/. Note: a start link cannot open a `wsc:` item (`startLink` accepts `prefix:digits` only).
