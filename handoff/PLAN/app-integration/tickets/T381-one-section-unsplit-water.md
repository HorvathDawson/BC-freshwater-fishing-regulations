# T381 — A water with no splits is one section (Nicomen Slough: polygons + lines)

Status: OPEN (owner 2026-11-04, in 0.5.0).

## Ask (owner 2026-11-04)
"could we make it so something like nicomen slough with streams that are polygons or lines. we should make it so
nicoment slough becomes a single section covering all because there are no splits etc?"

## First look (2026-11-04)
The live release's `core.item` has no row named "Nicomen…" (searched `name LIKE '%icomen%'`), so how Nicomen Slough is
drawn and tapped today (FWA river polygons + stream lines, T322 slough fold, or an unnamed reach) is to be found first.

## Work
1. Trace Nicomen Slough through atlas → registry → sections → release: which geometries (polygon / line) it has, how
   many sections it gets and why each cut exists.
2. Rule: a water whose regulations make no split is ONE section over all its geometry (polygons and lines); a cut only
   where a rule, a part boundary or a confluence that changes the answer needs it. Find every water this changes
   (diff section counts, binding diff as in T322) before promoting.
3. Atlas rebuild (heavy, heavy.sh) + reach step + bundle; owner sees the before/after list.
Size: M–L. Pipeline.

## Found 2026-11-04 (bundle.sqlite)
Nicomen Slough = `gnis:21105` (ord 4250), stream, ONE part (part 0, rule set 281/325), 9 sections, all the same rules:
5 LINE sections on FWA stream 355991886 (sids 59586–59590, measures 0–22,755 m) alternating with 4 POLYGON sections
(shape 1, sids 1266784, 1266788, 1454480, 1454614: where the slough widens into mapped water, measures 16,690–22,416).
So no rule splits it; the 9 pieces are geometry (line ↔ polygon), the atlas's section unit. (The release `core.item`
search by name found nothing earlier — check how names are stored there; the bundle has it.)
Question for the owner: what showed it as several — the tap / highlight lighting one piece, the polygon pieces drawn or
coloured differently, or a count ("9 sections")? Merging sections themselves is atlas-wide (handles, gauges, marks);
making every piece of a part light and answer as one is an app change.
