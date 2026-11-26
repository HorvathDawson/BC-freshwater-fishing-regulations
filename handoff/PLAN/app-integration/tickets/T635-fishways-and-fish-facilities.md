# T635 — Find where the fishways and government fish facilities are (the 23 m and 100 m no-fishing buffers)

Status: POSTPONED

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T635 | Find where the fishways and government fish facilities are (the 23 m and 100 m no-fishing buffers) | POSTPONED | — | owner 2026-11-11: benched, data not good enough; findings kept (2e92f9f0b); CABD is CC BY-SA, commercial allowed |

Source: owner 2026-11-11. Book p.9: no fishing "within 23 m downstream of the lower entrance to any fishway, canal, obstacle
or leap" and "within a 100 m radius of any government facility operated for counting, passing or rearing fish (e.g.,
fishway, fish hatchery)". Today these two are words only; the other three exceptions are drawn on the map. The owner looked
for location data and found none.

## Work
1. Research, read-only: every public source that places fishways, fish ladders, counting fences and weirs, hatcheries and
   spawning channels in B.C. (Province: obstacles to fish passage, FISS, dams; DFO: Salmonid Enhancement Program facilities,
   hatcheries, counting sites; Freshwater Fisheries Society hatcheries; PSF; OSM `waterway=fish_pass`, hatchery tags). For
   each: licence (quoted), count, fields, how current, how complete against a known list.
2. Say plainly what can be drawn (points with a buffer) and what cannot be known from data.
3. A proposal for how the app shows them (a marker and a caution near the water, never a claim that everywhere else is clear).

## Acceptance
- The owner has a page with the sources, counts, licences and a recommendation. Nothing enters a release without his yes.

Depends: none. Size: M (research first).

## Owner 2026-11-11: benched
"Let's bench this idea... we don't have good enough data for now and I don't have bandwidth to find fences." Findings kept in
git history (commit 2e92f9f0b, T635-findings.md).
Correction to the findings, from the owner's paste of the CABD site terms: the Canadian Aquatic Barriers Database grants
"personal or commercial uses ... under a Creative Commons license (the specific license is CC BY-SA)". So it is NOT
non-commercial: it can be shipped with attribution, and anything built on it must carry the same licence (share-alike). Check
what share-alike means for our own layer before using it.

## Still open (from the deleted T635-findings.md, 2026-11-25)
Benched; the research page is in git history (commit 2e92f9f0b). What it concluded:
- Hatcheries: DFO's licence-holder Enhancement list (Open Government Licence, Canada) names every DFO, FFSBC and about 100 community site, but its points sit 0.03 to 2.2 km off the real site: hand-place before drawing a 100 m circle.
- Fishways: CABD has 74 B.C. fishways, no "lower entrance"; licence CC BY-SA per the owner's paste of the site terms (share-alike on anything built on it; ask CWF before more than a checklist).
- No clean dataset for spawning channels or counting fences. The 23 m rule already applies at every T173 obstacle (FISS falls, dams); no data says where the entrance is.
- Proposal: a hand-placed layer of 35 to 45 points (marker plus 100 m circle; a 23 m mark only where hand-placed), about 2.5 days; community sites without a circle unless the owner says so.
- Wording never says "no restrictions here"; the layers key says not every fishway, falls or facility is shown.
- Not verified: DFO SEP facility count (23), counting-fence details, bcfishpass fishways, whether FFSBC or community sites count as "government".
