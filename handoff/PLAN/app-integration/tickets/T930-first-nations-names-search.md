# T930 — First Nations names in search, added to name_variants.json (future)

Status: POSTPONED (deferred, owner 2026-11-19: after the bathymetry map, stocked lakes, charts etc.; T200/T201/T634).

Source: owner 2026-11-19 20:34 (main chat). After T926 (4) was dropped (the curated file holds no First Nations names), asked
"How would we get the first Nations names otherwise?"; offered four sources (BC Geographical Names; OpenStreetMap
`name:*` tags; a short hand list of the big waters; the Nations' own place-name maps, which need their permission).
Owner, verbatim: "Add ticket to do that in future and add to name_variants JSON in future".

## Work
1. A short hand list first: the well-known names of the big waters, each with a public citation, e.g. Stó:lō = Fraser
   River, Xsan = Skeena River, Lisims = Nass River, Quw'utsun = Cowichan River, Tŝilhqox Biny = Chilko Lake (already in a
   note), Sts'ailes = the Chehalis/Harrison area (also the Nation's name: say which water, or leave it out).
2. Then BC Geographical Names (BCGNIS, Open Government Licence – BC): its official First Nations names and the alternate /
   former names it keeps beside them, matched by GNIS id to the waters.
3. Every name lands in `data/curated/waters/name_variants.json` as a PROPOSAL (rule: everything added is a proposal), with
   its own `source` (e.g. `first_nations_list`, `bcgnis`), the citation in `note`, and `display: false`: a search name
   only; the name shown on the map and cards does not change. Names as sourced, never respelled (diacritics, glottal
   stops and combining marks kept; search folds them, T926 no-accent forms).
4. Not here: the Nations' own atlases / FirstVoices maps (need each Nation's permission; only if the owner asks).

## Acceptance
Each added name finds its water in the top results (the T926 probe); `searchIndex.test.ts` pins Stó:lō -> Fraser River
and one BCGNIS name; no ranking change for exact English names; owner reviews the proposals before they are accepted.

Depends: T926 (search folding of "'", ":" and combining marks). Size: S (list) + M (BCGNIS). Owner: reviews the proposals.

## Owner 2026-11-19 20:38 (main chat)
Verbatim: "We will make t930 defered until after bathy map and stocky and charts etc..". Starts after T200 (stocked
lakes), T201 / T634 (depth maps and charts) ship.
