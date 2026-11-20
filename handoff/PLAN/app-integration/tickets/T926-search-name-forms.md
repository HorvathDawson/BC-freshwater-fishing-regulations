# T926 — Search: the name forms people type (abbreviations, "/", the book's own names)

Status: OPEN.

Source: owner 2026-11-19 (main chat), verbatim: "did we ever make sure handle name variants of water bodies?" Measured the
same evening (read-only probe, scratchpad `search-probe/`: the real `queryIndex` on release r-6258ffded676's index, 19,782
waters + 2,917 aliases, ~52k queries, found = in the top 40):
- curated `name_variants.json` 7,742 / 7,773 found; registry variants 6,328 / 6,338; book entry names 1,300 / 1,351;
  bracketed book names 59 / 62; `book_name_links` 368 / 422; bare names ("Vedder") 6,076 / 6,080; no-accent forms 41 / 41.
- Misses: abbreviations — "Lk" 22 / 6,125, "Ck" 0 / 10,906, "R." 0 / 855, "Lks" 19 / 1,172, "St"/"Saint" 0 / 58 ("St Mary
  Lake" finds nothing); punctuation "L.", "#", "*" ("CAMPBELL L.", "KETTLE LAKE #2", "SNOW LAKE *"); joined names —
  "Chilliwack / Vedder Rivers", "Atnarko/Bella Coola", "Lillooet Lake, Lillooet River", "Weaver Lake and Weaver Creek",
  "Champion Lakes No. 1 & 2", "Kootenay Lake, All Parts"; First Nations names ("Sts'ailes", "Stó:lō", "Xsan", "Lisims",
  "Quw'utsun") — no source holds them on the waters (Sxótsaqel only as a park).
- Side find: a water shows as "Pete'S Pond Unnamed Lake At The Head Of San Juan River" (title-casing after an apostrophe,
  and a description glued to the name).

## Work
1. `searchKey` (and its pipeline twin, rule 40): drop ".", "#", "*"; expand Lk/Lks/Ck/Cr/R/Mt/St(e)/Saint both ways. (S)
2. A query with "/", "," or " and " between names searches each name and returns the waters of all (the joined book
   headings). (S)
3. The book's own entry names (and bracketed alternates) as aliases of the waters they name (the bundle may read the
   corpus, rule 56). (S–M)
4. First Nations names: from the BC Geographical Names service (official and alternate names). Owner call: the source.
5. Fix "Pete'S Pond …": the name's casing after an apostrophe, and split the description off the name.
## Acceptance
The probe rerun: every miss class above found (except 4 until the owner picks a source); `searchIndex.test.ts` pins one
case per class; no ranking change for exact names (the probe's 22,699 exact queries unchanged).
Depends: none. Size: M. Owner: answered 2026-11-19 (4: BC Geographical Names). All of 1-5 to build.

## Owner 2026-11-19 20:31 (main chat, question batch)
(4) Picked "Use BC Geographical Names (Recommended)": official and alternate names from the BC Geographical Names
service become search names only; the names shown on the map and cards stay as they are.
