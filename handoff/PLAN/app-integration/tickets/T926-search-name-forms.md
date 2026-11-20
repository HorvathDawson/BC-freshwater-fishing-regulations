# T926 — Search: the name forms people type (abbreviations, "/", the book's own names)

Status: IN PROGRESS — 8ad09bf33, 9f287e8d3, 65694b0b4, 986c6e190, 244d086e3, af0bb8a04; merged via c6c46d7f9; owner looks approved 11-20; left (Claude): the atlas half (wbk variant registry pass, 21-23 of the 31) ships with the next atlas rebuild; the number-names follow-up is on lane/t926-numbers, not merged; the rest ships with the next data release. (Earlier note: IN REVIEW — owner: OWNER-TODO "T926 = every entry in name_variants.json finds its water" (owner looks at the pictures, then main chat review + merge; ships with the next data release — 21 of the 31 need the atlas registry pass, see "Lane 2026-11-20").)

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
Depends: none. Size: S. Owner: answered 2026-11-19 23:59 (see the last section): SUPERSEDES the earlier scopes: the variants file
as it stands, nothing else. (Earlier "build 1, 2, 3, 5" here was Claude's reading, never the owner's.)

## Owner 2026-11-19 20:31 (main chat, question batch)
(4) Picked "Use BC Geographical Names (Recommended)": official and alternate names from the BC Geographical Names
service become search names only; the names shown on the map and cards stay as they are.
## Owner 2026-11-19 20:33 (main chat): (4) withdrawn
Verbatim: "We have all the name variants in the file / We want to add those / I made a mistake there", then, asked which
file (curated `name_variants.json`: 7,742 / 7,773 found, no First Nations names except Tŝilhqox Biny in a note):
"It sound like it has them all already". So: no BC Geographical Names fetch; (4) is dropped. Add instead: make the 31
curated variants search misses into finds. "Sts'ailes", "Stó:lō" etc. still find nothing after this (no source holds them).
## Owner 2026-11-19 23:49 (main chat): scope
Verbatim: "We want all the variants no abbreviations though and no numbered lake names. We do want to add all the cuts on a
river as a searchable thing too. So you could search something like ten Mile pool and get chuckwalla river?"
So T926 is now:
- IN: every name variant finds its water: the 31 curated `name_variants.json` misses, and the book's own alternate names
  (bracketed names, `book_name_links`) that are real names. (5) the "Pete'S Pond …" casing/description bug stays (a
  display fix).
- OUT: (1) abbreviations (Lk, Ck, R., St); generic numbered names (a "Lake #265"-style label standing in for an unnamed
  water); the book's print marks ("L.", "*"); (4) First Nations names (T930).
- Owner 2026-11-19 23:56, correcting the line above: "number lakes like that are fine but something like lake #265 or
  whatever is bad. Kettle lake 2 is okay I think." So a real name with its number ("Kettle Lake #2" / "Kettle Lake 2",
  "Champion Lakes No. 1 & 2") IS a variant and finds its water.
- (2) joined headings ("Chilliwack / Vedder Rivers"): only if a joined heading is itself one of the variants; no general
  splitting rule.
- The cuts part is T927 (confirmed the same message; "Ten Mile Pool" is a placed cut on the Chuckwalla River).
## Owner 2026-11-19 23:59 (main chat): the variants file as it stands (supersedes 23:49 / 23:56)
Verbatim: "Maybe we should just leave all numbered lakes in and just user the name variants file as it stands. Only
replicate obvious times.... It feels less hacky that way". So T926 is ONLY:
- every entry in `data/curated/waters/name_variants.json`, as it stands, finds its water (the 31 misses today become
  finds), numbered names included, with no filtering or exclusion rules;
- no abbreviation rule, no joined-heading splitting, no book entry or bracketed names, no new name source;
- (5) "Pete'S Pond …" stays only if it is a display bug, not a search change;
- "Only replicate obvious times" read as: fix only obvious errors in the file (Claude's reading; confirm).
Acceptance: the probe's curated-variants class 7,773 / 7,773 found; exact-name ranking unchanged.
## Owner 2026-11-20 (main chat, question): generic numbered labels stay
Asked whether to remove the 290 generic labels ("LAKE #1013", "LAKE 5567": 288 from the stocking records, 2 markers; all
search-only, never displayed; the stocking records name those lakes by them). Answer: "Keep as search-only
(Recommended)". The file stays as it stands; they find their lakes.

## Lane 2026-11-20 (lane/t926-t927, commit 8ad09bf33 after the rebase onto L2)
The 31 misses, by cause (probe rerun: the main chat's 52k probes, scratchpad `probe/probe2.mjs`):
- 21 names on 16 lakes never reached their item: `apply_name_variants` names graph nodes, and an isolated lake is minted
  AFTER it, so Lilypad Lake's "LAKE #905" / "SCOTT #2", "TEANOOK L.", "BIGHORN RESERVOIR", "KETTLE LAKE #2" … stayed off
  the registry. Fix: registry pass `attach_wbk_variants` (atlas build + `pipeline.atlas.sidecars`, idempotent; on the live
  registry it adds 81 names to 43 items, `variants` + `name_sources` only — no name, kind or section moves). SHIPS ONLY
  WITH A NEW ATLAS REGISTRY (an atlas build, or `sidecars --out` + reach + tiles + deliver): main chat's call.
- 3 were folded away by the bundle's alias de-duplication (`tiles.names.normalise` turns "*", "#" into spaces): "SNOW LAKE *",
  "WEST EVERETT LAKE *", "Pigeon Lake # 2". Fix: aliases are new by the SEARCH key (`bundle.build.alias_rows`): +13 alias
  rows province-wide; a lone "." stays out as before.
- 5 one-word stocking names ("BEAVER" ×2, "FISH" ×2, "WEST") were aliases tied with every bigger
  Beaver Creek's prefix match and cut at row 40. Fix (core `queryIndex`): a name or alias typed EXACTLY is never cut off by
  the limit; it takes the last rows, the top is unchanged ("STANLEY", "PHANTOM": the same, once the registry pass gives them).
- "UNNAMED 3-20" read as MU 3-20 + "unnamed". Fix: a whole query that IS a name is found whatever part reads as an area.
- "." (a stocking record's name for Plateau Ponds) cannot be typed as a search (under 2 characters) — the one left:
  owner question below.
Probe, curated variants class: 7,749 / 7,780 found before → 7,779 / 7,780 after (all fixes together); the next data
release alone (no atlas pass) finds 7,758 — the other 21 wait for the registry pass. Exact-name queries (4,214 distinct): the expected
water's rank unchanged for 4,209; the 5 that moved all improved (3 not found → found, 2 rank 2 → 1). Keystroke time
unchanged (6.2 ms / query on the probe set, both).
(5) "Pete'S Pond …": a display bug — `_display_case` now lower-cases a lone letter after an apostrophe ("Pete's", "Sam's
Folly Lake"; "O'Connor", "D'Arcy" unchanged; 8 names). Atlas-side too, so it ships with the registry pass. The glued
description is the file's own text ("PETE'S POND UNNAMED LAKE AT THE HEAD OF SAN JUAN RIVER", source regulation) — not
edited (owner question).
Pictures (390 px, web, the side release): `T926-shots/` before/after for "SNOW LAKE *" and "Pigeon Lake # 2".
Owner questions: (a) the variant "." — leave it (it can never be typed) or remove it as an obvious error in the file?
(b) Pete's Pond: split the description off its regulation name in the file (shown name would become "Pete's Pond")?
(c) ship the registry pass: a new atlas registry (sidecars) before the next data release, or with the next atlas build?
Proof: the side deliver of T927's lane section (r-8d7bd8d70ff6 vs live r-6258ffded676): only the search tables moved
(bundle `alias` +13, `name_source` +13; core the same + `cut_place`); every rules file byte-identical. The atlas half
(registry pass, display case) was measured on the live registry in memory only, not built.
Side find: the lake the file calls "Pigeon Lake # 2" shows as "5566" (its marker label is its shown name) — not touched here.

## Owner 2026-11-20 ~06:34 (main chat dialog): pictures approved; clean-ups approved
The owner looked at the pictures and approved them. Approved data clean-ups in `name_variants.json`: (a) remove the
"." variant; (b) Pete's Pond's regulation-sourced name "PETE'S POND UNNAMED LAKE AT THE HEAD OF SAN JUAN RIVER" becomes
"PETE'S POND" (its `note` already holds the description; kept); (c) find out why the lake the file calls "Pigeon Lake # 2"
shows as "5566" and propose a fix (not applied).
## Owner 2026-11-20 06:38 (main chat): the condition on the clean-up
Verbatim: "Withe the name clean up make sure it still matches properly. And that it doesn't need a new override json".
Done as below: no override file touched, every match proved unchanged.

## Review fixes 2026-11-20 (lane/t926-t927)
- Exact names under the limit (9f287e8d3): every row typed exactly is kept ahead of the rest; only non-exact rows give
  way; more exact rows than the limit returns the first `limit` of them (pinned with 50 and 90 "Long Lake"s).
- The registry pass never makes a matcher key name a second water (65694b0b4): `attach_wbk_variants` keys each new name
  as `matcher.build_name_index` does and skips one whose key already names another water, listing it in
  `wbk_variant_report.json` (printed by the atlas build and sidecars); unknown name sources are refused. Dry run on the
  live registry: 43 items gain names, 0 existing keys change, 0 lost, 37 new keys each naming one water. NOT attached
  (7): "KOOTENAY LAKE - UPPER WEST ARM" / "- LOWER WEST ARM" on Kootenay Lake (keys name the parts wbk:-21 / wbk:-22),
  "WEST ARM" on Kootenay Lake (names wbk:329400588), "SCOTT #2" on Lilypad Lake (Scott #2 Lake, wbk:329055056),
  "BLUFF LAKE" on Barbour Lake (three Bluff Lakes), "Dorothy" on Dorothy Lake, "Edith" on Edith Lake (another lake each).
  So "BLUFF LAKE" stays a search miss (the probe: 7,778 / 7,780 with the atlas pass; the other is "."), by design.

## Clean-ups (a) and (b), with the owner's matching proof
Edit (`git diff` of `data/curated/waters/name_variants.json`, checked by loading both versions): 2 of 3,708 entries
change, exactly the two names; `check_curated --waters`: 0 errors, 0 warnings. `book_name_links.json` untouched.
What reads the regulation-sourced variant text: the atlas (graph name tuples → the registry item's name, variants,
name sources), and through the registry the matchers (`matcher.build_name_index`: `covered.make_matcher` for
`backfill_matched` / `reparse_candidates`, `dfo_salmon.match`, the batch exporter) and search. The build's bindings
read each entry's stored `matched`, never a re-match (`reach/covered.py`). `check_curated` only checks shape and targets.
`water_kind.flows` keeps the lake a lake ("Pete's Pond": a pond does not flow).
Proof (scratchpad `probe/match_proof.py`, read-only on the live registry, no atlas build, no parser): the registry as
the atlas would give it after the pass + the two edits, against the live one —
- name index: one key leaves, "petes pond unnamed lake at the head of san juan river" (it named only Pete's Pond,
  wbk:329504349); "petes pond" already named only that lake (its bathymetry "PETE'S POND *"), so no key is added; "." has
  no key at all. No other key gains or loses a water.
- every synopsis entry re-matched by the build's own matcher + overrides (`covered.make_matcher`), by its verbatim name
  and by its display name: 2,996 matches, 0 differ (live vs pass, pass vs edited, live vs edited). The Pete's Pond row
  ("\"PETE'S POND\" Unnamed lake at the head of San Juan River") matches wbk:329504349 through its existing override
  before and after; its stored `matched` is that lake. No new or edited override JSON is needed.
- The lake's shown name becomes "Pete's Pond" with the next atlas build. Not touched (A2 file, not asked): the synopsis
  entry's own `display_name` in `region-1.json` still reads "Pete'S Pond Unnamed Lake At The Head Of San Juan River".

## (c) Why "Pigeon Lake # 2" shows as "5566" — proposed, NOT applied
The lake (wbk:329116782) has no gazetted name; its names all come from the file: "PIGEON LAKE #2" (bathymetry),
"Pigeon Lake # 2" (marker), "5566", "CONTROL (PIGEON)", "PIGEON LAKE #2" (stocking). The shown name is the
best-ranked source's first name (`graph.names.apply_name_variants` → `_sorted_unique` by `NameSource` order: stocking
ranks above bathymetry and marker), and the first stocking name is the stocking record's lake number "5566". Three more
lakes show a bare number the same way: 3261 ("UNNAMED CAYOOSH CR"), 3296 ("UNNAMED VANHORLICK 1"), 3299 ("UNNAMED
VANHORLICK 2").
Proposed fix: in `apply_name_variants`, a name of digits only is never the shown name while the water has a name with a
letter in it (it stays a search name). Pigeon Lake #2 would then show "Pigeon Lake #2" (its stocking "PIGEON LAKE #2",
cased); the other three "Unnamed Cayoosh Cr", "Unnamed Vanhorlick 1" / "2". A code rule, no data edit; atlas-side
(ships with the next atlas build). Alternative: `"display": true` on the wanted name in the file, one entry per lake.
Matching: none of these numbers is a book name; the proof above would be rerun with the change.
