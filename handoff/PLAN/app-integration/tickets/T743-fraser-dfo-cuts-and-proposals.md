# T743 — The Fraser's DFO places: cuts checked, proposals fixed, a dossier to label from

Status: DONE on the data side, 319a447a0, a9fbaa802, eafa7b9f5 (Fraser data lane, 2026-11-14/15); the re-marks and cut acceptances wait on the owner

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T743 | The Fraser's DFO places: cuts checked, proposals fixed, a dossier to label from | DONE data side 2026-11-14 (suggester + dossier); Fraser marks reset at the owner's ask (cleared marks + places restored); 2026-11-15 channels / Redsand / spot checks / nine cuts report, Redsand through-river fix; re-marks + the nine cuts wait on the owner | 319a447a0, a9fbaa802, eafa7b9f5 | owner 2026-11-14 ("fix up fraser and make sure all the cuts are good and fix proposals") |

Source: owner 2026-11-14: "also please try to fix up fraser and make sure all the cuts are good and fix proposals etc."


## Work
1. Every DFO place on the Fraser, all regions, with its dated wordings, suggestion and mark.
2. Every cut those wordings need: exists, where, right bridge; missing ones as proposals.
3. Suggestions corrected where the cause is mechanical (suggester fixes with tests); the diff shown before it is applied.
4. Fraser marks that look wrong, listed for the owner to re-mark one at a time.
Dossier: `handoff/PLAN/curation-review/fraser-dossier-2026-11-14.md`.

## Result
- Dossier written (12 places: 10 mainstem, 2 Nicomen Slough; with marks 10; written by a group click 4; suspect 5).
- Suggester (commit 319a447a0): a DFO page is held to its region (open ends stop at a region-line cut, a whole
  water across regions is not suggested whole, an end past the line is named); "A to B and from B to C" is A to C
  and never clustered with a half; a proposal on the winning cut is no rival. Tests:
  `pipeline/tests/test_dfo_suggest_fraser.py`. On today's marks 3 of 136 suggestions change, all Fraser.
- Cuts: CPR Bridge at Mission and Spuzzum right (Mission is the rail bridge, one point with the tidal limit);
  Hope Hwy 1 and Alexandra missing from cuts.json but already proposed, each under two proposal ids. Nothing added
  to cuts.json (accepting a cut needs an atlas build).

## Waits on the owner
- Re-mark the three "Mission to Hope" wordings Mission -> Hope (now Mission -> Spuzzum; two by one group click).
- Before the next release: as the places file stands, Hope -> Spuzzum (~38 km) would show chum/coho open where DFO
  says no fishing for salmon (the live Mission-to-Hope place wins a tie with "upstream of Mission").
- Alexandra: which bridge, and keep the 4.3 km past the Region 2/3 line or not; the Lillooet signs; Region 2
  "upstream of Mission" ending at Spuzzum.
- Regenerate dfo-suggestions.json only after the re-marks: `PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.dfo_salmon.suggest`.

## Reset (owner 2026-11-14, after the dossier)
Owner: "can you just reset the fraser marks fully and I will redo them when app is usable again?"
- 7 Fraser places, 15 mark lines (2546-2560): one `marks.cleared_mark` each, appended under the store lock
  (`marks.append`); no line edited or removed. Backup:
  `~/bcfish-session-backup-2026-11-12/marks-backups/marks-before-fraser-reset-20261114-233106.jsonl` (+ curated diff).
- `region-2.json` back to 29fe9d5b4 (a9fbaa802): undoes the 11 write-through commits b325fb216..4976df1cb, which
  touched only Fraser places. No cut was in cuts.json. Mark-made Hope/Alexandra proposals went with the marks; the
  split hunt's remain.
- DFO queue 320: unmarked 179 -> 186, verified 112 -> 107, unsure 23 -> 21, wrong 4, stale 2; 0 orphans;
  `review.apply --check` passes; no Fraser DFO place is bound.
- `dfo-suggestions.json` NOT regenerated: a full run also changes 37 non-Fraser rows (34 places bound since it was
  written). A Fraser-only merge (9 rows) is prepared in the lane's scratchpad; applying it is a copy.

## Owner answers 2026-11-15 (on the triage of his overnight marks)
- Alexandra Bridge stretches running 4.3 km into Region 3, as DFO's words say: "this is fine".
- The 12 older group-saved labels (Nanaimo, Chilliwack-Vedder, Stave) being assessed by the data lane: "ok good".
- Atnarko "downstream of the sign" marked twice: owner sees one extent in the app ("i only see one extent on app?"). Correct:
  the later mark (2584, with tributaries) replaced the earlier (2582); only the one in force shows. Open only if he did
  not mean to include the tributaries.
- Seton mouth cut 250 m below the Hwy 99 bridge cut: owner asked "maybe iut is the way they are retired? … look at it
  then reccommend what to do". Looked: the two are different real places, not an artefact. Hwy 99 bridge cut
  (-121.92917, 50.68356, river km 329.4, a synopsis cut: BC's no-fishing stretch runs from it down ~1.2 km to the BC
  Hydro tail race, km 327.9); Seton River mouth (-121.926856, 50.681805, km 329.1, the exact FWA mouth), 254 m
  downstream of the bridge. Recommendation (main chat): keep the Seton mouth as its own cut; the 250 m piece between the
  bridge and the mouth is real water where DFO's place does not apply. Awaiting his yes/no.
- Atnarko, owner 2026-11-15 (second reply): "no it needs to include tributaries … we really need to check a lot of the
  items for this same issue and make sure we remove duplicates or bad ones?" So mark 2584 (tributaries included) is the
  right one and is in force; nothing to change there. Asked for: an audit of every place saved more than once, for
  leftovers of the replaced save and for bad marks. Running (read-only); report
  `handoff/PLAN/curation-review/resaved-marks-audit-2026-11-15.md`. Findings go to him as proposals.
- Owner 2026-11-15: "can i keep reviewing while you work?" Yes (fix lane is in its own worktree; audit is read-only).

## Channels, Redsand, spot checks, the nine cuts, group marks (2026-11-15, data lane)
Report: `handoff/PLAN/curation-review/fraser-channels-2026-11-15.md` (no mark, curated file or suggestion touched).
- Fraser channels Mission → Spuzzum: the 29 unnamed channels (57.8 km) are in `gnis:39325` and in every mark. Dropped
  by every Fraser place: Herrling Island (12.2 km), Seabird Island North (4.7 km), Jeperson braids (3.3 km), split
  out as `blk:` items with no gnis and not in the water record. Dropped by the item-restricted pair only: Maria Slough
  (14.3 km, owner's call) + Jeperson's gazetted line (5.3 km) = the 9 sections / 19.5 km. Nicomen, Strawberry: separate.
  Province-wide 4 waters; real gaps only Fraser and Squamish (Powerhouse Channel, 3.0 km, named in DFO's own water name).
- Redsand: `wbk:-1` is a curated added lake and the mark is right; the tributary walk took the through-river
  (T468 rule, no rows on a DFO place). Fixed eafa7b9f5 (POLICY_VERSION 39): Redsand 2,639 → 4 sections, Kitsumkalum
  Lake 2,626 → 983; the other 259 DFO places identical; no shipped binding moves.
- Spot checks right: Tahltan, Nakina, Tatsatua, Nitinat parts, Atnarko (by the cascade). Little Shuswap Lake and the
  lower Atnarko wait on their cuts. Found wrong: Bella Coola "all tributaries" (2583) covers the mainstem and wins
  the tie by id, so mainstem chinook reads no fishing (needs `tributaries_only`).
- Nine cuts checked against OSM: eight ok (Alexandra per his 11-15 "fine"); second look: the Lillooet signs (which edge of town). Morice 2591/2592 sit
  on the 100 m-off cuts of the other wordings (re-mark, no rebuild). Rebuild list in the report (not run).
- Group marks: of 8 places only Nanaimo "500 m below the power line" (2442) is wrong (carries 2443's extent).
- Pallant Creek, owner 2026-11-15: "Pallant Creek we can make the bridge the tidal boudnary i think". So Pallant's tidal
  boundary moves to the "bridge near the mouth" (57 m upstream of today's boundary). Queued for the rebuild after the
  marks are done (curated tidal limit change + reach rerun); not applied yet.

## Cut clean-up after labelling (2026-11-15)
Report, rebuild input and 23 owner questions: `handoff/PLAN/curation-review/cut-cleanup-2026-11-15.md`. Done (fd78c0a6f +
write-through 9652ef28f, e702daa70, 7ed7a57fa): Pallant tidal boundary at the bridge (owner row + authority `owner`);
marks 2734-2739 (Illiance, Wilauks open mouth end; Gitnadoix unused m-0 cut dropped; Morice at the Bulkley confluence;
Morice Gosnell put back as saved, Q8). 23 new cuts remain for the side rebuild, none a duplicate; the Skeena's Hell's Gate is
`skeena_river__kitsumkalum_river_into_skeena_river_u1500m` (never renamed); the Skeena Lakelse / Babine confluence cuts do not
exist (Q12, Q13).

## Before the release that carries the labels (added 2026-11-15)
- Puntledge: weir cut added and the 75 m cut moved onto the right spot (bf1a29567, owner asked); takes effect at the
  atlas rebuild; owner re-ticks marks 1263, 2447, 2687 after it. The dormant "from the weir at the lower hatchery" place
  has no mark: his to mark (weir cut to 75 m cut). The tailrace "50 m" cut has the same anchor fault (about 27 m off):
  not changed, owner to decide.
- Closed areas inside a DFO place (T573 entry B: Thompson Deadman / Juniper / Ashcroft, Shuswap middle): proposals
  exist, none accepted yet, and NO build step reads an accepted closed area into the atlas / bundle / feed. Must be
  written and tested before the release, or those closures do not reach the app. The reader for "except closed from …
  to …" is on branch data/except-closed (df546975a), to land after marking.
- T753 lake loops (cf7df0dba, branch data/t753-lake-loops) and Pallant Creek's tidal boundary at the bridge ride on the
  same rebuild.
