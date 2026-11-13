# T672 — T462 audit, the W findings and the cosmetics: tidal page, search dot, topic hubs

Status: IN PROGRESS — the mechanical fixes are in (items 1a, 2, 3, 5); left (Claude): hand the held items (1b, 4, 6) to the owner with their options, then the staging site deploy and, for item 2, the next release (it rebuilds the status index).

Source: `handoff/PLAN/app-integration/T462-audit.md` (85ba2e821), findings W1, W2, W3 and the cosmetic K9, G11, W4.
Owner 2026-11-11: fix what is mechanical; anything that is a choice, and any new wording, is held and proposed.
Release measured: the live one at the time (the audit's). Throwaway measuring scripts are not in the repo.

## Findings

1. **W1, a tidal part's page said "outside B.C." — reproduced.** 83 key-less tidal parts (81 names, 164 sections) and
   121 unnamed tidal sections read "This part of the water is outside B.C.: no B.C. rules apply."
   - **Can a map tap reach it: no.** None of the 285 tidal sections is in a drawn tile layer (stream, lake, wetland,
     lake link: 0 of 285; `undrawn.json` `tidal` lists exactly those 285); a tap there lands on the `tidal_water` area
     and opens the "Tidal water — DFO rules" sheet, which is right.
   - **What does reach it:** search. Ten waters are tidal throughout and are in the search index (Annacis, Bedford,
     Morey, Enterprise, Cannery, Sapperton and Parsons channels; Gilmour, Tilbury and Williamson sloughs): a search hit
     opens their only part. And a link whose place falls on a tidal stretch of the other 73 waters (113 part runs).
   - **1a. FIXED (named parts, 83 → 0).** A key-less part the store marks tidal (`part.tidal`, the pipeline's fact)
     draws the page the release's own tidal water (Nitinat Lake) draws: the "Tidal water" banner and the stored tidal
     note, word for word (`TIDAL_SHEET_WORDS`, the copy the map's tidal sheet already says). By the part's kind, so
     new tidal parts follow. `core/regs/edge.ts`, `page.ts`, `header.ts`, `data/regs/inputs.ts`.
     Tests: `core/regs/edge.test.ts`, `data/regs/inputs.test.ts`, `conformance/regs/tidalPart.release.test.ts` (every
     tidal part of the release under test; the app's words held equal to the release's stored note).
   - **1b. HELD (121 unnamed tidal sections).** The store says nothing that tells a tidal unnamed section from one
     outside B.C. (key 0, no place), so their page still says "outside B.C.". Reached only by a link to an unnamed
     reach, never by a tap. Needs a store fact (a release), not app code.
2. **W2, the search dot was not "closed" when every freshwater part is — reproduced: 14 waters, 1,850 water-days**
   (366-day table; 1,846 on the 365 days of 2026). **FIXED in the pipeline, matching the map:** on every one of those
   water-days every section the map draws for the water is closed, and the water's page says closed; the tidal piece
   is not drawn. `status_index.rollup` now ignores tidal parts when the water has fresh water (as it ignores parts
   outside B.C.). After: 0; exactly those 14 waters change, every changed day to closed, nothing else.
   Tests: `test_status_index.py` (`test_a_tidal_part_does_not_speak_…`, and, slow, every water × every day).
   **Shows after the next release** (the status index is rebuilt by deliver). Until then the slow test
   `test_the_shipped_index_is_the_one_the_parity_tests_check` is red against the bundle on disk: that is this change.
3. **W3, the fly-only and bait-ban hubs listed a fraction — reproduced** on staging and in a local build of the live
   release (Region 2 fly-only: 4 of 49). **FIXED:** the two hubs read the water's year strip (`PartModel.marks`, the
   presenter's legend), not the build day's sentences. Fly-only hubs, all regions: 112 → 175 entries; bait-ban:
   5,294 → 5,451; the same lists on a July and a November build day. Every water listed before is still listed.
   `tools/seo/topics.ts`, `model.ts`; tests `tools/seo/topics.test.ts`. Not deployed.
4. **W4, a water page's description names one region — reproduced (58 waters). HELD:** naming every region is new
   wording.
5. **G11, two identical "Classified (Class II)" boxes — reproduced (59 parts, 55 waters). FIXED:** a box that repeats
   an earlier one word for word is said once (`core/regs/licence.ts`); test
   `conformance/regs/licenceBoxes.release.test.ts`. After: 0.
6. **K9, "Up to 4 brown, cutthroat or lake trout of the 5 from streams" — reproduced (1,051 parts, Region 8).
   HELD:** a rewording.

7. **The site's text model after T670 — FIXED (e1345fae2).** The visible water page is the app's own pane, prerendered, so it
   already draws T670's lines (asked in the test: Charlie Lake, Kispiox, Mahood). The site's text model (hubs,
   content hash, /changes) selected presenter strings by hand and missed a row's fish that must go back ("Release
   every mountain whitefish."), the species key, the boats line and a caveat item's detail lines: now carried, same
   strings. Test `tools/seo/t670.web.release.test.ts`: each pinned line of T670's waters, and a guard that every
   string the view model carries in those fields is in the model.

## Not done
- No deploy (staging site or production). The site change shows after the next staging site build.
- No device or browser was driven: the pages are the view models, the hubs the built HTML.

## Owner 2026-11-12
W4 web description: name every region. W1-W3, G11: accept as built. K9: leave.
- DONE W4 (Claude, 2026-11-12): `waterRegions` in `tools/seo/html.ts` names every hub region of a multi-region water in the description ("Regions 2, 3, 5, 7"); one-region waters keep their code ("Region 7A"). Test `tools/seo/description.test.ts`; full SEO build not run. Qualifiers elsewhere still use the one region.

## 2026-11-12 (app wave): item 1b is NOT an app read (the premise was wrong)
The progress note said the release lists every tidal section in `undrawn.json` (`tidal`) and the app could read it. It cannot:
`layers/undrawn.json` is a BUILD-side file of the tile export ("Read by the bundle build", `pipeline/deliver/bundle/undrawn.py`);
the release ships neither it nor any list of them (`manifest.files` has no such file; the released tiles folder holds the
pmtiles, `bc_outside.geojson` and the map assets only; `regs-sid` carries `section_akey` (0 for these) and `section_place`
(none) per section). An unnamed tidal section and a section outside B.C. look the same to the app, so the page still says
"outside B.C." for the 121 (reached only by a link to an unnamed reach, never a map tap). Needs a pipeline change: ship the
fact, for example one bit per section in `regs-sid` (a `section_tidal` block) or the sid ranges in the water store; then the
app reads it where `loadRegsInputs` sees `a === 0` and sets `tidalPart`. Held, pipeline lane. Default if never done: unchanged.
