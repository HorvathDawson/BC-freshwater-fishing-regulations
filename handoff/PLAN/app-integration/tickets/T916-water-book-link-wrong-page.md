# T916 — "This water in the rule book p.7" on Sweltzer River points at the wrong page

Status: IN PROGRESS — owner OK (round 1 + the foot placement), merged into the 0.8.3 integration (app/int-083); left (Claude): ship with 0.8.3 (the release carries `row`).

Owner 2026-11-19 ~12:55 (main chat, phone screenshot of Sweltzer River, Regulations, What you can keep, 0.8.2 on
staging), verbatim: "Has page 7 link at top (which is wrong) with real page 26 at bottom. Should remove top one...".
Screenshot: scratchpad (session 1db4ea05) t916-owner-sweltzer.png. The top line "This water in the rule book p.7 ↗"
(`waterBookOf`, core regs/waterbook.ts, T474) sits above the Salmon/Game fish cards; the page's own rules link p.26
("Closed to all fishing by the rule book (p.26)", "Rule book p.26 ↗").

## Work
Find which rank-0 rule on Sweltzer River carries printed page 7 in `facts.book` (`display.decided.book_pages`), and
why: wrong data (an entry's pages wrong, or a general/regional rule sitting at rank 0), or the line's own logic. Fix
it where it is wrong (Python data / the rank, or the app line), never by hiding the line for this one water. Count every
water in the live release where the line names a page that is not one of that water's own entries' pages, before and
after. Pictures 390 light/dark before/after.

## Owner 2026-11-19 ~13:20 (main chat, after the before/after pictures)
- Asked "Put it in 0.8.3?", verbatim: "Ye sput to n 0?8.3 it was also the location that was awkward at the top". So yes,
  in 0.8.3, and the line's place at the top of the page (above the first status card) is awkward too: round 2 moves
  it down (pictures to the owner first).
- Asked whether waters named only inside a zone rule (Rubble Creek p.21, Horne Lake p.13, the p.4 youth waters) should
  get the line back, verbatim: "Keep it simple (Recommended)": the line is only for a water's own table row; zone-rule
  pages stay linked on the rules themselves.

## Cause (lane, 2026-11-19)
Printed p.7 (PDF p.9) is the 2025-27 synopsis's LICENCES page: conservation surcharge stamps (salmon, Kootenay Lake
rainbow, Shuswap), the White Sturgeon Conservation Licence, Classified Waters licences, angling guides. Sweltzer River's
part reaches `zp:white_sturgeon_licence` r2 and r4x (B.C. table, "Fraser watershed, Mission to Williams Lake River",
pages [7]), bound to the Fraser watershed (`province/water`). The ladder puts EVERY water-bound rule at rank 0
(`codec.rank`: `province/water` = `region/water` = 0), so `waterBookOf` read them as this water's own entry. Its own row
(`r2:sweltzer_creek@2-3`, p.26) is linked by its rules, so T60's dedupe left only p.7. Not wrong data, not a wrong rank
(the rank is the ladder's and stays): the line's test "rank 0 = this water's own entry" was wrong.

## Fix
- Python `display.decided.add_sources`: a rule whose entry is a water table's row (export entry kind "water", the same
  fact `applies_here` words as "This water's own row in the ...") carries display fact `row: true`
  (`model/display.py` `RuleFacts.row`, optional within display 20 as T892 did; `answers.generated.*` regenerated).
- Core `waterBookOf`: rank 0 AND `facts.row`. A release without the fact has no line (never a province page).
- No curated file changed. Mini release re-cut (`mini_release --write`: water files gain `row`). The regs fixture
  (`app/packages/data/dev/regs-release`) is NOT re-cut here: the release re-cuts it.
- `WaterScreen.regs-props.test.tsx` (T474) had pinned this bug: Chilliwack part 3's line was "p.7"; now no line (its row
  p.22 is linked by its rules).

## Count, live release r-379e72fc9524 (every water, every part, page built, T60 dedupe applied)
Waters whose line names a page that is none of the water's own entries' pages: before 5,533, after 12.
Before by page: p.7 3,939 (white sturgeon licence; Shuswap/Kootenay stamps), p.42 1,796 (`z5:white_sturgeon`), p.49 594
(`z6` Skeena/Nass winter closure, Iskut, steelhead streams), p.4 20 (`zp:youth_disabled_waters`), p.21/64/13/6/8/68/34 a
few each. Waters with any line: 6,190 before, 1,042 after; no water's own page was dropped.
After (12): other waters' ROWS bound here at rank 0, e.g. Williston Lake arms -> p.61 (Zone A Williston row), Columbia
River -> p.40 (Upper Arrow drawdown), Irishman Creek -> p.38 (Moyie River), Campbell River -> p.16 (John Hart Lake's
tribs), Caribou Creek slough -> p.35. These are the book's rows that do cover the water; left as they are.
Lost on purpose (owner may want some back, follow-up): zone/B.C. lines that NAME one water, e.g. Rubble Creek p.21
(`z2:rubble_creek`, no row of its own), Horne Lake p.13 (cutthroat tagging), Stave River p.21, Garnet Lake p.68, the
youth/disabled waters p.4. Their rules still link those pages on the page where they apply.

## Release must
Rebuild answers + store (display_rule gains `row`); re-cut the regs fixture on the candidate (the release does). Until a
release carries `row`, an 0.8.3 app shows no "This water in the rule book" line anywhere.

## Tests
Failing first: `pipeline/tests/test_t916_water_row.py` (KeyError 'row'), `waterbook.test.ts` T916 cases (2 failed).
Then: pytest t916 + answers_display/model/versions + mini_release/mini_deliver_inputs + t100 all passed (77 + 20; bundle tests
against the main checkout's bundle); vitest core/regs, ui-native regs, WaterScreen regs-props, data drivers/regs,
conformance/regs: 2281 passed (fixture), and 2472 passed with the live release.
Pictures (390, Sweltzer, Regulations top): scratchpad t916-shots/{before,after}-390-{light,dark}.png (after = scratch
copy of the release with `row` patched in, `t916/patch_release.py`).

## Round 2 (lane, 2026-11-19 13:28): the line moves to the page's foot
- Built: "This water in the rule book p.N ↗" is the last line of the page's foot block (`regs-legal`), under "Not legal
  advice — the B.C. synopsis and DFO notices are the authority." and "Regulations as of …", just above "Report a problem
  with this page"; small accent type like that link. Why: it is a reference for the whole page (the synopsis the foot
  already names as the authority), not a fact about today, so it no longer sits above the first status card, and the
  page's two quiet links end up together. One place per page; T474/T60 dedupe unchanged (`waterBookOf` untouched).
- The SEO water pages render the app's own Shell (`tools/seo/pane.tsx`), so they move with the app; no second template.
- Alternative pictured, not built: the end of "Species and limits" (after "Where you can't fish", before Gear).
- Tests: `WaterBook.test.tsx` pins the foot (failed on the old placement first); vitest ui-native regs + WaterScreen +
  core waterbook + tools/seo 773 passed (12 skipped: live release); `pnpm -C app check:fast` passed.
- Pictures (Elaho River, gnis:10716, own row p.23; patched scratch release): scratchpad t916-shots/
  r2before-{390-light,390-dark,1440-light}.png, r2after-{390-light,390-dark,1440-light}.png (scrolled to the foot),
  r2alt-end-of-keep-390-light.png.
- Seen, not changed (dedupe kept as is): on Elaho the Gear section's own sources also link p.23; `waterBookOf` reads only
  the Regulations page model, not the Gear section's links, so p.23 shows twice on that page.
