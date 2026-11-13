# T670 — Keep and Gear tabs: show the regulations the release holds and the screens left out (T462 audit K1–K5, G1–G5, G7)

Status: IN PROGRESS — eight findings fixed in the presenters and shown now (738f6608f, no release needed); G2, G5 answered and built; G7 built 2026-11-12 (fab4c0f6f, app only: the release already carries the terms; shows with the next app build); the web pages' copy (`app/tools/seo/model.ts`, T672's lane) still has to draw the two new fields.

Source: `handoff/PLAN/app-integration/T462-audit.md` (85ba2e821), owner 2026-11-11. Rule for this ticket (owner, same
day): a finding is FIXED here only when the screen dropped what the release plainly holds and the book plainly says,
in words the app already uses. A finding that turns on how a rule is read, or that needs new words, is HELD for the
owner with its evidence; nothing was changed for it.

Release measured: LATEST (the one the audit read). Measure: every keyed part of the province (20,763 parts, 19,806
waters) on six days (Jan 15, Mar 15, May 1, Jul 15, Sep 1, Nov 11) = 124,578 pages, through the app's own readers
(`loadRegsInputs` → `buildRegsPage`). "Missing" = held by the release, on no line of the tab.

## Findings

| # | Finding | Status | Where fixed | Missing before → after |
|---|---|---|---|---|
| 1 | **K1** a water's own possession limit (role `possession_cap`) was only inside a fish's sheet | FIXED, shows now | `core/src/regs/standing.ts` `possessionCaps`, `possessionOf` (`also`); `ui-native` `PossessionRow` | 39 rules on 643 waters (658 parts, 3,399 pages) → 0 |
| 2 | **K2** a list row's fish that must go back (the store's `back` condition) read as kept | FIXED, shows now | `core/src/regs/others.ts` `backOf`; `ui-native` `SpeciesRows` | 3 rules on 85 waters (91 parts, 540 pages) → 0 |
| 3 | **K3** a child row's fish nobody may keep was on no line | FIXED, shows now | `core/src/regs/tail.ts` `childNotKept` | 5 rules on 8 waters (17 pages) → 0 |
| 4 | **K4** a pooled row with no stored card was not drawn (Region 8 "20 brook trout from streams") | FIXED, shows now | `core/src/regs/others.ts` `looseRows`, `listRows`; `sheets/other.ts` | with K5: 2 rules on 1,042 waters (1,060 parts, 5,244 pages) → 0 |
| 5 | **K5** the same mechanism, a water's one-kind quota (Duncan River's bull trout 2) | FIXED, shows now | as K4 | counted with K4 |
| 6 | **G1** rules held "from a boat" (`frames.gear.circumstantial`, `while: in_boat`) were read by no screen | FIXED, shows now | `core/src/regs/boats.ts` `inBoatRules`; `methods.ts` `boatsVM` | 9 rules on 81 waters (97 parts, 430 pages) → 0 |
| 7 | **G3** gear and boat rules of an undrawn part (`frames.gear.in_part`) were read by no screen | FIXED, shows now | `core/src/regs/caveats.ts` `gearInPart`, `inPartOf` | 30 rules on 26 waters (32 parts, 219 pages) → 0 |
| 8 | **G4** a hook-size limit (`counts.hook_gap_mm`) was not worded | FIXED, shows now (wording: the rule's stored label; a shorter wording is the owner's, below) | `core/src/regs/gear.ts` `gearLines` | 3 rules on 3 waters (6 parts, 27 pages) → 0 |
| 9 | **G2** the bait heading on a day game fish are closed and the gear tab is drawn for salmon | HELD FOR OWNER | — | 5,156 of 7,932 such pages say "not today" above an all-✕ list (2,705 waters) |
| 10 | **G5** no Licence tab on a day game fish are closed and the gear tab is drawn for salmon | HELD FOR OWNER | — | 20,306 closed pages with a gear answer (8,259 waters); how many have salmon open was not counted |
| 11 | **G7** classified-water licence terms (draw, booking, 8-day cap, one licence a year) are on no tab | BUILT 2026-11-12 (RULINGS Li13), shows with the next app build | `core/src/regs/licence.ts` `bookTerms` (`NeedItemVM.book`); `ui-native` `LicenceAnswer` | the 8 Classified Waters Licence terms ride in the answers already; drawn now under the licence item |

G6 (Pend d'Oreille) is not in this ticket: the owner's reading.

## What each fix shows (exact strings, before → after)

- **K1** Charlie Lake, Jul 15 (p.65 "Walleye daily and possession quotas = 3; northern pike daily and possession
  quotas = 3"). Before: the row "Possession — 2 days’ quota ›" and nothing under it. After, under the row:
  "Possession: the daily quota for Arctic grayling. Fish at home don’t count." · "Possession: the daily quota for
  Dolly Varden or bull trout. Fish at home don’t count." · "Possession: the daily quota for lake trout. Fish at home
  don’t count." · "Have no more than 3 walleye in possession. Fish at home don’t count." · "Have no more than 3
  northern pike in possession. Fish at home don’t count." Every sentence is the release's own (the ones the fish's
  sheet already showed). The first three are not caps: they are the fish the "2 days’ quota" value does not hold
  for, which were also only in the sheet; drawn for the same reason.
- **K2** Peace River, Site C dam to the Halfway closure (p.66 "Mountain whitefish catch and release, including
  tributaries"). Before: "Whitefish · 15 a day". After: the same row with "Release every mountain whitefish." under
  the name, in the release colour (the rule's stored sentence, as its sheet shows it).
- **K3** Shuswap Lake, Nov 11 (p.28 "you must release … Lake trout from Oct 15-Jan 31"; that the zone release holds
  beside the lake's char quota is AGENTS rule 50). Before: no lake trout line. After, in the trout card's not-kept
  lines: "Release every lake trout, Oct 15–Jan 31." — "Region 3 rule · Rule book p.28". Jul 15: no line.
- **K4** Bruer Creek, Jul 15 (p.68 "20 brook trout from streams"). Before: brook trout nowhere. After: first row of
  the species list "Brook trout · 20 a day ›"; its sheet: "Separate from your 4 trout and char", "In streams, keep
  up to 20 brook trout a day."
- **K5** Mat Creek, Jan 15 (p.36 Duncan River "bull trout daily quota = 2 (any size)"). After: first row of the
  species list "Dolly Varden/bull trout · 2 a day ›".
- **G1** Kispiox River (p.51 "No angling from boats, no powered boats"). Before: boats line "No powered boats".
  After: "No angling (from a boat)" · "No powered boats", on the boats line and in the Methods list. The words are
  the rule's stored label (dated ones carry their dates: "No angling (from a boat), Aug 15-Dec 31").
- **G3** Mahood Lake, Jul 15 (p.32). Before: the in-part block held the two trout releases. After, added to it: "Bait
  ban · Jul 1–Dec 31" and "Single barbless hook · Jul 1–Dec 31", each "Where: Within fishing boundary signs at the
  western tip of the lake" / "Where: Within 200 m of the mouth of the Mahood River outlet". Cowichan Lake (p.15): a
  block "Boat rules in some spots" — "Speed restriction (10 km/h)", "Where: Within 60 m of shore".
- **G4** Qualicum River (p.18 "Single barbless hook, no hooks greater than 15 mm from point to shank"). Before: Hooks
  "One hook, lure or fly" · "Single barbless hook, no trebles". After: a third value, the rule's stored label,
  "Single barbless hook; no hook more than 15 mm from point to shank". Chilcotin (p.43): "No hook more than 15 mm
  from point to shank, Oct 1-May 31".

## Held for the owner (nothing changed)

**G2 — bait heading on a closed day.** Cowichan River, mouth to the white triangle sign, Jul 15. Book p.13: "Bait ban:
applies to all streams of Region 1, all year, with some important exceptions." Release: gear frame `bait_ban: true`
by `z1:bait_ban_streams.r1`; the year strip carries the bait-ban mark only on days game fish are open (Jan 1–Jul 3,
Nov 16–Dec 31) and "closed" alone on Jul 4–Nov 15. Screen today (gear tab drawn for salmon): "No natural bait Jan
1–Jul 3, Nov 16–Dec 31 · not today" above a list that is all ✕. Readings: (a) keep the dates, say "in force today"
whenever the day's frame bans bait; (b) say "No natural bait — all year" when the rule is undated (dates then come
from the rule, not the strip: needs the rule's dates in the frame, a release change); (c) leave as is. 5,156 pages
on 2,705 waters read "not today" above an all-✕ list in the six sampled days.

**G5 — Licence tab on a closed day with salmon open.** Somass River, tidal boundary to the signs 1 km above the dam,
Nov 11 (closed for game fish; Gear and Methods are drawn). Owner 2026-11-04: on a fully closed page the Licence
header disappears. If the tab were drawn as on an open day it would say: "You need: Basic angling licence — Needed
to fish at all"; "Also, only if…: Steelhead Conservation Surcharge Stamp — Only if you fish for steelhead, even to
release them; Conservation Surcharge Stamp for salmon — Only if you keep salmon (not kokanee)"; the paper-licence
line. Whether the as-open licence answer is right on a day game fish are closed (a steelhead stamp line on a closed
steelhead water, a classified-water licence while only salmon is open) is a reading. 8,259 waters have a closed day
with a gear answer.

**G7 — classified-water licence terms.** p.7. The release's licensing table holds 26 `licence_terms` records, all
`not_placed`, none in a licence frame. The ten the audit names: Dean River (93 no day limit for Canadian residents;
94 "A non-guided Non-Resident Alien wishing to fish the Class I - Main Section … must enter an annual limited entry
draw held each spring"; 95 and 105 one licence a year, one section, 8 days); 101 "A first-come-first-serve booking
system is in place for these Kootenay Class II waters: Michel Creek, Skookumchuck Creek, and the Wigwam River"; 102
and 103 non-residents: sold by the day, date and water-specific, at most 8 consecutive days; 104 residents: valid
for the year on every classified water; 108 disabled fee; 109 one annual licence a year. Screen today (Wigwam River,
non-resident): "Classified Waters Licence — Needed while it’s classified: class II whenever this water is open —
$22.86 a day"; no booking, no 8-day cap. Which terms the tab owes, to which angler, on which water is a choice; the
build is in the Python licence frame (`pipeline/deliver/answers/licence.py`) and needs a release.

## Owner wording choices on the fixed ones (defaults taken: words the app already had)

- G4: the Hooks value is the rule's whole label, so "Single barbless hook" is said twice on the Qualicum. A value of
  its own ("No hooks greater than 15 mm from point to shank", the book's words) is new wording.
- G1: the chip says the rule's label, "No angling (from a boat)"; the book prints "No angling from boats".
- K4/K5: the rows sit at the top of the species list, which the non-glance layout heads "Other fish".
- K3: the line reads as the rule's sentence ("Release every lake trout, Oct 15–Jan 31."), not in the stored not-kept
  form ("Lake trout: release every one"); the Python `card._not_kept` reads the card's own row only and could carry
  it at the next release.

## Tests

- `app/conformance/src/regs/t670.release.test.ts`: one water per class on the release under test, named by water and
  part ends, whole strings pinned (K1–K5, G1, G3, G4).
- Unit: `standing.test.ts` (K1, with mutation), `others.test.ts` (K2, K4/K5), `keep.test.ts` (K2 key), `boats.test.ts`
  (G1, with mutation).

## Goldens re-cut (`UPDATE_REGS_FIXTURES=1 pnpm -C app exec vitest run conformance/src/regs/harness`)

Ten frozen pages under `app/packages/ui-native/src/regs/fixtures/`: every one gains the two new fields empty
(`also: []`, `back: []`); `x35p_1010.json` alone changes words — its possession row now carries its water's own
possession limits (Bennett Lake, p.50: "Lake trout daily and possession quotas = 2 …; Arctic grayling daily and
possession quotas = 4 …; northern pike daily and possession quotas = 4"), the K1 fix.

## Not done

- The web water pages (`app/tools/seo/model.ts`) read the same view model and do not draw `possession.also` or
  `otherFish[].back` yet; that file is in T672's lane today.
- G1 on the "Ways to fish" list: "Angling ✓" is unchanged; the ban from a boat is on the boats line and the Methods
  boats list only.
- No device or emulator was driven; the new lines were read as view models and by the render tests.

## Owner 2026-11-12
G2 bait heading: "Why does it matter the river is closed? It shouldn't show any method or gear or quotas in the regulation card. If salmon are open or something at same time so that river is half closed then it should say in force." A fully closed water shows no gear, method or quota; where something is open the rule reads "in force today". G5 Licence tab: shown when salmon is open. K1-K5, G1, G3, G4 wordings: accept as built. G7 (which classified-water licence terms the Licence tab carries) is still the owner's.
- DONE in the app (Claude, 2026-11-12). G2: gear showed on the closed day because SALMON was open (`gearTabsFor` draws the open day's gear for salmon); the bait heading read the strip's bait mark, which exists only on game-fish-open days, so an all-year ban said "not today". Now `banHeading(…, fished)`: on that salmon-only day a dated ban the day's frame holds reads "in force today" (`bait.ts`). A page closed for game fish AND salmon already draws no Gear/Methods tab (and no quota). G5: `licenceTabFor` / `licenceIfFished` (`regs/page.ts`, `WaterScreen.tsx`): the Licence tab shows when salmon is open, hidden only on a fully closed page. Tests: `bait.test.ts`, `licenceTab.test.ts`. Held: a closed page whose salmon status is "check DFO"/unknown counts as salmon open (as the Gear tab already does).


G7 decision (owner 2026-11-12, "Show on each water"): each classified water's Licence tab lists the p.7 terms that apply to it, in the book's words. No longer held. RULINGS Li13.


## G7 built (Claude, 2026-11-12, fab4c0f6f)
- Measured first: the terms ARE in the licence frames already. `licence.documents` hangs every `licence_terms` record
  without a fee whose `who` matches the angler and whose class / units match a designation in force on the requirement
  that needs the document (`requirements[].terms`). On r-0a9930fb8826 the 273 shipped profile answers carry them:
  cwl_resident 27, cwl_non_resident 27, cwl_non_resident_guided 24, dean_canadian_no_day_limit 10, dean_nra_limits 5,
  dean_nra_one_licence 5, cwl_booking 2, dean_class_i_main_draw 1 (and the basic licence's basic_one_per_year 261,
  basic_disabled_fee 39). Nothing in the app drew them. So no pipeline change and NO RELEASE is needed: the current
  release shows them as soon as an app (APK / web) is built from fab4c0f6f or later.
- App: `bookTerms` reads the Classified Waters Licence's terms of the angler's answer into the "You need" item's `book`
  (the record's `verbatim`, once, answer order; a term the answer names that the release lacks throws); `LicenceAnswer`
  draws them under "From the rule book:". The golden line (`needLine`) includes them.
- Wigwam River, non-resident: "Classified Waters Licence … $22.86 a day", then "From the rule book:" + the Kootenay booking
  sentence + the non-guided per-day / 8-day sentence. A resident: the annual-licence sentence, no booking. The Elk (Class
  II, not booked): the per-day sentence only.
- Tests: pipeline `test_answers_licence.py::test_li13_*` (who / class / unit, mutations); core `licence.test.ts` (whole
  line, mutation, missing record); `methods-licence.render.test.tsx` (the Elk per angler; no term on the Vedder);
  `conformance/src/regs/t670.release.test.ts` G7 (Wigwam, the Elk). Ten regs goldens re-cut: `book: []` everywhere, the
  Elk's licence fixture gains its two sentences.
- HELD FOR THE OWNER: the basic licence's two administrative terms ("You are only permitted one annual angling licence per
  year." on every water's basic licence; the disabled-resident $1.14 fee) are in the answers too but are not p.7's
  classified-water terms, so they are not drawn. Default: leave them off (Li13 names classified waters).
  **ANSWERED 2026-11-12 (owner, dialog, "Show the fee only"): show ONLY the disabled-resident fee line; the one-licence line stays off. BUILT (Claude, Sonnet 5.5):** `core/src/regs/licence.ts` `bookTerms(.., feeP)`: on the basic licence "You need" item, a B.C. resident adult sees the book's sentence "If you are a resident of B.C. and disabled: You may qualify for a basic freshwater angling licence at the reduced fee of $1.14." under "From the rule book:". The app does not ask whether the angler is disabled, so the line comes from the stored answer of the DISABLED resident adult (profile `resident/16_plus/non_guided/disabled`: the plain profile's answer omits the term, the pipeline selects terms by `who.status`); the record is picked by data (a `licence_terms` record of `basic_licence` carrying `fee_cad`), never by id, and `basic_one_per_year` is never drawn. Non-residents and under-16s: none. A named record the release lacks throws. Tests (fail before): `licence.test.ts` (resident gets the fee line only, whole `needLine`; non-resident none; no-term mutation; missing record), live `t670.release.test.ts` G8 (the Elk), web `t670.web.release.test.ts` G8 (model.ts already draws `needs[].book`, no code change), 13 regs goldens re-cut (`UPDATE_REGS_FIXTURES=1`). RULINGS Li13 extended.
- Web water pages DONE 2026-11-12: `app/tools/seo/model.ts` Licence section now carries each "You need" item's `book` terms (the visible page already drew them: it is the app's pane prerendered; the text model, hubs and content hash did not). Test `tools/seo/t670.web.release.test.ts` (G7 Wigwam River case + the guard reads `needs[].book`; fails before). Basic-licence fee line: see above (owner 2026-11-12, "Show the fee only").
