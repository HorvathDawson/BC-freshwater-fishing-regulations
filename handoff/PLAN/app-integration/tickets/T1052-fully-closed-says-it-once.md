# T1052 — A water closed to all fishing says it twice (a Salmon row and a game-fish "Closed all year" block)

Status: IN REVIEW — owner: OWNER-TODO "T1052 fully closed water: one statement" (the row-line words are a proposal; part-year case). Code 33ecb6b96 (lane L22); ships with the next app build, no data release.

Source: in-app report `19a865256d6dd70d` (staging feeds worker, the owner, 2026-11-24 18:47 UTC), reason "other", on
Nelson Creek `gnis:20408`, part 0, "Today, Nov 24", Regulations view, web, app 0.9.0 (build 43, 127bf43), release
r-0aec88ed3edb. No screenshot attached. The words, verbatim:

> the closed tab herew with seperate salmon and game fish when fully closed seems weird should just say all closed including salmon and give explaination rathe rthen multiple headers maybe etc etc just has a lot of words the 2 schedule calendars are okay though

Triage: lane L19, 2026-11-24. Picture (staging, 390 px): `handoff/PLAN/app-integration/T1052-shots/report-now.png`.
Eagle Creek (`gnis:9405`, p.23) draws the same page.

## What the screen shows

Two status strips ("SALMON · B.C. SYNOPSIS ✕ Closed", "GAME FISH · B.C. ✕ Closed": the owner keeps these), then
"Species and limits" with one row, "Salmon closed" + "Closed to all fishing by the B.C. Synopsis (p.25): No Fishing (all
year)" + "B.C. Synopsis p.25 · Nelson Creek", then a second block "Closed all year" + "No fishing for any game fish. This
water" + its source line. One closure (`r2:nelson_creek@2-8::nelson_creek.r1`, book p.25: "NELSON CREEK ✱ No
Fishing") is said twice, in two vocabularies, under two headings.

## Cause

Two composers each say their half: the salmon row's words come from the salmon facts (`app/packages/core/src/salmon.ts`
`bookToday`, ~line 1476, T796: "Closed to all fishing by the rule book (p.N): …", from `pipeline/deliver/salmon.py`
`BOOK_WORDS`), and the closed day's banner from `app/packages/core/src/regs/banner.ts` (`WORDS.CLOSED_ALL_YEAR`,
`NO_FISHING_RUN`, `words.ts:75-77`). Nothing joins them when every fish, salmon included, is closed by the same rule.
Rule 23: the closed statement must come from ONE function in core.

## What is right

The book closes everything here ("No Fishing", ✱ tributaries; p.4 key: the ✱ means the regulation applies to tributary
streams too); the page should say so once, salmon included, with the reason and the page. The words are the owner's.

## Options

- **A (recommended):** keep the two strips; in place of the Salmon row and the "Closed all year" block, one block:
  heading "Closed to all fishing all year, salmon included"; line "The B.C. Synopsis row for Nelson Creek (and its
  tributaries) reads “No Fishing”."; then the source line "B.C. Synopsis p.25 ↗ · Who says so ›". On a walked tributary
  the line names the row it inherits ("Nelson Creek's row covers its tributaries: “No Fishing”.").
- **B:** smallest change: drop the Salmon row's sentence and make the banner "Closed all year" / "No fishing for any
  fish, salmon included. This water".
- **C:** A plus the two strips merged into one "All fish" strip (the owner said the two calendars are fine: not recommended).

Only when the same closure(s) close salmon and every game fish on the day shown; a page closed for one group keeps today's look.

## Work (after the pick)

1. One core function composes the all-closed statement (rule 23, sentences in core, rule 33 whole-string tests); the
   salmon row and the banner ask it first. Check whether core can tell "the same closure" from the frames it has; if not,
   the display frame carries a flag (then a data release).
2. Tests (Nelson Creek, Eagle Creek, a tributary of Nelson, a page with only salmon closed) as whole strings, mutation-pinned;
   before/after shots light + dark.

Size: M. Needs: owner pick; app build (a data release only if a frame flag is needed). Safe for 0.9.1.
Depends: none. Owner: yes (the words). Default: A, built only once picked.

## Owner's answer (2026-11-24 ~12:33 MST, verbatim)

Question: "Nelson Creek: a water closed to all fishing says it twice, once in the Salmon row and once in the 'Closed all
year' block. How should it read?" Answer: **"One block, both calendars (Recommended)"** (the option's text: keep the two
calendars; replace the two statements with one block: "Closed to all fishing all year, salmon included", with the book's
"No Fishing" line and its source). Earlier he had said the two calendars are fine.

## Built (lane L22, 33ecb6b96)

- Core `closedWithSalmon(page, card, rules)` (`core/src/regs/withSalmon.ts`), called once in `WaterScreen`'s `shown`
  (the one load path). It merges ONLY where the data says it is one closure: the salmon card's `todayBy` is "book" (every
  species on every chain answered today by the rule book's places, T796 A5), the book closures' rule keys (`entry::rule`)
  equal the banner chain's, each is all year in the release's `static` year (`bookAllYear`), the banner has no reopening,
  no merged-stretch fold, no `facts.unless_opened`, and the card has no DFO notice, finfish closure, undrawn or one-bank
  place. No text is matched. New card fields: `todayBy`, `bookAllYear` (salmon.ts).
- The banner then says the owner's headline and one line per closure; the Salmon row (and, without the glance table,
  the card) is not drawn (`BannerVM.salmon`); both calendars stay; the Source line is the closure's own.
- Line words (PROPOSALS, not the owner's): own row "The B.C. Synopsis row for Nelson Creek reads “No Fishing”."; walked
  tributary "Little Qualicum River’s row covers its tributaries: “No Fishing - All tributaries”." (the book line as
  `describe` cleans it; no second full stop after a line ending its own sentence). Each line keeps the level the banner
  gave it ("This water", "From downstream", "Federal / parks", "Named area").
- Kept as two statements (by design): part-year closures; salmon by DFO or by another rule (two sources, neither
  dropped); "unless opened" park closures; "closed all year on N stretches" banners.

Live bundle r-0aec88ed3edb, Nov 24 2026, the release's salmon (no live feed): 20,680 page parts, 1,534 closed; **936
parts on 936 waters get the one block** (555 of them on a walked tributary, 176 on several-part waters), e.g. Nelson
Creek, Eagle Creek, Crocker Creek, Brunette River (part 2), Kakwa River (part 1). Not merged among closed-all-year: 181
"unless opened" (National Parks), 107 merged-stretch banners, 7 salmon by another book rule (Still Creek: Brunette's
row), 5 salmon by DFO (Skagit River part 1), 1 salmon closure not all year (Kirkup Creek), 1 card with a DFO place
(Babine Lake east). Closed part of the year: 296, of which 130 by the same book rule (kept as two statements: a wording
for them is an owner question).

Pictures (web, 390 px): `T1052-shots/nelson-before-{light,dark}.png`, `nelson-after-{light,dark}.png`, the not-merged
two-source case `skagit-two-sources-{light,dark}.png`.

Tests: `core/src/regs/withSalmon.test.ts` (decision table, whole strings), `ui-native/src/WaterScreen.oneClosure.test.tsx`
(fixture release + core's salmon card: one block, tributary, several-part water, two sources, another rule, part year,
in-season notice on top, no glance, no salmon); `salmonBook.test.ts` pins `todayBy`/`bookAllYear`. 12 mutants, all killed.

Owner questions left: (1) the row-line words above; (2) a water closed to all fishing for PART of the year by one rule
(130 parts today, e.g. Buckley Creek Nov 1–Apr 30): keep two statements (default) or a merged headline such as "Closed
to all fishing until Apr 30, salmon included" (proposal).

## Review round 1 (#131, 2026-11-24)

- MED 1 (never drop a source): a merge now also needs every salmon closure to be one the banner cites (salmon ⊆ banner
  and banner ⊆ the salmon's all-year closures); pinned by "two all-year closures, the banner cites one" (core test).
  `banner.ts` (the closer list, T1078) is untouched.
- MED 3: the line keeps its level label (merged parts: From downstream 555, This water 213, Federal / parks 145, Named
  area 23).
- LOW 5: the `page.status`, `b.kind` and empty-chain guards are gone (only a closed day's banner carries a chain; the
  tidal banner's is empty, and the two rule sets must be equal and non-empty); new cases pin a tidal page, a closed day
  naming no closure, and a card whose all-year list names a closure it does not carry. 9 new mutants, all killed.
- LOW 6: the Source links name the row for a screen reader: "Who says so: the rules behind the Nelson Creek row",
  "B.C. Synopsis page 25, the passages behind the Nelson Creek row marked, opens the PDF" (asserted in the WaterScreen test).
- MED 2, THE REAL TEXTS (936 merged parts): own row reading exactly "No Fishing" 122; own row with another line 259
  (National Parks and Ecological Reserves 76, National Park Reserves 68, UBC Malcolm Knapp lakes 16, Whiteswan inlet and
  outlet streams 15 …); walked tributary with a bare "No Fishing" 163 (Brunette 74, Fording 13, Tsolum 11 …); walked
  tributary quoting another line 392, mostly the mainstem's own extent: Brunette 94 ("No Fishing upstream of Burnaby Lake
  or from Cariboo Dam to Salamander Creek"), Squamish 88 ("No Fishing tributaries EXCEPT: …"), Bulkley 34, Puntledge 22,
  Capilano 22, Sustut 16, Chilliwack 13. Pictures (390 px light, three real cases side by side): option A as built
  `T1052-shots/wordings-A-light.png` (Nelson Creek own row, Pup Creek a tidy Tsolum tributary, Crayfish Creek a Brunette
  tributary with the long extent line); option B, a proposal not built, `T1052-shots/wordings-B-light.png`: a walked
  tributary says the pipeline's own `facts.applies_trib` words "Inherited from Brunette River: its row covers its
  tributaries." with no quote (the line stays one tap away under Who says so); own rows as A. The owner picks.
- `nelson-after-light.png` is re-shot with the level label; `nelson-after-dark.png` predates it (no "This water").

Left (Claude): a brief two-statement frame when stepping a part or a day (the page draws before the day's salmon card
lands, then merges). Not fixed here: a fix would hold the closed banner until the card is in, which touches the T1049
"coming" wait; a follow-up for the planning chat.
