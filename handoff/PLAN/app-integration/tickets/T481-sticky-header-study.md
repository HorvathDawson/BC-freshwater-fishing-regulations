# T481 — Sticky header on the keep tab: a design study, then "here-max + one breadcrumb" (owner 2026-11-07)

Status: DONE — d2a7b4370. Owner approved the wording 11-07, no shot review (one Vedder pinned-bar shot for overlap only).

Owner: "The sticky header feels hard to read right now. I still like the idea but maybe they need to combine instead into
a single sentence when 2 items sticky. I think we could have same point across with less text. This is a regulatory and
comprehension issue and might need more thorough look at how to display it?" (Vedder River, 390 px, scrolled.)

## What goes wrong today

Two things pin, and they are drawn separately: the **pin bar** (the group line, its "(of N across …)" sub-line, then a
"↳" line per tier you are inside) and the **size box's own header**, which sticks *under* the bar.

1. **Show-through.** There is no gap or fade under the bar, so the line it cuts in half reads as part of it ("…from
   streams / ~~rainbow trout a day (part of the 5)~~"). When a size box ends, its stuck header slides up *under* the bar
   and half of it shows too (see the "Today" column).
2. **Order flips.** The size header always sticks below the bar, but a tier nested *inside* the size box goes into the
   bar, above it. At Big Bend Creek the bar says "↳ of those, Up to 1 brown trout…" and the header under it says
   "Up to 1 over 50 cm". The "of those" then reads as "of the fish over 50 cm". That is a misreading, not a style issue.
3. **Length.** A stack can hold up to 4 items and 222 characters (Andrea Creek). That is a third of a 390 px screen.

## Inventory (scratch release `data/generated/scratch/t479/release`, r-e374b72aa488)

Census: every water × part × date segment × keep group, built through core's `buildRegsPage`, then every root → leaf tier
path the pin chain can stack. Covers 61,240 groups on 18,574 waters, with 0 errors. The most items one water can stack
at once: 1 item on 2,211 waters, 2 on 8,092, 3 on 8,261, 4 on 10. Waters with an "Exception to regional quota": 162.
Waters with a hatchery word in a pinnable line: 2,155 (mostly the "2 if one is a hatchery steelhead" size line).

| waters | what pins | longest today (chars · items) | at |
|---:|---|---|---|
| 7,066 | group (1 line) + size header | 97 · 2 | Blue Danube Swamp |
| 6,669 | group (2 lines) | 60 · 2 | McArthur Island Slough |
| 6,359 | group (2 lines) + size header | 145 · 3 | Quinsam River Diversion Headpond |
| 4,206 | group (1 line) | 81 · 1 | Ulgako Creek |
| 1,462 | group (1) + size header + count | 162 · 3 | Andi Creek |
| 657 | group (2) + own tier | **222** · 3 | Andrea Creek |
| 421 | group (1) + count | 126 · 2 | Limonite Creek |
| 228 | group (1) + own tier | 132 · 2 | Esker Lake |
| 82 | group (2) + own tier, exception | 150 · 3 | Kaslo River |
| 67 | group (1) + size + own | 152 · 3 | François Lake |
| 50 | group (1), head exception (the water's own total) | 55 · 1 | Lois Lake |
| 24 | group (1) + size + own, exception, hatchery | 175 · 3 | Humphrys Creek |
| 5 | group (2) + own, hatchery | 148 · 3 | Coquitlam River |
| 4 | group (2) + own + size | 132 · 4 | Bella Coola River |
| 4 | group (2) + own, exception, hatchery (**the Vedder's**) | 159 · 3 | Stewart Slough |
| 3 · 3 · 2 · 1 · 1 · 1 | group (2) + size + count · group (2) + size + own · group (1) + size + own, exception · group (2) + own + count · group (1) + size + share · group (1) + own, exception | up to **182 · 4** | Big Bend Creek p2 |

**Lifted waters:** no pinnable line has the "+N … under" (`replaces`) form in this release (0 waters). A lift shows up
only as a water's own tier, which the rows above already count.

Every option's words over every stack: `node app/tools/pin-study.mjs words <census.json>` (worst case per option per row).

## The options (mock harness `app/tools/pin-study.mjs`; shots below)

Every study option gets the same 16 px fade under the bar. The size header no longer sticks on its own: whatever pins
is drawn in the one bar. That alone fixes problems 1 and 2.

| | Option | Vedder (exception) | Longest | What an angler could misread |
|---|---|---|---:|---|
| A | **One sentence**: the region total leads; every other number is "up to N" / "no more than N" inside it | "Trout and char: 4 a day across Region 2 — here up to 4 hatchery rainbow trout, and no more than 2 others from streams" | 236 ch (3–4 lines) | Little on the numbers: nothing reads as added on. But it is a paragraph, slow to scan, and "others" has to be read as "trout/char that are not hatchery rainbow". Long species lists make it 4 lines. |
| B | **Number-led**: total on line 1, then a "max N" column | "Trout and char · 4 a day, all of Region 2 / max 4 hatchery rainbow trout (this river's rule) / max 2 of any other from streams" | 219 ch (3–4 rows) | A number column invites adding (4 + 2 = 6 at the Vedder, where the most is 4). It also flattens "of those": at Big Bend, "max 1 over 50 cm / max 1 brown…" loses which fish the second limit is counted from. |
| C | **Top line only**: just the group line ("(of N across …)" inline), plus an "EXCEPTION BELOW ↓" tag on the 162 exception waters; the card's own lines never pin | "Trout and char · 2 a day from streams (of 4 across Region 2) EXCEPTION BELOW ↓" | 91 ch (1–2 lines) | While reading the hatchery rows, the pin says "2 a day from streams". Without the tag, that reads as a cap on the hatchery rainbow too. With size or count limits (13,491 waters), the pin no longer reminds you "only 1 over 50 cm"; the size box's own header is in the card, but it scrolls away. |
| D | **Solid + fade, today's words**: the sub-line inline, the size header drawn inside the bar (left rule), in path order | today's text, one surface | 182 ch (up to 4 rows) | Nothing new: the same words as today, and the order flip (2) is fixed. But it is no shorter, which was the owner's ask. |

**The owner's own example ("2 trout/char a day from streams, plus 4 hatchery rainbow — this river's rule") is wrong at
the Vedder.** "Plus" adds the numbers, which reads as 6 a day. The 4 hatchery rainbow are inside Region 2's 4 a day
("part of the 4 a day across Region 2"), so the most you can keep is 4. Any combined sentence has to lead with the total
and put the parts inside it, as A does. At the Kaslo, "plus" happens to be right (2 Dolly Varden + 2 others = 4, under
5), so the word cannot be chosen per water by hand.

### Recommendation

**C plus the fade, with A's sentence as C's line only on the 162 exception waters.** The pin is there to answer "which
group am I in, and its headline number", and C does that in 1–2 lines. Every detail line, with its full words and Rule
book link, stays in the card right under it. That also removes both overlap causes (one thing pins, nothing slides
under). The one place C's line can mislead is where a water's own number sits outside the share (87 waters: Vedder, Kaslo, Peace …).
There the "EXCEPTION BELOW ↓" tag works, but A's sentence is better, because it states the true composition in one line
of numbers ("4 a day across Region 2 — here up to 4 hatchery rainbow, no more than 2 others"). Second choice: D, the
safest change (fixes the overlap and the order flip) if the owner would rather keep every line.

### What each option would need (the cascade stays in Python)

- **C**: nothing new. The group line and the tiers' `exception` flags are already stored. Core words the tag
  (`WORDS`), PinBar draws one line, and the size header's `useStickyHead` is turned off.
- **A / B**: the harness reads the composition back out of the words: `outShare` ("not part of the N from …"), the share
  it stands outside, and `inTotal` ("part of the N a day across …"). Proposed: answers/tree.py stores them on an own tier
  as `outside_share: {n, from}` and `inside_total: true`, beside T479's `over_region`. Core words the sentence from them,
  with no parsing.
- **D**: no data. The size header moves into the bar (PinBar draws the size line; the box header stops sticking), and
  the bar gets the fade.


## Decision (owner 2026-11-07): "here-max + one breadcrumb" — built

The owner picked a design outside A–D. Built as the default (no flag):

- **One solid bar**, at most 2 lines, path order; 8 px solid padding under the lines, then a 24 px fade (PinBar `PinFade`,
  8 bands, no package). The size header no longer sticks on its own (`stickyHead.{web,native,d}.ts` removed).
- **Line 1, the water's limit** (core `pins.groupLine`; the region total never pins): plain root "Kokanee · 10 a day"; a
  share "Trout and char · 2 a day from streams"; a share plus a water's own tier beside it (`within: "total"`, the T479
  exceptions) "Trout and char · 4 a day — up to 2 of any kind, the rest hatchery rainbow" — `water_max` leads, those
  tiers never pin.
- **Line 2, one breadcrumb** (`pins.breadcrumb`): "↳ " + the levels the scroll is inside, "; of those, " between them; no
  origin clause; a level whose n ≥ its parent's is folded. Size level (coordinator, owner): "↳ 1 of them may be over
  50 cm (20 in) — 2 if one is a hatchery steelhead" (steelhead clause only where the store has `extra`). With a count
  level between line 1 and the size level, "of them" would point at line 1's fish, so it reads "…; of those, 1 may be
  over 50 cm (20 in)". A count inside a size level is counted from line 1 (a size level is no parent: it limits only what
  is over its size), so it follows with "; " — this fixes the Big Bend order flip.
- **Card size wording** (pipeline, the size tier's `says`): "Only 1 of all your trout and char may be over 50 cm (20 in)
  — not 1 per species. 2 if one of them is a hatchery steelhead." By the data: the closed group's name where the tier
  holds every fish that may pass the size, else the fish named; "not N per species" only over more than one fish.
- **The region total, once**: under it in the card head, "Fishing more than one water today? ›" (`GroupHeadVM.multi`) opens
  `multi:r<row>` (core `sheets.multiSheet`, the existing sheet blocks): per angler per day across every water; the
  region's N caps all {group} in {Region} today; the share and any lake share are parts of it; a water's own rule can sit
  outside a share, never outside the total; possession is separate; Source › with the rule book page.
- **Pipeline** (`answers/tree.py`, additive, answers/2): group `water_max` (`tree.water_max`: a tier holds min(n, its
  children together), a size tier its children, the root min(total, …); the Vedder 4 = min(4, 2 + 4)); every tier but the
  total `short` (lower-case clause, no origin clause — fixes "of those, Up to", ~1,878 waters); own tiers `within`
  ("share" | "total") and `fish` ("hatchery rainbow"). Schema regenerated.

**Williston Lake (blocking check): the flag is RIGHT, not a T479 false positive.** Only the Zone B part (wbk:-19) carries
it ("This lake's rule: up to 4 a day (part of the 5)", Dolly Varden 1 + lake trout 3), on every segment where lake trout
are open; Nation Arm, Davis Bay and Zone A (7A) do not. Region 7B's table has "2 lake trout" (`z7b:daily_quotas`
`trout_char_quota.r4`, a subcap of the 5); the lake's own "Lake trout daily quota =3" (`williston_lake_zone_b.r5`) carries
`exempts: trout_char_quota.r4` — the ladder records the subcap displaced by the lake's entry, and 3 > 2. That is exactly
"a quota bigger than the regional quota". No change to `over_region`. (No share on a lake in 7B, so line 1 is the plain
"Trout and char · 5 a day" and the own tier pins as a breadcrumb "↳ up to 4 Dolly Varden/bull trout or lake trout combined".)

Tests: `pipeline/tests/test_answers_t481_pin.py` (water_max on the real shapes + mutation; nothing the bar says sums above
water_max; every tier's short clause; size words); `core/regs/pins.test.ts` (group line, breadcrumb: fold, "of them" /
"of those", no number over its parent), `core/regs/t481.test.ts` (Vedder line + mutations, link, sheet);
`ui-native/regs/T481MultiWater.test.tsx`; PinBar / pinStore / RegsWide over `fixtures/pin-geometry.json` (its group lines
and shorts set to what the new store gives); VM fixtures re-cut (harness); six pipeline tests' size sentences updated.
Rebuild (not promoted): `data/generated/scratch/t481` (from `scratch/t468/reaches/full`), card_check 0 problems.
The study harness `app/tools/pin-study.mjs` is removed (decided; it also tripped the colour gate).

## Found on the way (not fixed here)

- ~~A count tier's short line keeps a capital mid-sentence~~ — fixed by `short` (above).
- A data defect in a "whose" name: "under **lIARD RIVER WATERSHED (see map on page 63)**'s rule" (578 waters, e.g. Esker Lake).
- ~~Williston Lake (Zone B) "Exception to regional quota"~~ — checked: right (above).

## Shots

Contact sheets (cases down, light then dark; options across: Today, A, B, C, D; top 62 % of the phone):
`shots/T481-sticky-options-390.png`, `shots/T481-sticky-options-360.png`. Cases: **vedder-own** (the owner's case,
exception + hatchery), **vedder-size** (the typical 3-item stack, 6,359 waters), **andrea-longest** (222 chars),
**bigbend-deepest** (size + count, where the order flips). The full-size shots, one per case × width × theme × option, are
in the session scratchpad as `sticky/<case>-<width>-<theme>-<option>.png`.

Re-shoot (study only; `pin-study.mjs` is removed, `git show bd29046d1:app/tools/pin-study.mjs`): serve a release (`RELEASE_ROOT=… TILE_PORT=<port> node app/tools/serve-tiles.mjs`), then run Expo web on it
(`EXPO_PUBLIC_DATA_BASE=http://localhost:<port> npx expo start --web --port <web>`), then
`APP_URL=http://localhost:<web> node app/tools/pin-study.mjs shoot <dir> <census.json>` and `… sheet <dir>`. The census is
a one-off vitest pass (not committed): for every `st.item(ord)` × `st.parts(ord)` × `st.segStarts(akey.seg)`, run
`loadRegsInputs` and then `buildRegsPage`, and write per keep group: `pinLine`, `head.form/leadN/exception`, and every
tier's `tid/tier/n/short/line/relation/exception/path`.
Note: the shots were taken on the shared working tree while another lane had size-chip and access-banner edits
uncommitted, so their in-progress look may show in the card.

Size: study S; the pick is S–M (C: core words + PinBar + the sticky switch; A on exceptions: +2 pipeline fields).
