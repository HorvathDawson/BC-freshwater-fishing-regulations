# T481 — Sticky header on the keep tab: a design study (owner 2026-11-07)

Status: IN REVIEW — design study, options for the owner to pick from; nothing shipped, today's default unchanged.

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

## Found on the way (not fixed here)

- A count tier's short line keeps a capital mid-sentence: "↳ of those, **Up** to 1 brown trout, …" (1,878 waters).
- A data defect in a "whose" name: "under **lIARD RIVER WATERSHED (see map on page 63)**'s rule" (578 waters, e.g. Esker Lake).
- Williston Lake (Zone B) carries "Exception to regional quota" on "4 of them under this lake's rule" (4 of 5). Check
  whether it is over a regional subcap (lake trout?) or a false flag.

## Shots

Contact sheets (cases down, light then dark; options across: Today, A, B, C, D; top 62 % of the phone):
`shots/T481-sticky-options-390.png`, `shots/T481-sticky-options-360.png`. Cases: **vedder-own** (the owner's case,
exception + hatchery), **vedder-size** (the typical 3-item stack, 6,359 waters), **andrea-longest** (222 chars),
**bigbend-deepest** (size + count, where the order flips). The full-size shots, one per case × width × theme × option, are
in the session scratchpad as `sticky/<case>-<width>-<theme>-<option>.png`.

Re-shoot: serve a release (`RELEASE_ROOT=… TILE_PORT=<port> node app/tools/serve-tiles.mjs`), then run Expo web on it
(`EXPO_PUBLIC_DATA_BASE=http://localhost:<port> npx expo start --web --port <web>`), then
`APP_URL=http://localhost:<web> node app/tools/pin-study.mjs shoot <dir> <census.json>` and `… sheet <dir>`. The census is
a one-off vitest pass (not committed): for every `st.item(ord)` × `st.parts(ord)` × `st.segStarts(akey.seg)`, run
`loadRegsInputs` and then `buildRegsPage`, and write per keep group: `pinLine`, `head.form/leadN/exception`, and every
tier's `tid/tier/n/short/line/relation/exception/path`.
Note: the shots were taken on the shared working tree while another lane had size-chip and access-banner edits
uncommitted, so their in-progress look may show in the card.

Size: study S; the pick is S–M (C: core words + PinBar + the sticky switch; A on exceptions: +2 pipeline fields).
