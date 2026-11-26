# T1052 — A water closed to all fishing says it twice (a Salmon row and a game-fish "Closed all year" block)

Status: IN REVIEW — owner: see ## Owner (the row-line words are a proposal; part-year case). Code 33ecb6b96 5e2ca0048 (lane L22); ships with the next app build, no data release.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1052 | A water closed to all fishing says it twice (a Salmon row and a game-fish "Closed all year" block; Nelson Creek) | IN REVIEW — owner: see ## Owner | 33ecb6b96 5e2ca0048 | owner picked A 11-24 12:33; 936 waters merged; line words (A or B pictured) + part-year case are owner questions; lane L22 |

Source: in-app report `19a865256d6dd70d` (staging feeds worker, the owner, 2026-11-24 18:47 UTC), reason "other", on
Nelson Creek `gnis:20408`, part 0, "Today, Nov 24", Regulations view, web, app 0.9.0 (build 43, 127bf43), release
r-0aec88ed3edb. No screenshot attached. The words, verbatim:

> the closed tab herew with seperate salmon and game fish when fully closed seems weird should just say all closed including salmon and give explaination rathe rthen multiple headers maybe etc etc just has a lot of words the 2 schedule calendars are okay though

Triage: lane L19, 2026-11-24. Picture (staging, 390 px): `docs/app-integration/T1052-shots/report-now.png`.
Eagle Creek (`gnis:9405`, p.23) draws the same page.

## What
One closure (`r2:nelson_creek@2-8::nelson_creek.r1`, book p.25: "NELSON CREEK ✱ No Fishing") was said twice under two
headings: the Salmon row (`core/src/salmon.ts` `bookToday`, from `pipeline/deliver/salmon.py` `BOOK_WORDS`) and the
banner's "Closed all year" block (`core/src/regs/banner.ts`, `words.ts`). The two status strips stay (the owner keeps
them). Rule 23: the closed statement must come from ONE function in core.

## Owner's answer (2026-11-24 ~12:33 MST, verbatim)

Question: "Nelson Creek: a water closed to all fishing says it twice, once in the Salmon row and once in the 'Closed all
year' block. How should it read?" Answer: **"One block, both calendars (Recommended)"** (the option's text: keep the two
calendars; replace the two statements with one block: "Closed to all fishing all year, salmon included", with the book's
"No Fishing" line and its source). Earlier he had said the two calendars are fine.

## Done (lane L22, 33ecb6b96 5e2ca0048; review round 1 #131)
- Core `closedWithSalmon(page, card, rules)` (`core/src/regs/withSalmon.ts`), called once in `WaterScreen`'s `shown`. It
  merges ONLY where the data says it is one closure (`todayBy` "book", rule keys equal, each all year by `bookAllYear`, no
  reopening / merged-stretch fold / `facts.unless_opened` / DFO notice / finfish closure / undrawn or one-bank place; every
  salmon closure one the banner cites). No text is matched. The banner says the owner's headline and one line per closure
  at its level ("This water", "From downstream", "Federal / parks", "Named area"); the Salmon row is not drawn; both
  calendars stay. Screen-reader Source links name the row.
- Line words (PROPOSALS, not the owner's): own row "The B.C. Synopsis row for Nelson Creek reads “No Fishing”."; walked
  tributary "Little Qualicum River’s row covers its tributaries: “No Fishing - All tributaries”.". Option B, not built:
  "Inherited from Brunette River: its row covers its tributaries." (no quote).
- Live bundle r-0aec88ed3edb, Nov 24: **936 parts on 936 waters get the one block** (555 on a walked tributary). Real
  texts: own row exactly "No Fishing" 122, own row another line 259, walked tributary bare 163, walked tributary quoting
  another line 392 (Brunette 94, Squamish 88 …). Kept as two statements by design: part-year closures (296, 130 by the
  same book rule), salmon by DFO or another rule, "unless opened" park closures, "closed all year on N stretches".
- Tests: `core/src/regs/withSalmon.test.ts`, `ui-native/src/WaterScreen.oneClosure.test.tsx`, `salmonBook.test.ts`; 21
  mutants, all killed. Pictures `T1052-shots/` (`nelson-before-*`, `nelson-after-*` — `nelson-after-dark.png` predates the
  level label —, `skagit-two-sources-*`, `wordings-A-light.png`, `wordings-B-light.png`).

## Left
- Owner: the row-line words (A as built or B), see ## Owner.
- The 130 waters closed for part of the year by one rule: owner "Yes, merge those too" — a follow-up PR, pictures first.
- (Claude) a brief two-statement frame when stepping a part or a day (the page draws before the day's salmon card lands,
  then merges). A fix would hold the closed banner until the card is in, which touches the T1049 "coming" wait; a
  follow-up for the planning chat.

Size: M. Needs: owner pick; app build (a data release only if a frame flag is needed). Safe for 0.9.1.
Depends: none. Owner: yes (the words). Default: A, built only once picked.

## Owner

- [ ] 2026-11-24 — Check the line under the closed-water headline as built (A: quote the river's row; `docs/app-integration/T1052-shots/nelson-after-*.png`, `wordings-A-light.png`) and keep it or take B (a tributary says "Inherited from Brunette River: its row covers its tributaries.", `wordings-B-light.png`).
- [x] 2026-11-24 — Fully closed water, one statement → "One block, both calendars (Recommended)"; second line → "A: quote the river's row"; the 130 waters closed for part of the year → "Yes, merge those too" (2026-11-24; the last is a follow-up PR, pictures first).
