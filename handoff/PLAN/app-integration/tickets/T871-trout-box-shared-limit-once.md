# T871 — Trout table: when every row "counts toward the 2", say it once at the top of the box

Status: DROPPED (owner 2026-11-18 ~20:53: "Not this change"; the pictured proposal stays on branch app/t871-counts-once, acf6f8ba8, unmerged).

Owner 2026-11-18 ~20:28, verbatim, looking at the Mamquam River trout table, where every row's right-hand cell says
"counts toward the 2": "When they are all the same do you think it would be better to just have a single counts towards
the 2". Then: "Make sure to give example for trout changes before so I can see and confirm".

## The proposal (what the branch draws)

- A keep box whose rows all count toward its one number says it ONCE, on its own line at the top of the box, under the
  head: "Keep 2 in all · HATCHERY ONLY" / "All of these count toward the 2" / then the box's existing lines ("only 1 over
  50 cm (20 in), 2 if one is a hatchery steelhead") under it.
- A row's right-hand cell is dropped when it would only repeat that limit; the row then runs the box's full width.
- A cell stays only where the row adds something, and no longer repeats the limit: Dolly Varden / lake trout keep "only 1
  between them" (was "counts toward the 2 · only 1 between them"); Sowerby Lake's "only 1" and "only 1 over 50 cm (20 in)
  between them".
- Rows keep their own sizes and notes (steelhead's "after keeping 2, stop fishing this river today").
- When the rows do NOT all share it (the Vedder: "2 total from these 6" beside rainbow trout's "counts toward the 4"),
  nothing changes: every cell stays as today.
- Each row's screen-reader label is unchanged (it still reads "counts toward the 2").

Pictures (staging web export, 390 px, dark), scratchpad: `t871-before-mamquam.png` / `t871-after-mamquam.png`,
`t871-before-vedder.png` / `t871-after-vedder.png` (unchanged: rows differ), `t871-before-sowerby.png` /
`t871-after-sowerby.png`.

## Where the cell comes from, and what the branch changes

The cell is the trout contract's `table.boxes[].bands[].text` (per-row bands, `pipeline/deliver/answers/trout/table.py`,
words `trout/words.py` `band_upto` "counts toward the N", `band_in` / `band_size` "counts toward the N · …").

The branch does it app-side, in the view model, as the coordinator asked (the decoder `trout/check.Table` reads the
bands, not the words):
- core `regs/trout.ts` `sharedOnce`: a keep box of 2+ rows where every band is the box's "counts toward the N" band or
  starts with it + " · " gets `TroutBoxVM.shared` ("All of these count toward the N", `words.ts` `TROUT_BOX.ALL` around
  the payload's own "toward the N") and each band a `cell` (the rest, or null). Left as the payload has it: a band that
  does not start with the limit, a one-row box, a dropped cell with a date or a nested band of its own.
- ui-native `KeepBox` (`shared`, `bare`), `TroutCard` (draws `cell`); the crawler page (`tools/seo/model.ts`) prints the
  shared line too. One component, so phone, tablet/desktop and 200 % text (the band folded under the rows) all follow.

**This breaks the T810 contract guarantee "the app states no rule, draws every payload word unchanged" (contract 2.12,
trout.ts header):** the app now matches the payload's "counts toward" words and the " · " join. The import gate
(`conformance/src/regs/trout.release.test.ts` ALLOWED) was widened by one word for it.

Pipeline-side alternative (the cleaner landing if the owner says yes): the table builder emits a box-level `shared`
line (contract version 6) and drops the band words that only repeat it (`band_part` on the "counts toward the N · …"
bands, no band at all on a row that only counted toward it, `Band` coverage relaxed or a new `none` kind); the app goes
back to drawing words unchanged and `sharedOnce` is deleted. `trout/check.py` A_TOTAL_UNSAID_ONLY_WHERE_THE_TOP_NAMES_IT
would need the box line as a place the total is said.

## Tests

`core/src/regs/trout.test.ts` (T871 block: all equal → hoisted; one differs, a different number, own cap → every cell
kept; one-row, dated, nested cases left alone), `ui-native/src/regs/TroutCard.test.tsx` (T871 block: one shared line
before the strip, only the adding cell drawn, no empty column; one differs → all cells; large text), the conformance
vectors (`expectHoisted`), regenerated VM fixtures, the wide-layout snapshot (longest line 27 → 31 characters on klwa).

## Open for the owner

1. Yes / no to the look (pictures above).
2. The line's words: "All of these count toward the 2" (proposal) — or e.g. "Up to 2 a day, all of these together".
3. Whether the head "Keep 2 in all" and the new line are one line too many (the head already says the 2).

## 2026-11-18 ~20:53: owner declined
Shown the Mamquam / Sowerby / Vedder before-after pictures and asked whether to keep or drop the "All of these count
toward the 2" line, the owner picked "Not this change": the trout table stays as it is. Nothing lands; the branch is kept
for reference only.
