# The trout and char table: contract

The table is the body of the Regulations page's "Trout and char" entry. **Python writes it whole** (every row, word,
number and date: `pipeline/deliver/answers/trout/`) into the release's `trout` section; **the app only draws it**
(`app/packages/core/src/regs/trout.ts`, `app/packages/ui-native/src/regs/TroutCard.tsx`, `KeepBox.tsx`). AGENTS 29a is
the rule; this page is what each side may and may not do. "The contract 2.x", "Q-Dn" and "the fight list" in code mean
this page; "the spec 3.x" (the retired build spec) means section 3 here.

**The five guarantees.**
1. **One source for the shape.** The payload is a strict pydantic model, `pipeline/deliver/answers/model/trout.py`
   (`FRAMES["trout"]`); its TypeScript, wire and decoded, is generated into `app/packages/core/src/answers.generated.ts`
   (AGENTS 40). The app writes no payload type by hand.
2. **No number the app could count with.** Words, structure (box, row, band, which rows a band spans) and rule indexes
   only. The one exception: `YearBar.runs` (`[state, days]`), used only as lengths to draw.
3. **Every empty field states why.** Each nullable field is paired in a validator with the condition that empties it.
4. **One fixture.** One committed vectors file, written from Python, read by both suites (2.8).
5. **One trout path.** No trout word comes from the old group tree or from a TypeScript composer.

## 1. The eleven answers (Q-D1 to Q-D11)

| Q | As built |
|---|---|
| Q-D1 Box words | Python writes every box head ("Keep 5 in all", "Release", "Closed") and every band text ("up to the 5", "only 3", "only 1 of these 4"). A box carries no `n`. |
| Q-D2 The day phrase | No Python text says "today" except the stop duty. `top.lead` carries one `{day}` slot (`"Up to 5 {day}"`); the app fills it with "today" or "on Jul 15" (`dayPhrase`). The validator allows `{day}` only in `top.lead`, at most once. |
| Q-D3 NULL cell | `cell.trout` is NULL exactly when the page draws no table: the display frame at the same key and segment is `closed` or `tidal`, or the rows frame asks about no trout or char. A trout-only closure is a table ("Closed for trout and char {day}"), never NULL. |
| Q-D4 Every fish once | Every trout and char fish on the frame is in the table exactly once. `covers` (rows-frame row indexes) says which rows the table replaces; the app's split reads `covers`, never a fish list. The trout set is `common.trout_char()`: CT, DV, EB, GB, LT, RB, ST. |
| Q-D5 One table | One table, one top line; two keep boxes where a water counts fish apart (Kootenay Lake, Lois, Khartoum). |
| Q-D6 Rule refs | Every element that states a rule carries `src`, rule indexes in the rows frames' space (`rule.rix`): a row's decides-first order, the table's every rule in first-cited order ("Sources ›"). A fish sheet's `src` is cut to 4 by Python (installed 0.9.0 refuses more); the app draws a row sheet's breakdown from the row's whole `src`, in order (T1031). The app never sorts, unions or cuts a list. |
| Q-D7 Angler-only notes | Not in the table: they stay in the page's caveat block (owner). |
| Q-D8 Possession | The page keeps its own line, chosen once in Python (2.11). A row carries a possession line only where `card.possession_duty` returns one; below the day's number it is a `possession` band plus a `quiet` line; shared by every row of a box, it is the box's `possession`. |
| Q-D9 The rows 34 drop | On covered rows `group` and `elsewhere` are gone; `year`, `week`, `items`, `conds`, `real_daily`, `everyone` and `stamps` stay (the rest of the page reads them). A field leaves a covered row only when the table says it and no other reader is left. |
| Q-D10 Sheets | Fish sheets are their own interned table, `trout_sheet`; decoded, the frame holds them inline. |
| Q-D11 Previews | Picture packs come from a local side release; the staging `side <dir>` scenario uploads one under `test/release/<id>/` (`deploy/staging/test-updates.mjs`). |

## 2. The contract

**2.0 Mixed rows are split.** Python splits a rows-frame row holding trout or char and other fish into its trout members
and a remainder row in its own words, so `covers` stays whole rows and the app never splits a row (`trout/table.py`).

**2.1 Where it lives, and versions.** Each `regs-{r}` file has `cell.trout` (the last column, NULL per Q-D3) and four
interned tables: `trout_frame` (`[table id, dates id | null, covers]`), `trout_table` (the body: every NOW word),
`trout_dates` (every NEXT word of one cell), `trout_sheet`; their zdicts sit in `water-shared`. The section is required
(`contract._REGION_TABLES`; the optional lists are empty) and the regs files are `regs-region/2`. **Any change of a
field, validator or wire layout bumps `trout`** (`answers.SECTIONS`), and `test_answers_versions.py` fails until the
app's hand-typed `KNOWN_VERSIONS` moves with it: the app's version says what its reader was written for, so it is never
generated. One exception: an optional field absent where unset, which installed readers ignore (`TRow.down`,
`TRow.tag`), keeps the version.

**2.2 Where it is built.** `Section.compose_key` runs once per answer key over all its segments, after the rows
post-passes and the display compose, and adds no day cuts. Weekday words, the licence and stamp line and the steelhead
stamp come from the POST-PASS values, so the table never says a stamp the licence dropped. The body reuses
`card.possession_duty`, `tree.across_rows` and `size_rule` as they are; dates come from the engine's answers, never from
words. The app taps outside any hours window, so a change inside one is a line in words.

**2.3 Shape.** `model/trout.py` is the shape, decoded (`TroutFrame` {table, dates, covers} down to `Box`, `Band`,
`TRow`, `FishSheet`, `DateSet`) and on the wire (`TroutFrameWire`, `TroutTableWire`, `BoxWire`, `TRowWire`). Every field
is required and the wire carries every key; keep boxes come first, then at most one release and one closed box.

**2.4 Who writes which word.** **Python, whole:** every text of the payload, from one words module, `trout/words.py`,
with whole-string tests: the top line, the fold, box heads, labels ("Hatchery only"), strips, tags, row names,
sub-lines, lines, quiet lines, band texts, `together`, the year title, `elsewhere`, sheet heads and lines, every date.
**The app, navigation only** (`core/src/regs/words.ts` `TROUT_NAV`): "How to tell a hatchery fish ›", "Sources ›",
"Fished elsewhere today? ›", the sheet's section heads, the marks ↩ and ✕, and the `{day}` phrase. No app word states a
rule; upper-casing a label is styling.

**2.5 Days and where a date sits.** One body and one date set per (answer key, segment); the app picks the segment with
`segmentIndex` (`tap.ts`, the port of `encode.segment_index`), its only date choice. The body holds no date word. The
date rule is 3.3, decided in Python; placement too: card if every fish shares the date and words, else the box strip,
else the band, else the row (one level per row, a frame validator). No date line names a weekday (Python does not know
the year).

**2.6 What NULL and empty mean.** `cell.trout` NULL: no table and no entry; `frame.dates` null: nothing changes later
in the key's year; any other null or empty field: nothing drawn. A null is never a fallback (AGENTS W46).

**2.7 Versions and older apps.** The install gate and the held set compare file FORMATS (`manifest.ts` `readsRelease`,
`boot.ts`), so a shape an older reader cannot read bumps `regs-region/n`: the older app keeps its data, and
`minAppVersion` is derived from the format (`app_gate.oldest_reader`, `app_reads.json`), never typed.

**2.8 The shared fixture and the screen-reader label.** `app/fixtures/trout/trout-vectors.generated.json` (`python -m
pipeline.tools.emit.trout_vectors`, held by `emit --check`) is decoded from the committed trout fixture release
`app/fixtures/trout/regs-release/`, cut for the waters of `trout_vectors.CASES`. Each case pins a water by stream key
and metre (or `wbk`), a day, the app's segment `s`, the decoded frame and `composed` (the `{day}` slot filled both
ways). `app/fixtures/trout/trout-hand.json` holds the edge cases BOTH sides must refuse, one file, never a copy per
side. Only `scripts/trout-refresh.sh` moves these files and the goldens. **The screen-reader label** is a row's payload texts in
payload order (name, origin, label, tag, sub, `lines`, `quiet`, the band text that covers it, then a nested band's,
then its date), joined by ". " (no ". " after a text that ends in ".", "!" or "?"). Python writes no composer for it;
the app pins its output in `app/fixtures/golden/trout-a11y.json`.

**2.9 Bytes.** The section counts against every `WEB_BUDGET` moment (`release/contract.py`), measured by
`pipeline/tools/audit/trout_size.py`. Over a limit: intern phrases through the `str` table, read `elsewhere` lazily, then
the owner's rule (RULINGS O5): raise the limit by what was measured plus 10%, and shrink later.

**2.10 The proof (J).** `trout/check.py`, a separate decoder (it imports nothing from the builder), reads the shipped
words with `trout/words.py`'s grammar (an unparsed text fails) and the law from the answers and the keeping engine, and
proves per key and segment what its docstring lists, each check with a mutation test (fight list #9: the build checks
that walked the old group tree are re-aimed here). It runs in the card gate; the store refuses a stamp that checked
fewer tables than the file holds.

**2.11 The page's possession line.** Chosen once, in Python (`card.page_possession`), and shipped in
`RowsFrame.page_possession`: the lead and its `src`, the page row's value words and its kept also-lines (non-trout fish
only). A table row has a possession line exactly where its fish's differs from that lead (`test_answers_f0_page_possession.py`).

**2.12 The screen half.**
- **Read** one `TroutFrame` per shown day. **Check** it first: the `trout-hand.json` refusals plus a rule index the
  store lacks and a `covers` index past the rows frame, through `reportFailure`, and then no table is drawn.
- **Compose** two things only: the `{day}` slot and the screen-reader label (2.8). **Draw** every text unchanged (fight
  list #5: a test holds each drawn string to the frame), each date where the frame names, the year bar only when set;
  the folded entry shows `fold.value` with its arrow (it replaces "Details" on the salmon and trout folds only).
- **Split:** other fish are the rows NOT in `covers`, each drawn once; the page's timed caveats skip covered fish.
- **Never in the app:** a word that states a rule, a count, a comparison of numbers, a sort / union / cut of rule refs,
  a choice of which date to show or whether the year bar shows, a hand-typed payload type.

## 3. The screen, element by element

**3.1 The table.**
- **Top line:** "Up to N {day}" or one of its forms ("…, or 5 if they're rainbow trout"; two boxes counted apart;
  "Release every one", "Closed for trout and char", "Nothing to keep"), `sub` for the exception under the number,
  `second` for a cross-box limit. A summed tier (a total that adds two limits) is never drawn.
- **Keep box:** one tint block, no outline; its strip says "Keep N in all", the filled HATCHERY ONLY label when it
  covers every fish, the size sentence, a wild limit only where wild fish have their own, the shared possession on its
  own line, an optional tag ("Lake's rule").
- **Row:** picture; the name as the book prints it ("Dolly Varden/bull trout"); the label or the origin word (twin rows
  only); the tributary tag; `sub`; bold lines (size, "uses the 1 over 50 cm", possession, the stop duty naming the
  stored noun, never "here"); quiet lines; the row's date.
- **Right band:** "up to the N" one cell per row, or a white box "only 3" / "only 1 of these 4" across its rows, with
  nested counts inside. **Release / Closed group** under the keep boxes: no fill, faded picture, one column word; a fish
  whose hatchery ones may be kept is not listed there as a wild row.
- **"All together"** under the table only for a limit across two boxes; the **year bar** only when every trout and char
  fish shares one state today on an open water; **links** "Sources ›" and "Fished elsewhere today? ›".

**3.2 Two sizes of text.** Today's rule is the bigger, bold ink: the strip sentence, row lines, limits, band texts, the
column word. The HATCHERY ONLY label is a filled label wherever it sits. A coming change is smaller, regular and
quiet-coloured, on its own line: the strip, row, band and column dates, the card line. The data marks each text NOW
(body) or NEXT (`DateSet`); the app picks the style.

**3.3 The date rule.** For each (key, segment, fish, row): walk the following segments, wrapping into next year, and
take the first whose answer (state, count, size bands, origin rule, possession) differs. A change that is only looser
reads "rule changes <day>". A tighter change or a change of state is said in words: "until <last day>, then <words>". A
coming closure carries no reopening date; a reopening date appears only where something is closed today (a closed
table's top date, or a closed fish's row or column date). A short gap is still dated (under 10 days warns). A one-day
segment puts the stricter value on the row and one sentence in the sheet. Placement is 2.5.

**3.4 The fish sheet.** A header (today's answer: "Keep · only 1 · counts toward the 5"), "Here, today" (Python's
lines, the stop duty bold), "How to tell it" (the app's pictures and `FISH_TIPS`) and Sources (the row's rules: Q-D6).

**3.5 Sources, large text, theme, screen reader.** "Sources ›" opens the every-rule sheet fed by the table's `src`. At
200 % text the right band folds away: each limit becomes a bold line under its fish, a left rail joining a shared
limit's rows; no sideways scroll at 360 px. Dark and colour-blind modes use tokens only, and state is carried by words
and marks, never by hue alone. Each row reads as one label (2.8); buttons use the ARIA props (AGENTS 34); press targets
are at least 44 px.
