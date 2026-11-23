# T809 — A tool that lists every answer that changed between two releases, in plain words, as one page for the owner

Status: DONE — 9c033d495, a3f9a7fb3, 0b936544f, 8bfd53567, 372bde554, 46c18af28, 8c4620fcd (the tool and its leftovers); the cutover diff run and recorded 2026-11-22 (below). One finding became T1018.

Source: the trout table build spec, `handoff/PLAN/design/trout-table-build-spec-2026-11-16.md`, section 5 ticket K and
section 7; owner 2026-11-16 15:42, asked how to start: "Start steps 1 and 2" (step 2 = this tool). Today the
changed-answers list he asks for before every shipment is put together by hand.

## What to build
- `pipeline/tools/audit/answers_diff.py`: compare two releases (or two local builds) per water and day range, for
  every block of the page (trout and other fish rows, salmon, closures, gear, licences), and write the differences in
  plain words, grouped by kind of change (closes / opens / limit lower / limit higher / size / wild or hatchery /
  possession / gear / wording only), with counts, water names and one example day each.
- Output: a text list for the main chat and one page for the owner (plain words, no ids in the visible text; ids in a
  quiet line). An identical pair gives an empty list.
- It reads built releases only (the bundle is the only source); no rule logic of its own beyond comparing answers.

## Tests
- A built pair with a known planted difference is reported exactly once, in the right group.
- An identical pair is empty.
- Mutation: a difference hidden in a day range that neither release starts a range on is still found.

## Pass
The tool run on the last two real releases gives a list the main chat can check against what was told to the owner
for that release; the T805 fixes are then shown to him with it before they ship.

## 2026-11-16 16:19: landed (merge dac013480; worker commit 9c033d495)
`python -m pipeline.tools.audit.answers_diff <release A> <release B> [--out DIR]` writes `diff.json`, `diff.txt` and
`owner.md`. Real pair r-46e4355a7277 → r-685b700b2802 (36.6 s, 3.28 GB peak): the 71 book-closure rows exactly (same
fish, days and sections, 71,046 distinct). It also found four differences in that candidate beyond the hand list (gear
lines on 3,745 sections; the finfish flag on 2,597; 698 same-answer groups now sourced from the rule book; Stamp River
words and Quesnel Lake Horsefly Bay): explained by a separate read-only worker before the salmon landing. 21 tests.
NOT YET DONE (left on this ticket): a cross-atlas pair (different section handles, e.g. r-a9914d6b25b7 →
r-46e4355a7277) has never been run; the regs side is proven on the mini only; possession and gear-count directions
read "unclear"; card rows and shared limits fall into the catch-all.

## 2026-11-17: the leftovers (lane/t809-k, `a3f9a7fb3`, `0b936544f`)
- **Rows 34**, keyed off F0's `PAGE_POSSESSION_SINCE` (`answers_diff.CUTOVER_ROWS`): each release is decoded at its own
  rows version (5 or 6 frame slots). Across the line, a trout row's dropped `group` / `elsewhere` (every fish a trout
  or char, `common.trout_char`) and the new `page_possession` are left out on both sides. Non-trout rows and every law
  facet are still compared. Two rows-34 releases are compared whole. The trout table itself is not compared (J proves
  it). Tested on a planted rows-34 mini release: the pair is empty both ways, plus mutations.
- **Directions:** possession reads the numbers of its rules (`take`, `lengths`, `per_daily` × daily). Counted gear reads
  the deciding rule's most or least, and otherwise the allowances in its words. A gear ban that comes is stricter and
  one that goes is looser. "Unclear" is now left only for changes that go both ways or carry no number.
- **Kinds:** shared limits (the numbers 2 or more fish share) and the fish card's rows (the same answer for every fish).
  A difference inside a decided answer that no facet reads stays "other".
- **Rule refs:** every rule of a gear conduct line is now resolved, and gear's `by` [rule, n] pairs keep their n.
  Before, when the rule lists differed, both bugs made every gear frame differ.
- **Cross-atlas pair run** r-f749475d0962 (atlas b9de120e) → live r-46e4355a7277 (atlas 55032563), with
  `--handles-a data/generated/atlas/full.b9de120e --handles-b data/generated/atlas/full`. Run time 51 s, peak 3.1 GB.
  All 1,957,029 sections matched by id. 125,974 have a changed answer and 153,387 changed in any way. The regs side,
  fish rules not salmon: 901 closes, 156 opens, 40 lower, 9 higher, 69 / 287 / 14 size stricter / looser / mixed,
  253 gear, 168 licence, 32 shared limits. Release-wide: 205 parts gone, 33 new. This proves the regs side on a real
  pair.
- **Not covered by this run:** the three releases on disk share one rule list, so ref resolution was not exercised on
  real data. 1,327 regs sections are still "other": 480 display status/closing, 357 ladder, 253 rows where only the
  winning rule or roles moved, 130 gear bait/decides refs, 57 water kind or steelhead presence, and 38 licence
  `holds.considered`.
- Left on this ticket: the cutover diff itself (spec section 7 point 2, step 11).

## 2026-11-17 22:30: review fixes (verdict FIX FIRST; `372bde554`)
- **The keeping law** (`Keeping`, `joint_changes`) replaces the per-key shared facet. Each side's law (decided
  answers, pooled rows, real-daily shared counts and caps, group total / most here / cap system, and from rows 34 the
  trout table read by J's decoder) is compared by the baskets it accepts, with J's method. Across the cutover the
  trout groups' numbers are now compared against the table (review M3: reported, and the mutation without it is
  empty). Caps no longer collide. A structural cap change with the same law is "other", never "no change".
- The cutover view splits mixed rows as L's `cut_over` does and drops by the cell's `covers`. A rows-34 release with
  no trout section stops the run.
- Possession `per_daily` multiplies the group total (`GroupPossession.of`). The skip applies only when the rules and
  their numbers are equal (M1).
- `card_only` refuses any path under `fish`, `group`, `conds`, `items`, `real_daily`, `daily` or `stamps` (Anahim's
  stamp is now "other").
- Gear: `clause` pairs and `specs` refs resolved, plus the count edges.
- Proved on L's own pair: the c26f2a384 mini release against bfc3faba3's cutover mini (r-1e5210776d72) is empty. The
  planted `_cut_over` fixture (real `compose_key` + `TroutCodec`) is empty both ways.
- Real pair rerun (51 s, 3.1 GB): the same 125,974 / 153,387 sections. Shared limits went from 32 sections to 0;
  those were artefacts already explained by the rainbow's own size change. Card rows went from 26 to 0 (now "other":
  stamps). Regs "other" went from 1,327 to 1,369.

## 2026-11-22: the cutover diff, run and recorded (lane L13, 0.9.0 batch)
`python -m pipeline.tools.audit.answers_diff` on the last pre-cutover-format release r-fec3fab4c5aa (production 0.8.4's
data: old formats) and the first packed-format release r-741815c6090d (min app 0.8.4; the one carrying the trout table
section), same atlas (section handles match by id), via `scripts/heavy.sh` lane 2: 1 minute, exit 0, 1,957,010 sections.
- 28,715 sections changed in 11 groups. 1 of them is Lost Lagoon (1 section): a local rule now closes it, so its bass,
  crappie, crayfish, kokanee, whitefish and trout rows go from "keep N" to closed, its two size rules are gone, and its
  salmon answer is now "closed by the rule book" instead of the Region 2 default (same words for the angler: closed).
  That is the book's Lost Lagoon rule arriving in the data, not a cutover effect.
- The other 28,714 sections (314 waters, 26,921 unnamed stretches) are ONE group, "other", the licence frame:
  `alien_closures[0].rule` differs on both sides (before and after read the same on the page: both print no licence
  line). The tool names the field path but cannot say if it is a ref it did not resolve or a real change: T1018.
- Not one trout or char answer changed across the cutover (no closes / opens / limits groups on trout rows other than
  Lost Lagoon's): the table reads as the old card did. Output kept in the lane's scratchpad only (diff.json, diff.txt,
  owner.md; not committed: 3 GB-class decode, reproducible with the command above).

