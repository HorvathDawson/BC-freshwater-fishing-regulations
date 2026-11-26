# T1105 — Sources "Technical detail" should show the effective value when an inherited closure yields to a nearer row

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1105 | Sources "Technical detail" should show the effective value when an inherited closure yields to a nearer row | OPEN — 0.10.1 (after T1053 ships); owner picks the words | | not built (hardening lane 2026-11-26): needs a per-part bundle field + a data build; see "Found"; planning chat |

Source: 0.9.1 chat 2026-11-24 ~23:55, a proposal left unbuilt by T1053 (draft PR #166): with the head-lake walk and
the "own row wins" rule (`read.ladder.ALL_YEAR_INHERITED_CLOSURE_YIELDS`), an inherited all-year "No Fishing" can
give way to the stream's own row or a nearer tributary row. The Sources sheet's Technical detail still lists the
inherited rule as it is bound, with nothing saying it yielded here or which row won. It needs an app change, so it
was left out of T1053 (a data-only release).

## Work
1. The pipeline already knows which rule yielded and to what (the ladder step): carry that in the bundle per part
   (resolved in Python, `cascade-resolved-in-python`; the app computes nothing).
2. Technical detail shows, for a yielded rule: "Does not apply here: <the winning row> is nearer" (words to the
   owner with a picture, light and dark, on Dove Creek above Anderson Lake and on Frosst Creek).
3. Tests: bundle contract for the new field; the sheet renders it; an old bundle without the field shows today's
   text (not an error).

## Acceptance
On a water where a closure yields, Technical detail names the yielded rule, the winning row and why; the owner
picked the words.

Depends: T1053 merged and shipped. Severity: low (the answer is right; the explanation is incomplete). Release:
0.9.2 (app + data). Model: Opus (pipeline contract), Sonnet for the sheet. Size: S–M. Owner: yes (the words).

## Found (hardening lane, 2026-11-26; nothing built)
Stopped at investigation: the fix the ticket asks for is a new per-part bundle field, which reaches the app only through
a data build, and its words are the owner's; the lane's brief excludes both.
- "Technical detail" is the fold on a RULE's own sheet (`app/packages/core/src/regs/sheets.ts` `ruleSheet`: the rule key
  and `export.fields` as JSON). A Sources row opens it. It is built from the rule alone (`RuleView`), with no part, day
  or fish, so it cannot say "does not apply here" from what it is given today.
- The yield itself already reaches the app: step 3c (`read/ladder.py` `_step3c_all_year_inherited_closure_yields`)
  records `why[k] = (own_row | nearer_row, winner)`, `trace=True` lists the yielded closure as a loser
  (`_answer`: `state` "displaced", `reason`, `by`), and the bundle's verdict frames carry it in `lost`
  (`answers/encode.py` `VERDICT_LISTS`; app `VerdictWire.lost`). A row's own Sources sheet (`src:rN`, `srcSheet`) shows
  it in the "Overruled" fold as "Beaten by “<the winning row's words>”". Words exist per reason in
  `answers/display/decided.py` `REASON_WORDS` ("Replaced by {by}: its water is nearer this stream.").
- The yield is per FISH, not per part: with `ALL_YEAR_INHERITED_CLOSURE_YIELDS = "retention"` it yields only where the
  winning row has a retention limit that speaks for that fish. A per-part "yielded" fact on the rule sheet must say for
  which fish (or the sheet must be opened from a row, which already has it).
- Two ways, for the planning chat: (A) as the ticket says, a per-part field on the rule's facts (`RuleFacts`, emitted
  to `answers.generated.ts`, rule 40; the export codec, rule 58), proved by Python unit tests and a data build; (B) no
  new field: the rule sheet opened FROM a row's Sources sheet takes that row's `lost` entry (reason + winner) and the
  existing reason words. (B) is app-only and needs no data build; both need the owner's words and pictures on Dove
  Creek and Frosst Creek.
