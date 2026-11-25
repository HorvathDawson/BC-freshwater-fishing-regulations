# T1105 — Sources "Technical detail" should show the effective value when an inherited closure yields to a nearer row

Status: OPEN.

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
