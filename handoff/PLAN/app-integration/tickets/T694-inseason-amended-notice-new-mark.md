# T694 — An amended matched in-season notice carries no "new" mark

Status: POSTPONED (owner 2026-11-12: gap 2 is not built now; after closed testing).

Source: the read-only in-season lane of 2026-11-12 (inferred from code), and the owner's dialog answer "Fix gap 1 only"
(T693 built gap 1). In-season notices are BC Fish and Wildlife's, not DFO's.

## Gap
A notice already matched to a water whose text is amended (the feed row's status `changed`, `previous` set) is the same
notice id: it is not in the bell (matched) and nothing on its page says it changed since the reader last looked
(`inSeasonUnseen` reads only the unmatched list). A reader who saw the old text is not told.

## Work (after closed testing)
1. Decide with the owner what "new" means for a matched notice (a mark on the page banner, on the bell, or both; how it is
   cleared: seen once per device by id + text, as the unmatched rows are).
2. Build it in core (`inseason.ts`) and the banner/bell UI with a test for the amended-row case.
3. Measure first: how often `changed` rows happen in the feed (staging 2026-11-11: none; T675 removed the false ones).

Depends: T693 built; T673 parked. Size: S. Owner: yes (the wording and the mark). Default: nothing built.
