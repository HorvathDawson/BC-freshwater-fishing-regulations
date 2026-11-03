# T366 — DFO live refresh that is clean to run (region 2 FN1179 first)

Status: OPEN (owner 2026-11-03: "make it so that this command works cleaner in future too"; batch 3 of
RELEASE-PLAN-0.5.0.md, with the salmon clean-up).

Why: T361 showed a refresh can silently move owner marks to "rule changed" (the typed rows were a season old, then
re-emitted from a newer page). Region 2 now needs one: staging's live fetch saw FN1179 (Fraser, mainstem upstream of
the CPR Bridge at Mission: chum 2/day Nov 3–30), which no saved page has (newest region 2 copy 10-28, modified 10-16).

## Work
1. **Preview before writing:** `live run --regions N --dry-run` fetches and parses, writes nothing, and prints what
   would change: new / dropped / reworded places, rows changed per place (old → new, with the FN number), and every
   owner mark that would read "rule changed" or "binding moved". The real run prints the same summary.
2. **One vintage, stamped:** the typed file records the sha256 + dateModified of the raw page it came from; the review
   app and the queue warn (banner) when typed/ is older than the raw page, or the raw page is older than N days in
   season. No more reviewing last season's rows (T361's cause).
3. **Archive the fetched page:** every live fetch saves a dated copy into `cache/dfo_salmon/history/` (as the Wayback
   seeding does), so the wording archive dates the new wording and a later "historic or new?" question is a lookup.
4. **Rule change ≠ place change in the review app:** a mark whose rows moved but whose place and binding did not shows
   "DFO changed the rules" with the row diff, and a one-click "rules re-checked" re-mark (P stays as the owner set it).
   Marks are never edited; the re-mark is a new line.
5. **Run it:** region 2 refresh (FN1179), then any other region whose live page is newer; owner re-reviews what the
   preview listed. No credits (no `claude` CLI); network only.

Depends: none (T361 found the cause). Size: M.
