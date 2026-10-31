# T332 — DFO salmon quotas in the SAME quota/size model as the game-fish (trout) rules; wording list checked + accepted

Status: OPEN (owner 2026-10-30: "please tidy them. yes and double check them all. and how they lead to the quota
structure data (which should have same format as trout ones for consistency like same size datatype, quotas etc.)").

## Why
T320 gave DFO salmon its own verdict shape `q` = {d, h, o, mx, w, mo, a, u}. The game-fish rules already have a quota
tree (aggregate + children), `lengths` (ordered size ranges, each with its own take — memory lengths-replaces-size-
polarity), `when`, and gear. Two shapes for one idea ("how many, what size") is the duplication the owner wants gone.

## Work
1. **Wording list:** tidy the split scraps ("f", "fraser river watershed" — fix the clause split, not the entry), check
   all 85 entries one by one against real cells (each entry: meaning correct? reads correct? example cell), then mark
   them `accepted` (owner approved 2026-10-30). Any entry whose meaning is doubtful → listed for the owner, not accepted.
2. **One model:** map every structure meaning to the game-fish types — daily count → the quota node's take; "only N
   over X cm" / "N of which may be over" / "none over" / "max size" → `lengths` ranges; hatchery-marked / wild
   allowance → the same retention qualifiers the regs use (or add ONE shared field both use); monthly → the quota
   period; the region bucket → the aggregate tier. Document the mapping table in the ticket.
3. Convert pipeline (`dfo_salmon/quota.py`, `typed.py`, `deliver/salmon.py`), feeds worker TS port, app (`salmon.ts`,
   `SalmonCard`) to the shared types; the app renders salmon limits with the same presenter as trout limits.
4. Tests: every saved DFO cell reads to the shared model; TS/Python parity; the T320 spot-check five read the same words
   as before; salmon file golden updated.

Depends: T320, T321. Size: L. Ships with the release after 0.4.0.
