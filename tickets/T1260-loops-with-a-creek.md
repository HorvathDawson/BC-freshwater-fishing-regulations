# T1260 — Side-channel loops that a creek flows into (what T1242's loop removal left out)

Status: OPEN — design saved, not built; after 0.10.0.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1260 | Side-channel loops that a creek flows into (what T1242's loop removal left out) | OPEN — design saved, not built | — | pick it up after 0.10.0: the owner's picks (limit, keep-kinds, link vs tail vs draw-only), then build and diff bindings |

Source: the owner, 2026-11-27, verbatim, in order:
- the goal: "the goal is to mostly have every stream be simply a single line the whoel way down ... sometimes taking the tail?"
- on the creek's end: "instead of the tail option could we make this a function in the build step so we could swap it if we want but could do gap when close to stream (with an added line to the feeder stream to connect it so it doesnt show a gap) or if far away we use. the tail? idk..."
- on size: "no limit for creekless but we need to decide the none creekless loop size we will simplify"; then "do 150m and show me any that end up being weird."
- the decision to defer, after the weird-links sheets: "i think i changed my mind for now we should just save info here and only remvoe the loops with no stream connecting it (if it isnt something like the stave spawning channels or following our other rules for when they cant be removed...) but we will leave loops that have a stream connecting for now. it is too much to figure out right now"

All figures are the data chat's dry-run and trial numbers of 2026-11-27, reported, not re-measured here.

## What 0.10.0 does (T1242)
Only loops NO creek flows into are removed, at any size (4,549 loops, 1,623 km). Kept: the named channels list (e.g. the
Stave River spawning channels), loops touching a lake, loops with a gauge / tidal limit / cut / owner mark. Every loop
with a creek stays (about 12,055). T1247 (the same-piece prune bug): check its ticket on main before saying this
closes it; it is not on main at the time of writing.

## Saved design (not built into main)
Where the saved material lives, read it, do not retype it:
- hand-over: `~/.claude/projects/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/night-notes-2026-11-27-data-release/loops/CREEK-LINKS-HANDOVER.md`; the same folder holds the sheets, the CSVs (`T1242-links-weird.csv`, `T1242-creek-loops.csv`) and `creek-links.bundle`.
- the rethink: `~/.claude/projects/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/night-notes-2026-11-27-planning/loops-rethink/loops-rethink.md`.
- branch `feat/T1242-creek-links` (head 0d49e80ca, unmerged, never pushed; the bundle restores it).

The LINK: a short new line from where a creek met the loop straight to the river. It is the creek's water (its code and
name, a made-up line id like the city streams), so no river water changes owner and no river rule is lost or wrongly
gained. A loop whose link would cross other water is kept. Dry run at a 150 m limit: 8,192 links on 6,863 loops, 5,977
with no flag, none crossing other water. At 300 m, 9 loops' links cross other water; with no limit, 121 (worst Iskut;
longest link 2.6 km, Stikine).

Proposed keep-kinds (about 85 loops kept instead of linked): new mouth on water with different rules 33; link crosses
a region / management-unit line 25; link doubles back 22; crosses a DFO place boundary 5; runs along a lake or wetland
edge 4. Flagged but linked in the proposal: another water closer than the river 938; new mouth crosses a park /
reserve / area line 880; three or more links from one loop 599; sharp angle 185; the creek has its own book row 3.
Not screened: road / rail embankments; creeks whose rules change (needs a built bindings diff).

## Open questions (each needs pictures and the owner's pick)
1. The limit (he said 150 m, then deferred).
2. The keep-kinds above.
3. The alternative "tail" (the creek takes the loop's lower part): in the trial 136 reaches lost a river closure and 27
   wrongly gained the creek's; a tail is the river's water by the province's code and the book.
4. The draw-only option (the rethink's A: hide same-answer side channels below zoom 12, keep the creek-carrying piece
   drawn); overlaps T1241.
5. A section that belongs to two waters does not exist in the model, so a "bent link" along the loop is the tail.

## Known leftovers of deferring
- The Peace River's 9 km loop carries the 1,200 m Site C closure along its whole length (an artefact that stays while
  loops with creeks wider than 150 m stay).
- Park loops with creeks the step-order check found "nothing real lost" for: Beaverfoot x2 (Yoho), Wood (Hamber Park),
  Ningunsaw (ecological reserve); Gordon, Adams and both Peace loops are wider than 150 m.
- The Goat River side channel near Creston stays (one creek flows in; it strays 1,494 m; owner: "it stays by 150m rule ...").
- The big braided rivers (Skeena, Fraser, Squamish, Chilcotin, Muskwa) lose no loop under the creek-less rule, since
  every loop on them carries a creek.

## Work (when picked up)
Show pictures and get the picks above; rebase the saved branch on the merged 0.10.0 build; one province-wide build;
bindings diff (no creek's or river's rule moves unasked); weird screen on the built result; the link's look in the tiles.

## Acceptance
Pictures and the owner's picks recorded; a bindings diff with no unasked answer change; counts match the dry run.

Related: T1242 (parent), T1247, T1241, T1240. Target: after 0.10.0. Depends: T1242. Size: L. Owner: yes (picks above).
Default: leave loops with a creek as 0.10.0 leaves them.

## Owner

- [ ] 2026-11-27 — Pick the limit, the keep-kinds, and link vs tail vs draw-only once pictures are shown. His last word: "we will leave loops that have a stream connecting for now. it is too much to figure out right now".
