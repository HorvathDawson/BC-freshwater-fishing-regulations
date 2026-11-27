# Tickets — the sole source of future work

Owner, 2026-10-22: "make sure it uses proper ticket system … add anything else we know we need to do to the tickets.
use that as the sole source of future work."

## How the system works

- **Tickets are the only backlog.** Work that is not a ticket here is not planned. A progress note, a decisions.md row,
  a memory note, a REPORT "open" line or an audit item that needs doing becomes a ticket (or a line in one) before
  anyone works on it. implement_progress.md records what happened; it does not hold the queue.
- **One file per ticket:** `tickets/Tnn-short-slug.md`, numbered in order (`node app/tools/ledger.mjs --next` prints the
  next free number: past every closed id, claim, open file and branch's claim; never reuse a number). Small related items go into one ticket as numbered lines. **The file holds its
  own ledger row** (T1041): the one-row table right under the Status paragraph (see "Ledger" below).
- **Format** (as T56–T95):
  ```
  # Tnn — title

  Status: OPEN.

  | id | title | status | commits | what's left |
  |---|---|---|---|---|
  | Tnn | title | OPEN | — | the next step, a pointer (240 characters at most) |

  Source: where it came from (owner quote + date, file:line, commit).
  ## Work            what to do
  ## Acceptance      how we know it is done (tests, mutation pins, shots, numbers)
  Depends: … Size: S/M/L. Owner: no | yes (what for). Default: the call we build with until the owner answers.
  ```
- **Status values** (the `Status:` line sits under the title of every ticket):
  - `OPEN` — not started.
  - `IN PROGRESS` — a lane is on it; name the lane/worktree.
  - `PARTIAL` — some built and merged; say what is left and where it went (a follow-up ticket).
  - `IN REVIEW` — merged, proof pending: name the proof (a phone check, a run, an owner look).
  - `BLOCKED (owner)` — cannot proceed without the owner and no default is safe.
  - `POSTPONED` — deliberately parked, with the owner's word.
  - `DONE` — merged into `redesign/stream-sections`, acceptance met.
  - `DROPPED` — will not be done; one line why (owner or recorded Claude call).
- **Adding a ticket:** take the number from `node app/tools/ledger.mjs --next`, write the file with its row under the Status line, run `node app/tools/ledger.mjs --claim` (it
  writes `tickets/ids/Tnn`, the number's claim: two PRs that take one number collide there), add any owner call to "Owner questions" with its default,
  commit with explicit paths, the claim with them (`git commit -- <paths>`; never `-A`/`-a`/`git add .` — other lanes share the branch).
- **Closing a ticket (the lane's last step):** set `Status: DONE — <commit hashes>` (plus one line of anything
  notable), update the row under it (status, commits, what's left "—"; the row moves to the DONE view by itself), move any answered owner question to Answered,
  commit those files with explicit paths. Anything found and not done becomes a new ticket, not a footnote.
- **Closed tickets are archived (T1081).** A DONE / DROPPED ticket has no file, no claim and no picture folder: its row
  is one line of `tickets/closed.tsv` (id, title, status, commits, release, left, archive, report ids / crash hashes it cited:
  tab-separated, id order) and its full text is in `tickets/archive/closed-2026.tar.gz`. Read one with
  `tar -xzOf tickets/archive/closed-2026.tar.gz T123-short-slug.md` (`tar -tzf` lists them;
  cite one as `tickets/archive/closed-2026.tar.gz:T123-short-slug.md`). A ticket that closes stays a file until the next
  archive pass: `node app/tools/ledger.mjs --archive [Tnnn …]` (`--dry` says what it would do) writes the line, rebuilds the tar
  (old entries + the new ones, same bytes for the same input), checks it reads back byte for byte, then deletes the file,
  its `ids/` claim and its `Tnnn-shots` / `-files` folders (`KEPT_ASSETS` in `ledger.mjs` lists any a test reads). Commit
  that with explicit paths. `pipeline reports` and the commit check read `closed.tsv`, so a Tnnn in a subject or a report
  id a closed ticket cited still resolves. Reopening: put the file back from the tar, delete its line, run `--claim`.
- **Owner calls never block work:** every open call carries a recommended default; build with it and record it
  (owner call 11). The owner's open asks are lines of the ticket's own `## Owner` block (see "Owner blocks" below).
- T84 adds a guard to `pnpm check` that keeps this ledger whole (T1041: it reads each file's row).

## Ledger

**The rows are not kept here any more (T1041).** Each ticket's row lives in its own file, as a one-row table right
under its `Status:` line (same five cells as ever):

    | id | title | status | commits | what's left |
    |---|---|---|---|---|
    | Tnnn | the ticket's title | OPEN | — | what's left, a pointer (240 characters at most) |

`node app/tools/ledger.mjs` builds the tables into `docs/views/LEDGER.md` (every row not DONE / DROPPED) and
`docs/views/DONE.md` (the closed ones), git-ignored; the git hooks rebuild them after a checkout, merge, commit
or rebase, but a view MAY BE STALE (another worktree, a reset): `node app/tools/ledger.mjs --check` says so and the
ticket guard warns. So two PRs that touch different tickets never conflict. A row's view follows its status keyword.
A NEW ticket also claims its number: `node app/tools/ledger.mjs --claim` writes `tickets/ids/Tnnn` (the file's name);
two PRs that take one number then collide there in git instead of both merging. A CLOSED ticket (DONE / DROPPED) is
archived (T1081): its row is a line of `closed.tsv` beside this file and its text is in `archive/closed-<year>.tar.gz`;
`node app/tools/ledger.mjs --archive` moves it, `--next` prints the next free number. `node app/tools/check-tickets.mjs`
checks the rows and claims, and refuses this section holding anything but this text.

## Next after closed testing

Owner, 2026-11-13: "after the closed release we will do stocking and depth maps next. so add that to next after closed
tickets." Both start once Play closed testing (T495 / T371) is running; they are not part of T495. In this order:

1. **T200** stocked lakes (feasibility: `docs/research/stocking.md`).
2. **T201** depth maps / lake bathymetry (feasibility: `docs/research/bathymetry.md`).

Both ride on **T634** (the Lake tab and the owner's 2026-11-11 decisions; its "Path to ship" is the shared step list).
T218 (28 digitised lakes) and T219 (tracing scans) stay POSTPONED: they follow T201 and its licence answer. The other
deferred rows (river widenings T695, map-tile trimming T699, T389, T626, T223, T234) stay where they are.
2026-11-26: the owner moved T201 to "start soon" (next after the re-cut work; the lake-matching checks gate it) and
gave the licence for the Province's vector lake maps (T201).

**T714** (share saved spots) and **T715** (store video builds its own state) are after closed testing too (owner
2026-11-13: "these are after closed"); they are not stocking or depth work and have no place in the order above yet.

## Order for open tickets

The order lives on the restart pages `docs/agents/START-HERE.md` lists and in `docs/agents/ROADMAP-after-0.9.0.md`, not here (T540: the 2026-10-22 list
named 73 closed tickets out of 80; git has it, `git log -p -- tickets/README.md`).

## Owner blocks (T1081)

**Anything only the owner can do or decide is an `## Owner` block in the ticket it belongs to.** There is no owner
to-do page or folder: `node app/tools/ledger.mjs` builds `docs/views/OWNER.md`, one short page of every open
line, newest first, grouped by ticket (git-ignored, rebuilt by the hooks; `--check` says if it is stale).

    ## Owner

    - [ ] 2026-11-25 — the ask in one or two sentences; the options (A / B / C) and the default we build with.
    - [x] 2026-11-25 — the ask → "his answer, verbatim" (2026-11-26)

- **Add an ask:** a `- [ ] YYYY-MM-DD — <ask>` line (the date it was asked) under the ticket's `## Owner` heading (make the
  heading at the end of the file when there is none). A longer ask continues on lines indented two spaces. Pictures and
  files the ask needs stay beside the ticket (`Tnnn-shots/`) and are named in the line. A thing with no ticket yet is a
  `## <short title>` section of `tickets/OWNER-UNFILED.md` with the same lines; the planning chat numbers a ticket for it,
  moves the lines in and deletes the section.
- **Answer one:** the chat writes the answer into the line in the same turn the owner gives it: `[ ]` becomes `[x]` and
  the line ends `→ "his answer, verbatim" (date)`. An answered line stays as the record; nothing is copied elsewhere.
- **See what is open:** `node app/tools/ledger.mjs`, then read `docs/views/OWNER.md` (one line per ask).
- **A Status that waits on the owner** (IN REVIEW / PARTIAL) says `owner: see ## Owner`; the guard needs an open line in
  the block. A DONE / DROPPED ticket has no open line (answer it or take it out), and `ledger.mjs --archive` refuses one.
- `check-tickets.mjs` fails a block that does not parse (a line that is not `- [ ]` / `- [x]`, a missing or invalid date).
  The format is `OWNER_LINE` in `app/tools/ledger.mjs`.

## Answered owner calls, for the record

### Shipped on default (reverse any)

The ticket is closed and the owner never answered; the build stands on the default. Say so to reverse one (T540).

- Q2 (T54) Conditions map: keep the flow colour in no-access areas, or mark them there too? Built with: keep flow colour; Tier A gets only the banner on Conditions
- Q47 (T78) Tidal sentence: closed "Notes and sources" fold, or open under the Level headline? Built with: in the fold
- Q79 (T116) Regs page says "river" for a stream ("this river's rule", "Whole river") while search rows say "Stream" — switch the page to "stream"? Built with: keep "river" on the page
- Q91 (T105) The 54,386 matched-only sections now say "No gauge on this water is close enough in size" — wording ok? Built with: as built
- Q103 (T117) The gauge shed climbs the way the walk does: the Theodosia (08GC005) and Quinsam (08HD034) diversion gauges no longer speak for water above their canal (the river's own gauge takes over on 174 + 107 sections); 24 sections lose every gauge; a gauge ON a canal reads only its own piece? Built with: as built — ANSWERED 2026-11-12: "Canal gauge also serves river" (P54); measured: nothing qualifies, T696
- Q113 (T122) Gauge cuts follow the roster's realtime snapshot, sheds follow ECCC Active: 38 active-but-silent stations have a shed and no cut. Make cuts follow Active (atlas rebuild, moves section ids)? Built with: unchanged — ANSWERED 2026-11-12: "silent doesn't mean retired so we should add cuts Incase they start transmitting again?" (P53); built T691, rides the atlas build
- Q123 (T151) How the water Map view marks the selected part without bold lines? Built with: dim the other parts; one line width
- Q124 (T153) Does a river row's rule apply to an UNNAMED lake polygon its main flow runs through (book "river" includes its widenings unless it names the lake)? Built with: yes for unnamed polygons covered on both sides; a named lake never takes the river's row
- Q126 (T143) Were you on Level when +1/+3/+5/+10 said "no forecast"? (Flow answered at every horizon on today's feed) Built with: yes, assumed
- Q130 (T144) Under a combined Change, also draw each model's own line (quieter)? Built with: yes
- Q132 (T144) The +1/+3/+5/+10 chips keep the old model order (CLEVER then ELF) — or follow the Change source too? Built with: keep
- Q139 (T147) The no-access block also leads Conditions for a tapped no-access section at full size? Built with: keep
- Q146 (T150) Opening the parts list shortens the map but does not re-centre it (the Fraser's long lower part gets cut off on a phone) — re-centre on open? Built with: no (keeps the reader's pan)
- Q147 (T153) A pond a steelhead row now takes also gets the zone's lake wording of the steelhead rule (zp:steelhead r1b/r2b/r4b +68 polygons each, z5 r6b +44)? Built with: accept
- Q148 (T154) Promote "dammed widening = lake" (R2 with the dams layer: 26 candidates → LAKE; catches Quinsam and South Nanaimo, not Bonaparte) into the slough candidates, or keep dams as evidence only? Built with: evidence only
- Q149 (T154) Bonaparte (329055141) has no dam in the provincial data — mark it lake by hand on the slough page? Built with: yours on the page
- Q151 (T166) Unnamed streams: assume their tidal limit is at the mouth (no crossing work)? Built with: yes
- Q152 (T153) A pond a steelhead river takes as its widening is ANADROMOUS (a rainbow over 50 cm in it is a steelhead, p.80) — 68 ponds; found by the card gate on the Kitimat's pond? Built with: yes (built; one flag)
- Q153 (T174) A river's own side piece is adopted onto the LARGEST same-code section — use the nearest ANSWERED one instead (84 Flow gaps)? Built with: measured: largest ANSWERED in reach, +148 side pieces, 0 changed — SHIPPED (`ADOPT_ANSWERED`)
- Q154 (T174) Walk a river's own unowned double-line polygon as the river (as T101 does for stream items' polygons)? Built with: measured: they are small FWA LAKES, not river polygons; ON moves 96,590 sections (+95k Flow) — kept OFF (`RIVER_WIDENINGS`). 2026-10-28 owner "lakes stay lakes unless a pool labelled a lake": all 136,080 unnamed, 75% < 1 ha (mostly beaver/muskeg ponds); proposed cut < 1 ha AND avg width ≤ 2× the river's → 10,650 polygons, 11,423 sections — curation-review/q154-examples.md; owner 10-28 wants to SEE them: review app Sloughs tab → "Small ponds (Q154)" (20 md examples + 60 stratified sample, satellite + outline + river, pool / real lake / unsure marks of scope q154, tally vs the cut)
- Q155 (T174) Gravel Slough's 1425 connector lines take the answer of the polygons around them? Built with: yes — built as `water_line_gaps` (any stream water; Gravel 2 of 3, the 3rd has two answers)
- Q158 (T168) A renamed continuation's gauge counts as "on this water" (the Vedder lists the Chilliwack's 08MH001 as its own)? Built with: yes
- Q159 (T168) "Reporting, not ranked" gauges fold with the silent ones ("not reporting or not ranked")? Built with: yes
- Q160 (T243) Bottom tab labels follow the OS text size only to 1.3× (a quarter of a phone holds no more; the platforms' own tab bars do the same) Built with: 1.3× cap

- Retired with their DROPPED tickets (T540): Q27 (T87 sign-off, merged into T240), Q42 (T98, folded into T212/T166), Q161 (T259, owner 2026-11-05: not necessary).
- Q31 (T93) was answered 2026-10-28 (above, in its row's own words): drainage plus tributary gauges across a continuation link.

### Answered (recently, for the record)
- Q183 (T1039, gauge page chart) — owner 2026-11-23: chart "App's chart, ~260 KB (Recommended)" (option B); ELF "Yes, fix in 0.9.0 (Recommended)".
- Q176 (map line casing, T470) — owner 2026-11-09 (owner checks page): no casing, as built.
- Q178, Q179, Q180 — owner 2026-11-09: Q178 "not right now" (an in-season closure does not change status or map colour; as built, T230/T472). Q179 "sure": shorten / purge the backups to match the privacy page (T559). Q180 "dfo surveys are just in addition to psf curve. psf is primary": PSF is always the curve shown; a DFO survey is added beside it and never replaces it (reverses the 10-24 "DFO where it is better" replace rule; T552).
- Q170 — owner 2026-11-07: B ("Ok let's try that but should add a note that we could tune the parameters on the threshold of magnitude of streams to zoom"); built in T470, the order × zoom fade is one block (`fadeByOrder`).
- Q144 — owner 2026-10-28: the defaults — steelhead the default species; spawning reaches solid and passage reaches DASHED (not a paler ramp); its own map mode (a third choice beside Standard / Salmon, with its own index for painting), not inside Conditions. Built in T199 (decisions.md "Q144").
- Q165 — owner 2026-10-28: PSCIS is in the BC Data Catalogue, treat it as OGL-BC (decisions.md "Q165"). Built in T173: 12,192 PSCIS culverts (barrier / potential / not yet rated) join the obstacles layer; FISS culverts at a PSCIS crossing (50 m) give way.
- Q87, Q140, Q62, Q63 — owner 2026-10-28, built defaults reversed (decisions.md "owner answers Q87, Q140, Q62, Q63"): Beattie Lake's bull trout sits INSIDE the lake's 1 (RULINGS Q27; only Beattie changes); a tapped section outside no-access land leads with its part's banner; slots say "between"; no-count size rules say "X must be …; release any …". Release r-a67545574492 staged, not promoted.
- Stale-item sweep 2026-10-28 (already settled): Q114 — pilot DROPPED by the owner 2026-10-25 (implement_progress.md CURRENT STATE: "no time"; the real review replaces it). Q145 — answered by T211 part 2, built 2026-10-27 with the owner's corrections: the feed host runs `regs.dfo_salmon.live` every 6 h Jun 1–Oct 31, daily otherwise (cron + R2 sync are the owner's, OWNER-TODO). Q32 — obsolete: T95 DROPPED (superseded by T164), salmon scope set by the owner since (T164, T211 option A 2026-10-27). Q125 — answered 2026-10-24 (muted fill, built; the row had stayed in the open table). Q142 — owner 2026-10-28: lake splits deferred ("lake split later" severity); the default "not now; caveat line" stands. Q119 — not an owner question: the unification audit was Claude's own run; Claude re-derives the two pointers by search or drops them (T125).
- Q160 — owner 2026-10-24: combine CLEVER + COFFEE on every forecast day, weighted by measured skill → T177. Q161 — owner: a spot records conditions for its TRIP date (not save time) → built in the T177 lane.
- Q84 (side channel and its river) — owner 2026-10-24 delegated ("do what you think is best just make sure it is sane"): T169 one panel per side-channel water; Q169d (one panel for a long side-channel water) decided yes.
- Q150 / Q137 / Q138 (no-access look) — owner 2026-10-23: streams dotted grey, warning amber in the page banner, own keeps amber (decisions.md).
- Q1, Q3, Q46, Q48, Q49, Q50, Q51, Q52, Q54, Q56, Q57, Q58, Q61, Q64, Q65, Q66, Q67, Q68, Q70, Q71, Q72, Q73, Q74, Q75, Q76, Q77, Q78, Q80, Q81, Q83, Q85, Q86, Q89, Q92, Q93, Q94, Q96, Q97, Q98, Q101, Q102, Q104, Q105, Q106, Q108, Q109, Q110, Q111, Q115, Q116, Q117, Q118, Q120, Q121, Q122, Q127, Q128, Q129, Q131, Q133, Q135, Q136, Q141, Q143 — decided by Claude 2026-10-23 (owner-question triage, owner asked), decisions.md "CLAUDE 2026-10-23 — owner-question triage"; Q29, Q43, Q59, Q60, Q82, Q88, Q90, Q95, Q99, Q100, Q107, Q112 — already answered or obsolete (same record). The owner reads the record and can reverse any.
- Q5, Q34–Q41 (curation review plan, T97) — owner 2026-10-23: "The review app changes seem good" — PLAN.md approved with all §6 defaults; plus: adding splits, rebuilding and continuing review must work (T97, T133–T136); a salmon pass later (T132).
- Q13 (reader gate, T82) — owner 2026-10-23: "you can run the reader gate" — Claude runs it (an Opus agent run, not the parser).
- Q44 (side-channel gauges, T101) — owner 2026-10-23: a side channel's gauge never answers Level for the river it branched from; it answers Flow for that river only when the side channel carries a large share of it. T101 unblocked.
- Q53 (order guard, T102) — owner 2026-10-23: as built — the code (WSC) decides parent/child; stream order only breaks ties on equal or unrelated codes.
- Q45 (eight waters, T89) — owner 2026-10-23: leave them unbound and listed (AWAITING_WATER); never bind a same-name water elsewhere.
- Q55 (release size) — owner 2026-10-22: "we can raise the size limit". TOTAL_GATE 14,000,000 → 15,000,000 B gzip (pipeline/deliver/release/build.py); private land stays in the access file.
- Q33 (the 5 gate-failing entries) — settled by T96: four were a gate bug (province-wide rule holds a region lift), Shuswap 3-26 quotes its whole printed line.
- Delegated calls Q8, Q11, Q12, Q14–Q23, Q25 — decided by Claude (owner call 11), decisions.md 'CLAUDE 2026-10-22 (T81)'; the owner reads the record.
- Palette D stands (T47 part 3) — decisions.md 2026-10-22.
- D4 dark fish cut-outs with no tile are fine (T38) — owner 2026-10-22.
- Image permissions held (T43).
- Main Lake applies to Mine Lake — ccea1eaf, f4552285 (ships in T56).
- Vedder ▲ Chilliwack is one back step — 87ba267c, 7e3cd448.
- Ambiguous cuts: the cut nearest the rule's other end — b17a1a7e, ee6b5d5f, 00057356, f2bc9fd3.
- Level moved into the Conditions layers — 476b06d8.
- One Map entry (call 3) — decided; built in T71.
- p15 continuations (was Q24) — owner 2026-10-21 (late): keep the 10 municipal ones; the button says where it goes
  (88478f44); reversed continuations refused by rule (9defa9a5).
- No ArcGIS key: Esri World Imagery without a key, the key stays optional — owner 2026-10-21 (late).
