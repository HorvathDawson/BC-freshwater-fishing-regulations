# Tickets — the sole source of future work

Owner, 2026-10-22: "make sure it uses proper ticket system … add anything else we know we need to do to the tickets.
use that as the sole source of future work."

## How the system works

- **Tickets are the only backlog.** Work that is not a ticket here is not planned. A progress note, a decisions.md row,
  a memory note, a REPORT "open" line or an audit item that needs doing becomes a ticket (or a line in one) before
  anyone works on it. implement_progress.md records what happened; it does not hold the queue.
- **One file per ticket:** `tickets/Tnn-short-slug.md`, numbered in order (the next free number after the last row of
  the ledger below; never reuse a number). Small related items go into one ticket as numbered lines.
- **Format** (as T56–T95):
  ```
  # Tnn — title

  Status: OPEN.

  Source: where it came from (owner quote + date, file:line, commit).
  ## Work            what to do
  ## Acceptance      how we know it is done (tests, mutation pins, shots, numbers)
  Depends: … Size: S/M/L. Owner: no | yes (what for). Default: the call we build with until the owner answers.
  ```
- **Status values** (the `Status:` line sits under the title of every ticket):
  - `OPEN` — not started.
  - `IN PROGRESS` — a lane is on it; name the lane/worktree.
  - `PARTIAL` — some built and merged; say what is left and where it went (a follow-up ticket).
  - `BLOCKED (owner)` — cannot proceed without the owner and no default is safe.
  - `POSTPONED` — deliberately parked, with the owner's word.
  - `DONE` — merged into `redesign/stream-sections`, acceptance met.
  - `DROPPED` — will not be done; one line why (owner or recorded Claude call).
- **Adding a ticket:** write the file, add a ledger row, add any owner call to "Owner questions" with its default,
  commit with explicit paths (`git commit -- <paths>`; never `-A`/`-a`/`git add .` — other lanes share the branch).
- **Closing a ticket (the lane's last step):** set `Status: DONE — <commit hashes>` (plus one line of anything
  notable), update the ledger row (status, commits, what's left "—"), move any answered owner question to Answered,
  commit those files with explicit paths. Anything found and not done becomes a new ticket, not a footnote.
- **Owner calls never block work:** every open call carries a recommended default; build with it and record it
  (owner call 11). The owner answers in one place: the "Owner questions" table below.
- T84 adds a guard to `pnpm check` that keeps this ledger whole.

## Ledger

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T00 | core `regs/` skeleton: model, line, words; `regulations.ts` rewritten | DONE | 40b2c67d | — |
| T01 | release loader, the one day, the tapped section to the water screen | DONE | c5119071 | — (loader prop, no `/regs.sqlite` route; D22) |
| T02 | blob codec (fflate dict-inflate + UTF-8 decode) | DONE | 53ea8298 | — |
| T03 | `openRegsStore`: format, digests, section versions, refusal | DONE | d5cb3680, f0bea808 | — |
| T04 | store reads | DONE | bb28757f, d6d3ee0e | — |
| T05 | core tap: `segmentIndex`, `weekdayOf`, `profileIndex`; taps.json parity | DONE | f9d3cf12, 89f0d3d2, 46ccd2d0 | — |
| T06 | hydrate 1: rule views, species names, `describe()` + oracle | DONE | fc69e20a, c764595b, 1970139d | — |
| T07 | hydrate 2: ladder verdicts, `settle()` | DONE | 94cb6eca | — |
| T08 | hydrate 3: rows model | DONE | b64af2ed, 279236b5 | — (`dayStatus` dropped) |
| T09 | `loadRegsInputs()` | DONE | 2721902a | — |
| T10 | regs fixture store + conformance suite | DONE | c7b7708c | — |
| T11 | ui hooks, the one day | DONE | c91ea1a1 | — |
| T12 | golden parity harness | DONE | d41f8522, 087388c2, 6e9e0443 | — |
| T13 | presenter: year strip + status line | DONE | 4e9db5ab, 3c5d3628 | — |
| T14 | presenter: closed banner + chain + tidal banner | DONE | 7affdf01, 5b7e858d, cb71e1d0 | — |
| T15 | presenter: inherited line, where-you-can't-fish, possession | DONE | a1f32fee | — |
| T15b | presenter: top caveat blocks | DONE | 9dec0425, 3a304861 | — |
| T16 | presenter: header | DONE | b763f12d, d2f55c3b | — (header Map button removal is T71) |
| T17 | presenter: group head | DONE | 670521fc, fe2014cb, cb71e1d0 | — |
| T18 | presenter: quota tree | DONE | ae0a8abf, fe2014cb | — |
| T19 | presenter: not kept, How it adds up, quiet line, Other fish | DONE | 365a172f, fe2014cb, cd0a2a59 | — |
| T19b | presenter: plain rows + "How this was decided" | DONE | 5cda8390, 8a1c3c52 | — |
| T20 | fish manifest → `fish.generated.ts` + image map | DONE | 803bc30d | — |
| T21 | `buildRegsPage()` Keep slice + Keep parity | DONE | 3665f151, a445aa91, b824f4ee | — |
| T22 | theme + type tokens | DONE | 24e63e82, 8f010a17, 44a09ac2 | — |
| T23 | RN `RegsScreen`, `RegsHeader`, the one scroll | DONE | 830312a5 | — |
| T24 | RN `YearStrip` | DONE (in T21) | 3665f151 | — |
| T25 | RN banner, inherited, where-not, possession, `LineText` | DONE (in T21) | 3665f151 | — |
| T26 | RN group components | DONE (in T21, other names) | 3665f151 | — |
| T27 | RN `OtherFish`, `KeepTab`, Keep render gates | DONE (in T21) | 3665f151 | — |
| T28 | core `shortOf`, `pinChain`, `tabInView` | DONE | 14b90015 | — |
| T29 | RN anchors, `PinBar`, tab-in-view, scroll-to | DONE | 61ef4fe6, e13452ff | — |
| T30 | presenter: gear lines, changes, sources, closed note, day line | DONE | c5770fa4, 9d408f38 | — |
| T31 | presenter: bait list | DONE | efa0a841 | — |
| T32 | presenter: ways to fish, boats, reference fold | DONE | b76a9941 | — |
| T33 | presenter: licence | DONE | b3ab85c5, a43a0de2, cd0a2a59 | — |
| T34 | RN Gear tab components | DONE | f93594ee | — |
| T35 | RN Methods/Licence + parity | DONE | 28e6d404, aeeb52de | — |
| T36 | presenter: every sheet | DONE | abfc42ea, d82411c5, d91d9ba3 | — |
| T37 | RN `SheetHost` + sheet bodies | DONE | c1bffeef, c3566bc2, d91d9ba3 | — (O5 → T82) |
| T38 | RN `AdiposeArt` + How to tell | DONE | e924a347, 75a8505c | — |
| T39 | dark theme + palette gate | DONE | e4a5bef9, 2ebe4055 | — |
| T40 | a11y, 200% text, hit areas, style-token gate | DONE | d9cb96fa, cdc1edb8 | — |
| T41 | web parity walk, side-by-sides, reader gate | PARTIAL | 279ec371, a2159a32, 5dcce89d, d9e87835, 459811a8 | reader run + O3 + O5 → T82; native walk → T50 |
| T42 | edge states | DONE | b2e116c6, 0e36a2d5 | — (caveat look → T82) |
| T43 | Rule book links, fish credits | DONE | b4e5734c, 75a8505c | — |
| T44 | one WaterFrame | DONE | 825cf1a8, f2e98e0d | — |
| T45 | desktop layout for Regulations | DONE | 919cdb6f, 412f4bfc | — |
| T46 | fish ID on desktop | DONE | e3332e62 | — |
| T47 | status colours you can tell apart | PARTIAL | daf61b67, 5473776d, d6a5a51c, 056d6cc5, e6754333 | boundaries under water, warm dot hue, sub-20 pairs → T83 (palette D closed) |
| T48 | search in the top bar | DONE | d1b5680a, f733ec5e, 81000acf, 349968b0, 3f8c277f | — (keyboard → T69) |
| T49 | map's day: calendar icon + date sheet | DONE | 02570d07, 1fc9be2e, 9db57864 | — |
| T50 | Android emulator parity + system bars | OPEN | 7da6deff, 294dd4d5 (ticket) | all; after T61–T63, heavy, alone |
| T51 | spot similarity forecast ("Like your visit?") | OPEN | a7b94f4e (ticket) | all |
| T52 | Conditions "Expected change" | OPEN | 6ffda443 (ticket) | all (pipeline daily series + app chip) |
| T53 | unnamed stream shows no regulations | DONE | 74c1decd, 79c74252, 46f3d2f1, 9ef5ea75, 1925ecdc | — (real-data sweep is T57) |
| T54 | no-access streams: fourth colour + banner | OPEN | a43a7122 (ticket) | all; Q1–Q3 build with defaults |
| T55 | sloughs/canals still drawn as lakes | IN PROGRESS | fec02c29, 96b0eb73; prior 669fd065, a59f3dc3, ac8a0e1c | finder DONE (865 candidates) → ★ owner marks them on the review page (artifact Es3Mokt7Fs2yNvm7JgQua8) → reclassify → reach + deliver; polygon-walk lane wf_3976eddc-bba |
| T56 | release rebuild: Twin Lakes + stream polygons + "See X" | IN PROGRESS | b9a832ca, ad77fda5, f4552285, 669fd065, 7b64c558, 020f4ca8 (binding diff) | merge lanes wf_3976eddc-bba + wf_e4e2b70f-f21; swap full_twin; `deliver all`; panels test; re-cut fixtures; strict + smoke |
| T57 | unnamed streams: real-data regs sweep incl. trib rules | OPEN | — | all (T53 merged: unblocked) |
| T58 | back follow-ups | OPEN | (search-back merged cc4fcee1, f74b50a4) | split lake stacked twice; parts-sheet rows; stale slow map tap; ‹ label names where back goes |
| T59 | two rivers meet (Atnarko/Bella Coola); Fraser exit → Annacis | OPEN (app part done) | 87ba267c, 7e3cd448 | pipeline `continuation.py` rule; Fraser `water_exit`; deliver |
| T60 | consistency auditors (N16, Fable) | OPEN | — | all |
| T61 | cleanup wave 2: old pathways/duplicates | OPEN | — | all (check F07 a6aa4d86 before redoing) |
| T62 | cleanup wave 3: split long files | OPEN | — | all |
| T63 | cleanup wave 4: docs, tests, knip | OPEN | — | all |
| T64 | tiles: S2 size wins + tile rebuild | OPEN | — | all |
| T65 | determinism proof (N12) | OPEN | — | all (heavy) |
| T66 | curation review tool | PARTIAL | a3f66b7c (plan) | owner reads curation-review/PLAN.md; build → T96, T97 |
| T67 | braid splitting as a build step | OPEN | 1497beb0 | assessment done (handoff/PLAN/braids/T67-assessment.md) → ★ owner answers R14 Q1/Q2/Q4 → build (braid_sweep.py) → side atlas/reach/deliver/tiles |
| T68 | HYDAT / feed follow-ups | OPEN | — | Forecasts line; roster `--stations`; R2 upload step |
| T69 | search on desktop: keyboard leftovers | OPEN | — | focused-but-closed; tab order; keyboard-walk test |
| T70 | cleanup wave 1, app side + stale worktrees + `pnpm clean` | OPEN | — | all |
| T71 | one Map entry: drop the regs header's Map button | OPEN | — | all |
| T72 | release LATEST pointer + prune old releases | OPEN | — | all |
| T73 | smaller reach hand-off (N14, disk) | OPEN | — | all (heavy) |
| T74 | native colouring engine + native map gaps | OPEN | — | all |
| T75 | basemap in the release manifest | OPEN | — | all |
| T76 | own gauge across a continuation (Vedder) | OPEN | — | all |
| T77 | D1 phone testing | OPEN | — | all (owner) |
| T78 | tidal/backwater disclosure on Level | OPEN | — | all |
| T79 | imagery corridor P0 + pools validation | OPEN | — | all (owner go/no-go) |
| T80 | small open items (S1 minors, Kitimat witness, D19, flash, search, CI) | OPEN | — | all |
| T81 | settle the pending small owner questions | OPEN | — | all |
| T82 | T41 leftovers: reader gate run, caveat box O3, sheet height O5 | OPEN | — | all |
| T83 | map: boundaries under water, warm dot, sub-20 pairs | OPEN | — | all |
| T84 | ticket-ledger guard | OPEN | — | all |
| T85 | native install size: z13 overzoom + landuse filter (D32) | OPEN | — | all (heavy) |
| T86 | production hosting + ship config | OPEN | — | all (owner) |
| T87 | owner sign-off walk, then final clean-up | OPEN | — | all (owner) |
| T88 | iOS build | OPEN | — | all (owner timing) |
| T89 | rules that bind no water (10 unmatched + 3 no extent) | OPEN | — | all |
| T90 | Chehalis closure reaches Harrison side channels | OPEN | — | all |
| T91 | Atnarko campsite split location | OPEN | — | all (owner) |
| T92 | gear omission check | OPEN | — | all |
| T93 | gauge ↔ section relationship table (policy) | OPEN | — | all |
| T94 | gauge promote: `--only`, demotion guard | OPEN | — | all |
| T95 | salmon (DFO) regulations: the plan | OPEN | — | all (owner timing) |
| T96 | curation review phase 0: tool safe + the 5 gate-failing entries (ingest R3/prov broken) | OPEN | — | all |
| T97 | curation review phases 1–5 (marks, one vintage, review screen, queue, gate) | BLOCKED (owner) | — | all; owner approves PLAN.md |
| T98 | tidal water spec: Fraser below Mission (RULINGS C-2, K7/K8) | POSTPONED | — | all; spec ON HOLD |
| T99 | pin the rulings nothing enforces (Z1 base+deltas, 12 ◐ rulings, C-4 citation) | OPEN | — | all |
| T100 | lakes divided into closure areas (K5): Shuswap A/B/C, 67 widened rules | OPEN | — | all |
| T101 | gauges walk through slough/canal polygons (held) | BLOCKED | 8d57e759..de39b70c (branch, unmerged) | ★ owner Q44 → merge + deliver |

## Order for open tickets

Heavy jobs run one at a time through scripts/heavy.sh (≤ 4 workers). ★ = needs the owner (works with its default
meanwhile unless marked BLOCKED).

1. **In flight — finish first:** T56 (merge wf_3976eddc-bba + wf_e4e2b70f-f21, then the one rebuild),
   T66 (plan written → ★ owner reads PLAN.md), T67 (assessment → ★ owner), T55 (finder → ★ owner reviews candidates).
2. **Right after T56:** T57 (T53 is merged), T82 (fixtures re-cut; ★ owner launches the reader run), T58, T69, T71,
   T83, T81 (cheap, unblocks recorded calls), T84, T96 (ingest for R3/prov is broken until it lands).
3. **Ride the next reach rerun / deliver together** (one heavy job): T59 → T76, T89, T90, T78, T80 item 3 (D19);
   T91 ★ when the owner has confirmed the sign location. Then T73 (reach format, alone; not concurrent with T61's
   normaliser).
4. **Cleanup waves, in order:** T70 → T61 → T62 → T63 (knip starts clean after T70). T72 after T56 settles the live
   release. T92, T94, T60, T99 fit anywhere in this band.
5. **Tiles window (one tile build):** T64 + T85, then T75. T65 (determinism proof, heavy, background, alone) in a
   quiet window between heavy jobs.
6. **Native, after the cleanup:** T50 (alone, heavy) → T74 → T77 ★ (phone) → T86 ★ (R2, keys, name) → T88 ★.
7. **Features / pipeline additions:** T54 (build with defaults), T52, T51, T68, T93 ★, T79 ★, T97 ★ (after the owner
   approves the plan), T100 ★ (rides a reach rerun).
8. **Last:** T87 ★ (owner sign-off walk, then delete app/design), T95 ★ (salmon plan after the app release),
   T98 ★ (POSTPONED until the owner lifts the tidal hold).

## Owner questions

Every open owner call, with the default we build with until answered (owner call 11: Claude decides the smaller ones
and records them; T81 records them in decisions.md). Answer here or in decisions.md; answered rows move to Answered.

| # | ticket | question | default (built with) | kind |
|---|---|---|---|---|
| Q1 | T54 | Permit-only areas: Tier B with "permit needed" wording, or Tier A? | Tier B, "permit needed" | owner |
| Q2 | T54 | Conditions map: keep the flow colour in no-access areas, or mark them there too? | keep flow colour; Tier A gets only the banner on Conditions | owner |
| Q3 | T54 | Lakes inside Tier A areas (Coquitlam Lake): same fourth colour for the fill? | yes | owner |
| Q4 | T55 | Which finder candidates are sloughs/canals to reclassify? 7 grey polygons (Nation, West Road, Honna)? | nothing reclassified without owner review (dry-run + confirm); the 7 stay grey | owner |
| Q5 | T66/T97 | Curation review: approve handoff/PLAN/curation-review/PLAN.md (phases 0–5, ~3–4 weeks before a human starts)? | phase 0 (T96) goes ahead; phases 1–5 wait | owner (BLOCKS T97) |
| Q6 | T67 | Braid splitting at build: go, and which plan? | braids stay paused until the owner reads the assessment | owner (BLOCKED until plan) |
| Q7 | T72 | Prune list for old releases | keep live + LATEST + previous | owner approves list |
| Q8 | T76 | Gauge across `water_continue` counts as own? | yes, `water_continue` only | delegated |
| Q9 | T77 | When to do phone testing D1a / D1b | D1a when the phone is at hand; D1b after the R2 bucket | owner |
| Q10 | T79 | Imagery go/no-go; SPEC §12 (10 m pack, buffer, sources, licence, server, caps) | no offline imagery before the first native release; skip 10 m; 250 m buffer; online-only on web; pools a study | owner |
| Q11 | T80 | D19 klmb_1010 pooled row | (a) pipeline-side `group` | delegated |
| Q12 | T82 | O3: timed caveat in a grey box? O5: sheet height | O3 same caveat box as the other top caveats; O5 size to content up to the prototype's cap | delegated |
| Q13 | T82 | Launch the reader-gate agent run | after T56 | owner launches |
| Q14 | T83 | Region/MU boundaries above or under the water status lines (T47 part 1 Q4) | under the status lines, above fills | delegated |
| Q15 | T83 | Warm temperature dot reuses restricted plum | own hue, not a status colour | delegated |
| Q16 | T83 | Sub-20 contrast pairs (thin lines, permit/temperature, dark boundary 15, dark closed/base 19.1) | lift where cheap; else recorded exceptions | delegated |
| Q17 | T81 | T48: keep "open on the best match" preview? | no | delegated |
| Q18 | T81 | T48: "Add a spot here" wording | close (owner removed it) | delegated |
| Q19 | T81 | T49 `dayRange` | this calendar year | delegated |
| Q20 | T81 | D18 bait `also_allowed` | hidden | delegated |
| Q21 | T81 | T44: name size on Conditions/Map, desktop column, Map full-bleed, wheel outside the column | as built | delegated |
| Q22 | T81 | Selected-tab cue | as built | delegated |
| Q23 | T81 | Dark water names near status.base / flow f5 | as built; revisit in T83 gates | delegated |
| Q25 | T81 | Prototype bugs: closed-all-year "."; `pinUpdate` throws on 25/134 states | close: the prototype is a reference | delegated |
| Q26 | T86 | R2 bucket, contact address, app name "Can I Fish This?" (no ArcGIS key: owner 2026-10-21) | keep the name; staging bucket first; publisher every 30 min | owner (credentials) |
| Q27 | T87 | Sign-off: the app looks right; delete app/design | hold until the owner walks it | owner |
| Q28 | T88 | When to build iOS | after the Android ship | owner |
| Q29 | T89 | Rows with several same-name candidates | the candidate in the row's MU; if several, ask | owner (only ties) |
| Q30 | T91 | Where the Atnarko campsite boundary signs are | keep the current binding until confirmed | owner |
| Q31 | T93 | Gauge representativeness policy | the current drainage gate stays default | owner |
| Q32 | T95 | Salmon (DFO) timing and scope | after T50 and the cleanup | owner |
| Q33 | T96 | The 5 gate-failing entries: Shuswap `min_cm` 50/60; zp set_lining, spear_fishing, bait, white_sturgeon_licence exemptions | fix Shuswap against the book; loosen the gate only where the book exempts across unshared ground | owner |
| Q34 | T97 | May the app's start link back the review pane? | yes, read-only, on the reviewer's own port; deep link until approved | owner |
| Q35 | T97 | Who may fix as well as mark? | reviewers mark only; fix mode owner-only; wrong/unsure marks become tickets | owner |
| Q36 | T97 | Review overrides.json, steelhead_waters, cross_references, DFO entries, name variants in this pass? | not in this pass; the `answer` fingerprint surfaces their effects | owner |
| Q37 | T97 | Per-rule or per-entry marks? | per entry for water rows, per rule for the 132 zone entries, per point for splits | owner |
| Q38 | T97 | Offer sampling for P4? | off; full review | owner |
| Q39 | T97 | Retire the ingest ledger or move it into curated data? | retire; new ids only + `--replace-entry` | owner |
| Q40 | T97 | Which PDF is the book? | the repo copy, sha256 on every mark, one-time check of chapter pages vs gov.bc.ca | owner |
| Q41 | T97 | Start review before phase 3? | no | owner |
| Q42 | T98 | Lift the tidal-spec hold? | K8 as built until lifted | owner |
| Q43 | T100 | Lake closure areas: a lake part (ring) or an area caveat on the whole lake? | whole lake + area caveat line | owner |
| Q44 | T101 | May a gauge on a side channel/slough speak for the river it branched from? (Nicomen level gauges would answer ~71,668 Fraser sections) | no for Level; Flow only when the side channel carries a large share of the river | owner |

### Answered (recently, for the record)
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
