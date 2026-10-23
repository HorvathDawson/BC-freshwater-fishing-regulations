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
| T51 | spot similarity forecast ("Like your visit?") | DONE | fa7235c0, 2f5cdc70, a6fd848d, aba28187 | — |
| T52 | Conditions "Expected change" | DONE | 8ad715d6, 1089d4a2, be32f926, 9a050ec1, 7b47a6ab, ee1234e0 | live after the feed republish |
| T53 | unnamed stream shows no regulations | DONE | 74c1decd, 79c74252, 46f3d2f1, 9ef5ea75, 1925ecdc | — (real-data sweep is T57) |
| T54 | no-access streams: fourth colour + banner | DONE | a43a7122, 83f28d54, fec02c29, a5f9b5eb, e08f791b, b291b598, c155f40f, 7c0798d7, bd12dfb9, 4d20b016, 128cc4e0 | live in r-36fbc5326506; Q54/Q55 |
| T55 | sloughs/canals still drawn as lakes | IN PROGRESS | fec02c29, 96b0eb73; prior 669fd065, a59f3dc3, ac8a0e1c | finder DONE (865 candidates) → ★ owner marks them on the review page (artifact Es3Mokt7Fs2yNvm7JgQua8) → reclassify → reach + deliver; polygon-walk lane wf_3976eddc-bba |
| T56 | release rebuild: Twin Lakes + stream polygons + "See X" | DONE | 78a4eed8, 020f4ca8, 794f9c07, cfe405c8, 04357dad | R2 upload (owner) |
| T57 | unnamed streams: real-data regs sweep incl. trib rules | DONE | 9f434cd3, b846c925, a9455d40, 5dac27c8, 21289c24 | — |
| T58 | back follow-ups | DONE | 21d41623, 9ec8114a | — |
| T59 | two rivers meet (Atnarko/Bella Coola); Fraser exit → Annacis | DONE | c5b1e3d0, 36df866f, 463c013b, c432f6df | live in r-36fbc5326506 |
| T60 | consistency auditors (N16, Fable) | OPEN | — | all |
| T61 | cleanup wave 2: old pathways/duplicates | OPEN | — | all (check F07 a6aa4d86 before redoing) |
| T62 | cleanup wave 3: split long files | OPEN | — | all |
| T63 | cleanup wave 4: docs, tests, knip | OPEN | — | all |
| T64 | tiles: S2 size wins + tile rebuild | OPEN | — | all |
| T65 | determinism proof (N12) | OPEN | — | all (heavy) |
| T66 | curation review tool | DONE | a3f66b7c (plan) | plan approved 2026-10-23; build → T96, T97, T133–T136 |
| T67 | braid splitting as a build step | OPEN | 1497beb0 | assessment done (handoff/PLAN/braids/T67-assessment.md) → ★ owner answers R14 Q1/Q2/Q4 → build (braid_sweep.py) → side atlas/reach/deliver/tiles |
| T68 | HYDAT / feed follow-ups | DONE | e25d05fd, 17610988, 0fa29df4, 98817665 | F1/F2 follow-ups in ticket |
| T69 | search on desktop: keyboard leftovers | DONE | 448ebe1b, 389e451a | — |
| T70 | cleanup wave 1, app side + stale worktrees + `pnpm clean` | PARTIAL | 52ec4cb5, 799614a5, 4615ee0f, 1e8cbdb5, ac95e09f, a7947f99, de3cb79d, fd3f8c06, ef11d8e4, 17e4fe8f | merger steps done 2026-10-22; Q17 dead follow-branch → T63 |
| T71 | one Map entry: drop the regs header's Map button | DONE | 4817a2e7, 40d6a47c | — |
| T72 | release LATEST pointer + prune old releases | OPEN | — | all |
| T73 | smaller reach hand-off (N14, disk) | OPEN | — | all (heavy) |
| T74 | native colouring engine + native map gaps | OPEN | — | all |
| T75 | basemap in the release manifest | OPEN | — | all |
| T76 | own gauge across a continuation (Vedder) | DONE | ce29990c, 2470ef08, f648ab5c, 4fba2181, e5dfe217 | live in r-36fbc5326506 |
| T77 | D1 phone testing | OPEN | — | all (owner) |
| T78 | tidal/backwater disclosure on Level | DONE | 4d35d021, c0412c4d, 8185dd28, 5aec64f3, ca52fa7c, c0ea4eb9 | live; Q47–Q49 |
| T79 | imagery corridor P0 + pools validation | OPEN | — | all (owner go/no-go) |
| T80 | small open items (S1 minors, Kitimat witness, D19, flash, search, CI) | PARTIAL | ca5ecb26, 309cd3e8, b08af5e9, ea63d8cb, 46f49485, 53abfd9c, 2c2d3d69, 3a6d20c3, 55b30ec7, 128cc4e0, 84d5919d, fb5d72e5 | two open shapes, item 4/6 (see ticket); Q50–Q52 |
| T81 | settle the pending small owner questions | DONE | 2fb23d92, 116b0e19, 4b197646, bd824a69 | — |
| T82 | T41 leftovers: reader gate run, caveat box O3, sheet height O5 | DONE | f7075463, d8997fbd, 663242b1, 7b9cf940 | — |
| T83 | map: boundaries under water, warm dot, sub-20 pairs | DONE | 111bd363, cbd3e139, e61a6e7d, 7bb14b79, a9dae070 | — |
| T84 | ticket-ledger guard | DONE | caf5ef52 | — |
| T85 | native install size: z13 overzoom + landuse filter (D32) | OPEN | — | all (heavy) |
| T86 | production hosting + ship config | OPEN | — | all (owner) |
| T87 | owner sign-off walk, then final clean-up | OPEN | — | all (owner) |
| T88 | iOS build | OPEN | — | all (owner timing) |
| T89 | rules that bind no water (10 unmatched + 3 no extent) | PARTIAL | 010b5c28, 1ea79ca0, 894854c0, d47a38cd, 5f1eec34 | 8 rows stay unbound (Q45 answered: leave) |
| T90 | Chehalis closure reaches Harrison side channels | DONE | ca9aa451, 9eb7d92f, 9d305ebf, 8fa32076, c8c72d6f | live; T102 part B |
| T91 | Atnarko campsite split location | OPEN | — | all (owner) |
| T92 | gear omission check | DONE | de0581e1, b931f93d, 750ba2ad, 9c78ef20 | — |
| T93 | gauge ↔ section relationship table (policy) | OPEN | — | all |
| T94 | gauge promote: `--only`, demotion guard | DONE | 969ec8fe, 28f03711, 01bf3873, 6753fd6f | first real match+promote dry-run is the owner's |
| T95 | salmon (DFO) regulations: the plan | OPEN | — | all (owner timing) |
| T96 | curation review phase 0: tool safe + the 5 gate-failing entries (ingest R3/prov broken) | DONE | 3e841e31, 3760fd5f, f0228e09, 15723b83 | — |
| T97 | curation review phase 1 (marks that last) + umbrella | PARTIAL | 303f6d9d, 15c04705, 5717c9fb | phase 1 done; phases 2–5 → T133–T136 |
| T98 | tidal water spec: Fraser below Mission (RULINGS C-2, K7/K8) | POSTPONED | — | all; spec ON HOLD |
| T99 | pin the rulings nothing enforces (Z1 base+deltas, 12 ◐ rulings, C-4 citation) | DONE | f8742374, 53d57288, f3bc2b4b, 857bed9a | — |
| T100 | lakes divided into closure areas (K5): Shuswap A/B/C, 67 widened rules | OPEN | — | all |
| T101 | gauges walk through slough/canal polygons + Q44 side-channel rule | DONE | 74e10c03, 2ffbae66, 92c188b4, a2fae3cf, f99f2cd8, bb632aa9, 22835b2f | Q83, Q84 (defaults built) |
| T102 | receiving river never walked into (parent-code check in the walk) | DONE | b6749f62, 60786bc9, 402786c9, b8f5fb3f, 95e78dd5, 0c4a9366, b44461d5, e0bc10a0, 06b8d1cb, 149714b6, bb614474 | part B = one entry check `may_enter` (rides the batch-4 reach rerun); delivered r-941dcc149b17 |
| T103 | one size shape: plain words contradict bands/title (24 strings) | DONE | 326e8848, 677158f8, 92cba91f, 671eabad | live after the next deliver; Q62–Q63 |
| T104 | one "when" in words: titles/plain drop hours and weekdays | DONE | 677158f8, 92cba91f, 671eabad | live after the next deliver; Q64–Q65 |
| T105 | one gauge speaker for map and sheet (54,401 sections disagree) | DONE | df5da53a, d532f31e, 51f77e32, 46af7a18 | Q90–Q93 defaults |
| T106 | one shown quantity: read the declared parameter (08ME030) | DONE | 7276735f, f17523c2, 91ea0abc, 82a37db5 | publisher line lands on the next feed publish |
| T107 | one day's envelope: Python band_for vs TS bandAt | DONE | 4a93cf64, 1399d3d7, a06c1b01, 4965f6ec | — |
| T108 | one "goes on as": continuation vs spans (11 of 17 pairs) | DONE | fb1b12a5, 551c8090, 93d39ad3, c38fbbec | delivered r-b518a2b507fa |
| T109 | one entry-id grammar (z7a reads "Region 7") | DONE | 6c03f5fe, 89fea9c0, 1ca9bc00, 6d731a02 | delivered r-d72230f10993 (fixtures 76e7ef05) |
| T110 | record years per quantity (Level-mode weights) | DONE | ad835645, 31ade2ee, 999b2659, c97c4a98 | delivered r-b518a2b507fa (paint/4) |
| T111 | every cut reaches the walk (second extent's window dropped) | DONE | ea894c8a, 271b2747, b8c00620 | no binding moved; leftovers → T141; delivered r-941dcc149b17 |
| T112 | one entry walker over catalogue extents | OPEN | — | all |
| T113 | one name caser for tiles and bundle (O'rourke) | OPEN | — | all (tiles window + deliver) |
| T114 | one search key for SQL filter and ranking (barriere) | DONE | ebc435e5, 6a9384d3, 21021abb, e7c4d3ca, 4ae97a60, 48f712e9, 2683568e | delivered r-e88ea8112b1a (core 1.09 MB) |
| T115 | one home-filtered region membership (12 Fraser pairs) | DONE | e0461d31, 2699715d, 330c029e, b8c00620 | 12 Fraser + 3 steelhead-twin + 1 licensing pair removed; leftovers → T140; delivered r-941dcc149b17 |
| T116 | one water noun: wetlands called "river" | DONE | 4cf77c7a, 86e6ad40, 0c533d4a, 91b2952e | — |
| T117 | may_enter everywhere: other walks + outlet/WSC/edge-type owners | DONE | c7aa109f, 3b07fefc, 7552cab6, 5a07b674, 10cc45c6, 3aa88557 | 1 licensing section; gauge shed change ships at deliver (Q103); delivered r-941dcc149b17 |
| T118 | one main line for spans and continuation | DONE | 0ac0fc2b, 348f9519, 0f918ea0, 749482e9 | no shipped row changes (13 waters' exit evidence moves, no water_exit row); delivered r-012fb43aa175 |
| T119 | one acceptance gate for ingest, validator and review app | OPEN | — | all (after T112) |
| T120 | one leading origin: weekday rows take the most permissive | DONE | f757d3e0, 74a712ce, e49f688e, 97b93246 | Q66 (app mainRes) |
| T121 | water kind in reach and gauges via kind_of | DONE | 7c11c27b, 35dd2ff1, d5899459, 82c59195 | no binding moved; +18 gauged river polygons at deliver; delivered r-012fb43aa175 |
| T122 | one station liveness (active/realtime/ECCC) | DONE | 9fe57691, 348f9519, 0f918ea0, 749482e9 | no station changes; Q113 cuts vs may_speak; delivered r-012fb43aa175 |
| T123 | one donor state for Conditions and map labels | DONE | 7355fc2e, 5887de97, b78a9bb0, 7a72d388 | app only |
| T124 | one per-section layer list in map | DONE | 4976bf2b, 5887de97, b78a9bb0, 7a72d388 | app only; native route colours wetland sloughs |
| T125 | latent duplicate checks: pins and small helpers | PARTIAL | 370a3978, 7fcdfaab, bffe4431, 81c05b55, 4c0b6272, e81ee1b7, 8d720fb9, 84525b29, 4076c58b, 1fdecc94, ddcd5aca, 1b106ff9, 0e5baf51, 720e9a6f, 388e6f08, a97c63a0, 9835fa2e, 2b86b39f | left: item 8 gauges 9 (Q118), answers 12 + reach 11 (Q119); delivered r-d72230f10993 |
| T126 | forecast reading bugs: CLEVER times 8 h off, 5-day summary on a 10-day run, dots ignore horizon | DONE | e3688e2b, c7ccec1f, d7b9ed24 | live after a feed republish (owner R2 upload) |
| T127 | displaced rule still shows "counts toward" and bounds the card gate | DONE | 2a0458f2, 358b5076, 77566458 | live after the next deliver |
| T128 | sub-cap line names its total ("of your 5 trout and char a day in streams, up to 2 may be…") | DONE | 59aba774, c7d9f57f, 77566458, c410ca42, e839d0cd, 6d768bd6, bb46f5f1, 001e8ffd | — |
| T129 | web request budget: gate first-paint / per-tap bytes, not only the release total | OPEN | — | all |
| T130 | Conditions: zoom out further before the basin heat map; Change chip greyed under Level | DONE | b4f3a40f, cd95bb63, b5218057, c9d48ea7 | Q-T130a/b (defaults built) |
| T131 | spot pins look better and open the spot on tap | DONE | 00398a3b, fafcd21a, 64b96b55 | Q-T131-1..5 (defaults built); native tap with T50 |
| T132 | curation review: salmon (DFO) pass | OPEN | — | later, after the first pass |
| T133 | curation review phase 2: one vintage on screen | DONE | 94e06e52, 5052824f | digests live from r-b518a2b507fa (reach rerun, no binding changed) |
| T134 | curation review phase 3: review screen separate from fixing | DONE | 18ad4bf8, 35f6e2db, b66105e6, 32f5654f, 5a9a2243 | leftovers → T142; row_boxes sidecar run by Claude |
| T135 | curation review phase 4: queue, pre-checks, ticket export | DONE | 8d0f5d76, 19dbc1ee, 9b8de2e3, 0a2be2da | no rebuild |
| T136 | curation review phase 5: parity gate + pilot | DONE | b42fc8cd, 12739188, 9b080497, 371f1a85, 0634891b | parity 3,405 rules, 0 unexplained; scratch E2E false-stale 0; ★ pilot (Q114) |
| T137 | reader shooter: TALL cards shot drops the Other fish list (re-shoot + re-run the gate) | DONE | be8b4693, 111dcea3 | — |
| T138 | reader shooter: chilliwack-p2-row-rb frame misses the rainbow row | DONE | d8997fbd | — |
| T139 | third headless one-kind pool: Liard River watershed (7-53) | DONE | 8fb5f1ac, 85288617, 67d72c8e, c4a3da48 | delivered r-b518a2b507fa |
| T140 | bundle regions of a water + area families ask the one region membership (G3) | DONE | 41f541a9, 35dd2ff1, 82c59195 | latent: no bundle row changes; delivered r-012fb43aa175 |
| T141 | carve-out walk seeds from the extent's cuts; braid cut reports side-channel crossings | DONE | 8c05e725, 35dd2ff1, d5899459, 82c59195 | no binding moved; delivered r-012fb43aa175 |
| T142 | review screen leftovers: focus after click, error wording, partial mark (Q96), draft-guard test | DONE | 9c141d24, 8d0f5d76, 9b8de2e3, 19dbc1ee | Q96 not built (default no) |
| T143 | Conditions horizons say "no forecast" for every gauge (Change works) | OPEN | — | high; live regression |
| T144 | Change: CLEVER + COFFEE (combined), ELF only as last resort with a low-water warning | OPEN | — | research first |
| T145 | spot markers: smaller, capped, deselect on tap off, better look (T131 rework) | OPEN | — | app |
| T146 | no-public-access colour too close to base regulations | OPEN | — | app |
| T147 | "no public access" leads the regulations page, bigger and more obvious | OPEN | — | app; with T146 |
| T148 | sloughs/canals second sweep by FWA feature codes + inside line codes, calibrated on the owner's marks | IN PROGRESS | — | workflow wf_dc7c8506-0c6 |
| T149 | review page: slough complexes marked in bulk; intermittent-lake spot check | OPEN | — | after T148; ★ owner marks |
| T150 | drop the regs header's "Parts" button; the part list lives in the Map view | OPEN | — | after T147 |
| T151 | water Map view: bold emphasis makes the regs legend hard to follow | OPEN | — | with T150 |

## Order for open tickets

Owner 2026-10-22 (back): features and correctness BEFORE the cleanup waves, so the cleanup is done once over finished
code. Heavy jobs run one at a time through scripts/heavy.sh (≤ 4 workers). ★ = needs the owner.

1. **Finishing now:** the batch-3 rebuild (T59, T76, T78, T80, T89, T90 + T96's Shuswap text: reach → binding diff →
   deliver), T52 (Expected change).
2. **Features and correctness (before cleanup):** T54 (no-access streams, built with Q1–Q3 defaults), T51, T68, T82
   (O3/O5; ★ the reader-gate run), T41 / T47 leftovers, T126 (forecast reading bugs), T127, T128 (sub-cap names its total), T130 (heat map zoom + Change chip), T131 (spot pins), T92, T99, T100 ★ (rides a reach rerun), T102 (rides a reach rerun), T94.
   Unification audit 2026-10-22 (wrong answers today): T103, T104, T106, T109 step 1, T116 first (cheap, no binding
   diff); then T105, T107, T108, T110, T120, T114; T113 rides the tiles window (band 5); T111 + T115 ride the reach
   rerun with T102, each with its own reviewed binding diff.
3. **Waiting on the owner:** T101 (Q44), T55 (candidate marks), T67 (braid Q1/Q2/Q4), T97 (curation plan), T91, T93.
4. **Cleanup, once features settle:** T60 (consistency auditors) → T70 leftover → T61 → T62 → T63; T72 and T73 in
   this band (T73 alone, not concurrent with T61's normaliser).
   Unification tickets (latent or plausible): T117 (after T102 part B), T118 (with T108), T112 → T119, T121, T122,
   T123, T124, T125.
5. **Tiles window (one tile build):** T64 + T85, then T75; T129 (web request budget) alongside.
6. **Native and shipping:** T50 (alone, heavy) → T74 → T77 ★ → T86 ★ → T88 ★. T65 (determinism, heavy) in a quiet gap.
7. **Last:** T95 ★, T98 ★ (POSTPONED), T87 ★ (sign-off walk, then delete app/design).

## Owner questions

Every open owner call, with the default we build with until answered (owner call 11: Claude decides the smaller ones
and records them; T81 records them in decisions.md). Answer here or in decisions.md; answered rows move to Answered.

| # | ticket | question | default (built with) | kind |
|---|---|---|---|---|
| Q1 | T54 | Permit-only areas: Tier B with "permit needed" wording, or Tier A? | Tier B, "permit needed" | owner |
| Q2 | T54 | Conditions map: keep the flow colour in no-access areas, or mark them there too? | keep flow colour; Tier A gets only the banner on Conditions | owner |
| Q3 | T54 | Lakes inside Tier A areas (Coquitlam Lake): same fourth colour for the fill? | yes | owner |
| Q4 | T55 | Which finder candidates are sloughs/canals to reclassify? 7 grey polygons (Nation, West Road, Honna)? | nothing reclassified without owner review (dry-run + confirm); the 7 stay grey | owner |
| Q6 | T67 | Braid splitting at build: go, and which plan? | braids stay paused until the owner reads the assessment | owner (BLOCKED until plan) |
| Q7 | T72 | Prune list for old releases | keep live + LATEST + previous | owner approves list |
| Q9 | T77 | When to do phone testing D1a / D1b | D1a when the phone is at hand; D1b after the R2 bucket | owner |
| Q10 | T79 | Imagery go/no-go; SPEC §12 (10 m pack, buffer, sources, licence, server, caps) | no offline imagery before the first native release; skip 10 m; 250 m buffer; online-only on web; pools a study | owner |
| Q26 | T86 | R2 bucket, contact address, app name "Can I Fish This?" (no ArcGIS key: owner 2026-10-21) | keep the name; staging bucket first; publisher every 30 min | owner (credentials) |
| Q27 | T87 | Sign-off: the app looks right; delete app/design | hold until the owner walks it | owner |
| Q28 | T88 | When to build iOS | after the Android ship | owner |
| Q29 | T89 | Rows with several same-name candidates | the candidate in the row's MU; if several, ask | owner (only ties) |
| Q30 | T91 | Where the Atnarko campsite boundary signs are | keep the current binding until confirmed | owner |
| Q31 | T93 | Gauge representativeness policy | the current drainage gate stays default | owner |
| Q32 | T95 | Salmon (DFO) timing and scope | after T50 and the cleanup | owner |
| Q42 | T98 | Lift the tidal-spec hold? | K8 as built until lifted | owner |
| Q43 | T100 | Lake closure areas: a lake part (ring) or an area caveat on the whole lake? | whole lake + area caveat line | owner |
| Q46 | T57 | A nameless lake at the top of a reach stops the tributary walk like a named one (Bull r1, St. Mary r3: 3 sections)? | no change | owner |
| Q47 | T78 | Tidal sentence: closed "Notes and sources" fold, or open under the Level headline? | in the fold | owner |
| Q48 | T78 | Flag other possibly tidal gauges (e.g. 08MH026 Brunette) without a recorded reason? | no; add to data/curated/gauges/tidal.json with a reason | owner |
| Q49 | T78 | Standing non-tidal backwater (08HB074, Comox Lake) — its own sentence? | not flagged | owner |
| Q50 | T80 | Whole "Kootenay Lake" as a search result? | no — search lists its three parts | owner |
| Q51 | T80 | A "See X" row whose water has no other row: a page of its own? | no — the pointer opens X's page | owner |
| Q52 | T80 | Spot photo picker: arm the fallback only where cancel is unsupported? | as built | owner |
| Q54 | T54 | Fourth colour per theme is a recorded compromise (dark ochre 12.7 from closed as a thin line; colour-blind mauve 3.1 from the grey boundary line) — keep? | keep | owner |
| Q56 | T51 | "Like your visit?": today + 4 days, or +1..+5? | today + 4 | owner |
| Q57 | T51 | Similarity weights: flow 45 %, air 20 %, rain before 20 %, wind 10 %, cloud 5 % — right? | as built | owner |
| Q58 | T68 | `update-hydat.sh --stations <file>` copies the refreshed default roster over that file — ok? | as built | owner |
| Q59 | T52 | Expected change: which model colours a river when several forecast it? | CLEVER, then COFFEE, then ELF (alt: the largest move of any model) | owner |
| Q60 | T52 | Use COFFEE for the +1/+3/+5/+10 chips too? (its day 0 runs ~1.45× the gauge) | no; Change only | owner |
| Q61 | T52 | Colour Change by ratio to today (a river already high that triples shows as rising), not by rank for the date? | by ratio; the words give the rank | owner |
| Q62 | T103 | Slot size wording: plain says "none over 60 cm, under 90 cm" where the book says "none between 60 cm and 90 cm" — reword? | keep the existing wording rule (RULINGS P37) | owner |
| Q63 | T103 | "Keep only lake trout, none over …" can read as "only lake trout may be kept" — reword the house style? | unchanged | owner |
| Q64 | T104 | Slocan "12:00 to 00:00" vs the book's "12 pm to midnight" — say noon/midnight (needs an export rebuild)? | unchanged | owner |
| Q65 | T104 | Licensing labels still word "when" their own way (dates.When.words); routing through when_words adds "as printed:" on 2 labels — do it now? | leave until licensing labels are reworked | owner |
| Q66 | T120 | The app decides the leading origin again (core regs/card.ts mainRes) — should the pipeline ship it per fish? | fold into T125 (cleanup) | owner |
| Q67 | T130 | Go below z8 by drawing the heat map under the big rivers? | no — one answer per zoom (rivers from z8) | owner |
| Q68 | T130 | Flow arrows start at z10, so z8–z10 shows coloured rivers without arrows — start them earlier? | as built | owner |
| Q69 | T131 | Spot pin colour: neutral ink on card, or a hue of its own? | neutral ink | owner |
| Q70 | T131 | Several spots on exactly one point: open the Spots list, or fan them out? | open the Spots list | owner |
| Q71 | T131 | A spot opened from a water's map: its tab bar leaves the water for that tab? | yes | owner |
| Q72 | T131 | Selected spot's name hidden when a neighbour pin sits there — drop it or place it below the pin? | drop it | owner |
| Q73 | T131 | On Conditions a spot pin can cover a gauge reading label and takes the tap — ok? | as built | owner |
| Q74 | T126 | Gauge dots under a forecast horizon or Change follow the horizon (no dot on a day no forecast reaches) — or stay at Now with a legend note? | follow the horizon | owner |
| Q75 | T126 | Dot label under Change: "3× today" (the station's own ratio)? | as built | owner |
| Q76 | T106 | A record with no declared quantity (old feed file, spot saved before T106) reads as discharge (the publisher's default)? | yes | owner |
| Q77 | T107 | A value exactly on p10/p90 reads "at or below/above the 10th/90th percentile" (a dry creek at 0 when p10 is 0 included)? | yes | owner |
| Q78 | T114 | No fallback to raw-name matching when the app meets an older bundle without search keys (that fallback is the second rule T114 removes)? | no fallback; ship app and release together | owner |
| Q79 | T116 | Regs page says "river" for a stream ("this river's rule", "Whole river") while search rows say "Stream" — switch the page to "stream"? | keep "river" on the page | owner |
| Q80 | T97 | A mark's binding counts km per named water (to 0.1 km), not total sections — so a new split alone does not stale zone rules and walks crossing it? | km per named water | owner |
| Q81 | T97 | A split added or moved on a water stales EVERY entry matched to that water ("binding moved"), not only rules whose reach changed? | every entry on that water | owner |
| Q82 | T97 | "unsure" is a mark verdict (the "?" outcome) beside verified / wrong / deferred? | yes | owner |
| Q83 | T101 | "Large share" for a side channel's Flow to answer its river: drainage ≥ 0.5 of the river's (only Hanson/Lewis, Seton Canal, Bell/Nelson reach it today)? | 0.5 | owner |
| Q84 | T101 | Should a side channel also NOT take its river's Level (the walk cuts it inward; `mainstem_of` still gives an adopted channel line the river's full answer)? One rule both ways, or only your outward rule? | walk cuts inward; adopted lines keep the river's answer — so Nicomen's main channel alternates between the Fraser's answer and its own (historical-only, grey today). Make it one: (a) adopt the connector pieces too, or (b) the channel's own gauges lead on all its water | owner |
| Q85 | T133 | While an entry awaits its build, the review tool still shows the live binding labelled "preview" (P cannot be answered; bundle copy and answer pane withheld)? | as built | owner |
| Q86 | T133 | A moved split is only detected after the next atlas build records split anchors (until then only added splits, by id)? | leave it to the next --atlas build | owner |
| Q87 | T139 | Beattie Lake 7-57: does the Liard watershed's "bull trout 1 (30–50 cm)" sit BESIDE the lake's own "trout 1" (2 a day) or INSIDE it (1 in all)? | beside (the reader's ruling; headline "2 a day per angler") | owner |
| Q88 | T108 | RULINGS P10 called a creek alone at the receiver's top a "Mouth" unless the magnitudes match; p15 kept the 10 municipal continuations — which wins for goes-on-as? | as built (see T108) | owner |
| Q89 | T110 | 1,302 sections lose their Level answer because a distant donor's short level record falls under the minimum weight — go silent ("no baseline")? | yes, same rule as every panel | owner |
| Q90 | T105 | A new spot saves the sheet's lead station (the speaker) rather than section_gauge's? | unchanged for now (follow-up) | owner |
| Q91 | T105 | The 54,386 matched-only sections now say "No gauge on this water is close enough in size" — wording ok? | as built | owner |
| Q92 | T105 | Lake reason: "This lake has its own gauge, read here directly; nothing is estimated for it from other water." | as built | owner |
| Q93 | T105 | Give the design fixture's Chilliwack a real panel so design screens show a chart? | no | owner |
| Q94 | T134 | Review tier S0 holds 150 split points, not PLAN's 165 (15 old "[osm-candidate … VERIFY]" stamps were later answered by "[human-review]")? | 150; the "every split" toggle shows all 409 | owner |
| Q95 | T134 | The A check opens your app (:39218) as a deep link in a new tab, not an iframe? | deep link until you approve an iframe on the reviewer's own port | owner |
| Q96 | T134 | While an entry awaits its build (only T and R answerable), allow a partial "text and rules verified" mark? | no (T142 builds it if yes) | owner |
| Q97 | T134 | T134 counts as DONE with the ≤ 1.5 min per entry target measured only for the tool (~2 s); the human timing is the T136 pilot? | yes | owner |
| Q98 | T115 | Fraser piece 356364114:699253 (home Zone 7A) also loses the steelhead twins r1b/r2b/r4b and steelhead_targeting_known (AGENTS 54's reading of the Fraser in 7A)? | yes | owner |
| Q99 | T115 | The bundle's regions_of_waters (which neighbouring region a printed exemption carries to, G3) moves to home membership too? | yes, as T140 with a measured diff | owner |
| Q100 | T111 | The carve-out walk seeds from the extent's cuts too (differs only on braid side channels)? | yes, as T141 with a measured diff | owner |
| Q101 | T117 | Edge type 1150 (secondary flow in a wetland): count it as a side channel for the gauge panels? | unchanged (neither main flow nor side channel) | owner |
| Q102 | T117 | Burnaby Lake's two equal-order main-flow outlets (Brunette River + an added stream) both count as the lake's own river? | yes: on an order tie every main-flow outlet counts | owner |
| Q103 | T117 | The gauge shed climbs the way the walk does: the Theodosia (08GC005) and Quinsam (08HD034) diversion gauges no longer speak for water above their canal (the river's own gauge takes over on 174 + 107 sections); 24 sections lose every gauge; a gauge ON a canal reads only its own piece? | as built | owner |
| Q104 | T117 | build_panels.candidates (reading propagation) keeps its own rules rather than stepping through may_enter? | keep (it propagates readings, it collects no tributary set) | owner |
| Q105 | T135 | Pre-check codes: the audit's A1–A13 were never committed, so the 12 pre-checks are named for what they check (A = fact, B = heuristic)? | these codes; name any missing audit code | owner |
| Q106 | T135 | "Undrawn part" is a risk factor (P1 starts with closures with undrawn parts; moves 1 entry P4 → P3)? | yes | owner |
| Q107 | T135 | The review order (all regions, S0 split points first) is the queue's default; "book (page)" one click away? | yes | owner |
| Q108 | T135 | A split added on a water stales every marked entry on that water, even when no rule names the split (T133's binding fingerprint)? | yes (as Q81) | owner |
| Q109 | T135 | A cut picked by hand (cut_pick) is no longer the "ambiguous cut" risk (stays a B pre-check)? | yes | owner |
| Q110 | T123 | A donor with no forecast at the horizon is labelled "no forecast" on the map (was "not reporting")? | yes | owner |
| Q111 | T121 | A gauge on standing water means lake OR wetland (no live station affected)? | yes | owner |
| Q112 | T121 | 08MH024 (Fraser at Mission) now speaks, weakly, for the Vedder Canal polygon of the Vedder — keep, or should the Vedder's own gauge win there? | keep (follows the walk and drainage test) | owner |
| Q113 | T122 | Gauge cuts follow the roster's realtime snapshot, sheds follow ECCC Active: 38 active-but-silent stations have a shed and no cut. Make cuts follow Active (atlas rebuild, moves section ids)? | unchanged | owner |
| Q114 | T136 | Run the 30-entry curation pilot (handoff/PLAN/curation-review/PILOT.md, ~1 h; then `python -m pipeline.regs.review.gate pilot-report --pilot handoff/PLAN/curation-review/pilot.json`)? | ★ yours to run; PLAN's "no tool blocker" check stays open until then | owner |
| Q115 | T136 | After a split the review tool prints `pipeline build --promote` WITH tiles (~40 min more), since --no-tiles cannot pass deliver's vintage check? | yes | owner |
| Q116 | T136 | A tool blocker in the pilot is a deferred mark whose note starts "TOOL:" (no new verdict)? | yes | owner |
| Q117 | T136 | The 9 book-known steelhead-twin rules read "needs corpus" in the review app (the bundle binds 0–15 sections): should the app read the run for them? | no (explained by design) | owner |
| Q118 | T125 | promote.evidence checks a station's name only against the match's display name (Arrow Reservoir vs Lower Arrow Lake never auto-promote): fix as its own ticket (new StationMatch field, match.json regenerate, owner-run promote)? | not built; say if wanted | owner |
| Q119 | T125 | Audit references for item 8 "answers 12" (fish-list wording) and "reach 11" (double computation) — the copies could not be found from the ticket? | left open | owner |
| Q120 | T109 | Bump the regs store section version so the app refuses a release built before prov.region (it would tag rank-3 rules "Region rule", never a wrong region)? | no bump | owner |
| Q121 | T125 | Item 4 renames two unbound reasons (between_cuts_not_ordered → parallel_branches; outside_*) without a POLICY_VERSION bump (no live rule moved)? | no bump | owner |
| Q122 | T150 | Where the "Parts of this water" list sits in the Map view? | a collapsible list under the map | owner |
| Q123 | T151 | How the water Map view marks the selected part without bold lines? | dim the other parts; one line width | owner |

### Answered (recently, for the record)
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
