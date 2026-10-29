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
| T50 | Android emulator parity + system bars | OPEN | 7da6deff, 294dd4d5 (ticket), T74 follow-ups ee5d259f, e8225cd0, 58ea79bf | marks/PDF/insets/sheets/font-size restart fixed; back table + landscape walked; regs scroll not kept on recreate |
| T51 | spot similarity forecast ("Like your visit?") | DONE | fa7235c0, 2f5cdc70, a6fd848d, aba28187 | — |
| T52 | Conditions "Expected change" | DONE | 8ad715d6, 1089d4a2, be32f926, 9a050ec1, 7b47a6ab, ee1234e0 | live after the feed republish |
| T53 | unnamed stream shows no regulations | DONE | 74c1decd, 79c74252, 46f3d2f1, 9ef5ea75, 1925ecdc | — (real-data sweep is T57) |
| T54 | no-access streams: fourth colour + banner | DONE | a43a7122, 83f28d54, fec02c29, a5f9b5eb, e08f791b, b291b598, c155f40f, 7c0798d7, bd12dfb9, 4d20b016, 128cc4e0 | live in r-36fbc5326506; Q54/Q55 |
| T55 | sloughs/canals still drawn as lakes | IN PROGRESS | fec02c29, 96b0eb73; prior 669fd065, a59f3dc3, ac8a0e1c, d4a22561 | review app "Sloughs" tab: 936 pre-marked stream (801 narrow ≤ 44 m + 135 wide marsh), 324 on the owner list (113 intermittent) → ★ owner confirms (one / accept all visible = marks, scope `slough`) → fold into rivers → reach + deliver |
| T56 | release rebuild: Twin Lakes + stream polygons + "See X" | DONE | 78a4eed8, 020f4ca8, 794f9c07, cfe405c8, 04357dad | R2 upload (owner) |
| T57 | unnamed streams: real-data regs sweep incl. trib rules | DONE | 9f434cd3, b846c925, a9455d40, 5dac27c8, 21289c24 | — |
| T58 | back follow-ups | DONE | 21d41623, 9ec8114a | — |
| T59 | two rivers meet (Atnarko/Bella Coola); Fraser exit → Annacis | DONE | c5b1e3d0, 36df866f, 463c013b, c432f6df | live in r-36fbc5326506 |
| T60 | consistency auditors (N16, Fable) | DONE | 2ac30e42, 9259fa16, ed367f33, d02e1448 | six mutation-pinned gates in pnpm check; F1–F5 fixed; copy move → T180; Q162 |
| T61 | cleanup wave 2: old pathways/duplicates | PARTIAL | 9db616f0, 884fe9c7, 46cab8cd, baeda569, 0ad1a84, bb22f3eb, a097e407 | lanes A/B/C + 2.8 normaliser merged and promoted (r-5380a42dab54, no binding change); left: 2.17 fixture move → T63 |
| T62 | cleanup wave 3: split long files | DONE | b619ca7b…bb31281e, 7b946345…66b81aa6, c6a967bc | app + pipeline lanes merged (byte-identical outputs; live r-bb4331929428); 3.9 dfo_salmon done (whatchanged archived, README index); 3.2 atlas/build.py: phased main(), --gnis parity IDENTICAL (Kennedy River) |
| T63 | cleanup wave 4: docs, tests, knip | DONE | 341fff64, 11d29685, 9c22614a, 9867bc86 | MAP.md ×2, 16+ duplicate tests gone, knip in check, synopsis isolation fixed, 2.17 fixtures moved |
| T64 | tiles: S2 size wins + tile rebuild | DONE | ebfbba7d, 05096efd | built and promoted with r-9b51c28ce0fe: atlas.pmtiles 548 -> 228 MB, basemap 918 -> 876 MB; map look on OWNER-TODO |
| T65 | determinism proof (N12) | OPEN | — | all (heavy) |
| T66 | curation review tool | DONE | a3f66b7c (plan) | plan approved 2026-10-23; build → T96, T97, T133–T136 |
| T67 | braid splitting as a build step | DONE | 1497beb0, ed1a5c0, cb3e0654, b91018e5, 5a6e7911 | promoted 10-24; own phase `cut_braids` 10-28 (parity + bindings identical) |
| T68 | HYDAT / feed follow-ups | DONE | e25d05fd, 17610988, 0fa29df4, 98817665 | F1/F2 follow-ups in ticket |
| T69 | search on desktop: keyboard leftovers | DONE | 448ebe1b, 389e451a | — |
| T70 | cleanup wave 1, app side + stale worktrees + `pnpm clean` | DONE | 52ec4cb5, 799614a5, 4615ee0f, 1e8cbdb5, ac95e09f, a7947f99, de3cb79d, fd3f8c06, ef11d8e4, 17e4fe8f | merger steps done 2026-10-24 (clean + strict green); Q17 dead follow-branch → T63 |
| T71 | one Map entry: drop the regs header's Map button | DONE | 4817a2e7, 40d6a47c | — |
| T72 | release LATEST pointer + prune old releases | DONE | 0eb70144 | Q7 answered: 22 old releases pruned |
| T73 | smaller reach hand-off (N14, disk) | DONE | e375cc61 | all runs gz: ~0.75 GB each vs ~20 GB; no convert needed |
| T74 | native colouring engine + native map gaps | DONE | 740f37b8 | Android: per-tile event via the binding patch, one engine, hatches/pill via Images, expo-location; emulator 13 pans / 49 frames 0 grey; iOS → T217; lake modes wait on lake facts in a release |
| T75 | basemap in the release manifest | PARTIAL | d4589cb5, 6f25c3c9 | manifest names `basemap-147bd82d051f` (274 MB) in the live LATEST r-c706f5eaa2a3 (verified 2026-10-27); left: offline native launch draws ground (phone, T77) |
| T76 | own gauge across a continuation (Vedder) | DONE | ce29990c, 2470ef08, f648ab5c, 4fba2181, e5dfe217 | live in r-36fbc5326506 |
| T77 | D1 phone testing | OPEN | — | all (owner) |
| T78 | tidal/backwater disclosure on Level | DONE | 4d35d021, c0412c4d, 8185dd28, 5aec64f3, ca52fa7c, c0ea4eb9 | live; Q47–Q49 |
| T79 | imagery corridor P0 + pools validation | POSTPONED | — | parked by owner 2026-10-28; defaults stand |
| T80 | small open items (S1 minors, Kitimat witness, D19, flash, search, CI) | PARTIAL | ca5ecb26, 309cd3e8, b08af5e9, ea63d8cb, 46f49485, 53abfd9c, 2c2d3d69, 3a6d20c3, 55b30ec7, 128cc4e0, 84d5919d, fb5d72e5 | two open shapes, item 4/6 (see ticket); Q50–Q52 |
| T81 | settle the pending small owner questions | DONE | 2fb23d92, 116b0e19, 4b197646, bd824a69 | — |
| T82 | T41 leftovers: reader gate run, caveat box O3, sheet height O5 | DONE | f7075463, d8997fbd, 663242b1, 7b9cf940 | — |
| T83 | map: boundaries under water, warm dot, sub-20 pairs | DONE | 111bd363, cbd3e139, e61a6e7d, 7bb14b79, a9dae070 | — |
| T84 | ticket-ledger guard | DONE | caf5ef52 | — |
| T85 | native install size: z13 overzoom + landuse filter (D32) | DONE | aeba83ca, b17dfa1f, d4589cb5, 6f25c3c9 | atlas z13 + basemap landuse filter + basemap z13 under the owner 2026-10-27 rule, all via T202 (basemap 918 -> 274.5 MB; audit clean on 6 extents; shots 8cc3af7e) |
| T86 | production hosting + ship config | POSTPONED | — | parked by owner 2026-10-28 (prod a while off; SEO prod too); includes the data-worker double-gzip fix before any prod release |
| T87 | owner sign-off walk, then final clean-up | PARTIAL | 1267d358, 8a043348, 7e656fcb | sign-off walk (owner); clean-up done 2026-10-28 |
| T88 | iOS build | OPEN | — | all (owner timing) |
| T89 | rules that bind no water (10 unmatched + 3 no extent) | DONE | 010b5c28, 1ea79ca0, 894854c0, d47a38cd, 5f1eec34 | 6 rows / 21 rules await a located water: proposals in curation-review/t89-awaiting-water.json (Q45) |
| T90 | Chehalis closure reaches Harrison side channels | DONE | ca9aa451, 9eb7d92f, 9d305ebf, 8fa32076, c8c72d6f | live; T102 part B |
| T91 | Atnarko campsite split location | DONE | c357fcdb 94599d8b | closed under the lake-boundary rule: campsite end = owner pin; Tenas Lake outlet @630.47 (edge facing the extent); 96/96 ambiguous_cut picks nearest the other end, nothing moved |
| T92 | gear omission check | DONE | de0581e1, b931f93d, 750ba2ad, 9c78ef20 | — |
| T93 | gauge ↔ section relationship table (policy) | DONE | 0e31959c, b4ca30d9 | `rel` + distance bucket on section_gauge (conditions 870,919 B ≤ 950,000), `gauge_relation` candidates (bundle), POLICIES with default `drainage` = today's gate (107,799 rows identical); app says "about 11 km downstream"; Vedder finding → Q31 |
| T94 | gauge promote: `--only`, demotion guard | DONE | 969ec8fe, 28f03711, 01bf3873, 6753fd6f | first real match+promote dry-run is the owner's |
| T95 | salmon (DFO) regulations: the plan | DROPPED | — | plan exists (pipeline/regs/dfo_salmon/PLAN.md); work → T164 |
| T96 | curation review phase 0: tool safe + the 5 gate-failing entries (ingest R3/prov broken) | DONE | 3e841e31, 3760fd5f, f0228e09, 15723b83 | — |
| T97 | curation review phase 1 (marks that last) + umbrella | PARTIAL | 303f6d9d, 15c04705, 5717c9fb | phase 1 done; phases 2–5 → T133–T136 |
| T98 | tidal water spec: Fraser below Mission (RULINGS C-2, K7/K8) | DROPPED | — | superseded: folded into T212/T166 (owner 2026-10-28): the tidal piece is removed from the freshwater map |
| T99 | pin the rulings nothing enforces (Z1 base+deltas, 12 ◐ rulings, C-4 citation) | DONE | f8742374, 53d57288, f3bc2b4b, 857bed9a | — |
| T100 | lakes divided into closure areas: whole lake + area caveat citing the book's map (Q43 default) | DONE | f72b57d0, 8710bdd2, 1cccc29e, da79ce4d | deliver + fixture re-cut |
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
| T112 | one entry walker over catalogue extents | DONE | 9cde1fbe, f99cd705, 05a8b704 | no rebuild (review app restart) |
| T113 | one name caser for tiles and bundle (O'rourke) | PARTIAL | 699c1392, 892db3d2 | shared caser confirmed; "2ND"/"JAJI7EM" fixed; tiles build + deliver still owed |
| T114 | one search key for SQL filter and ranking (barriere) | DONE | ebc435e5, 6a9384d3, 21021abb, e7c4d3ca, 4ae97a60, 48f712e9, 2683568e | delivered r-e88ea8112b1a (core 1.09 MB) |
| T115 | one home-filtered region membership (12 Fraser pairs) | DONE | e0461d31, 2699715d, 330c029e, b8c00620 | 12 Fraser + 3 steelhead-twin + 1 licensing pair removed; leftovers → T140; delivered r-941dcc149b17 |
| T116 | one water noun: wetlands called "river" | DONE | 4cf77c7a, 86e6ad40, 0c533d4a, 91b2952e | — |
| T117 | may_enter everywhere: other walks + outlet/WSC/edge-type owners | DONE | c7aa109f, 3b07fefc, 7552cab6, 5a07b674, 10cc45c6, 3aa88557 | 1 licensing section; gauge shed change ships at deliver (Q103); delivered r-941dcc149b17 |
| T118 | one main line for spans and continuation | DONE | 0ac0fc2b, 348f9519, 0f918ea0, 749482e9 | no shipped row changes (13 waters' exit evidence moves, no water_exit row); delivered r-012fb43aa175 |
| T119 | one acceptance gate for ingest, validator and review app | DONE | 9cde1fbe, f99cd705, 05a8b704 | no rebuild |
| T120 | one leading origin: weekday rows take the most permissive | DONE | f757d3e0, 74a712ce, e49f688e, 97b93246 | Q66 (app mainRes) |
| T121 | water kind in reach and gauges via kind_of | DONE | 7c11c27b, 35dd2ff1, d5899459, 82c59195 | no binding moved; +18 gauged river polygons at deliver; delivered r-012fb43aa175 |
| T122 | one station liveness (active/realtime/ECCC) | DONE | 9fe57691, 348f9519, 0f918ea0, 749482e9 | no station changes; Q113 cuts vs may_speak; delivered r-012fb43aa175 |
| T123 | one donor state for Conditions and map labels | DONE | 7355fc2e, 5887de97, b78a9bb0, 7a72d388 | app only |
| T124 | one per-section layer list in map | DONE | 4976bf2b, 5887de97, b78a9bb0, 7a72d388 | app only; native route colours wetland sloughs |
| T125 | latent duplicate checks: pins and small helpers | PARTIAL | 370a3978, 7fcdfaab, bffe4431, 81c05b55, 4c0b6272, e81ee1b7, 8d720fb9, 84525b29, 4076c58b, 1fdecc94, ddcd5aca, 1b106ff9, 0e5baf51, 720e9a6f, 388e6f08, a97c63a0, 9835fa2e, 2b86b39f | left: item 8 gauges 9 (Q118), answers 12 + reach 11 (Claude re-derives the pointers; was Q119); delivered r-d72230f10993 |
| T126 | forecast reading bugs: CLEVER times 8 h off, 5-day summary on a 10-day run, dots ignore horizon | DONE | e3688e2b, c7ccec1f, d7b9ed24 | live after a feed republish (owner R2 upload) |
| T127 | displaced rule still shows "counts toward" and bounds the card gate | DONE | 2a0458f2, 358b5076, 77566458 | live after the next deliver |
| T128 | sub-cap line names its total ("of your 5 trout and char a day in streams, up to 2 may be…") | DONE | 59aba774, c7d9f57f, 77566458, c410ca42, e839d0cd, 6d768bd6, bb46f5f1, 001e8ffd | — |
| T129 | web request budget: gate first-paint / per-tap bytes, not only the release total | DONE | ef40ed10, 36e4b3ae, 511aadb1, 3ff84f30 | bytes table + WEB_BUDGET gates (release step + app test) + pinned request walk; splits → T182 |
| T130 | Conditions: zoom out further before the basin heat map; Change chip greyed under Level | DONE | b4f3a40f, cd95bb63, b5218057, c9d48ea7 | Q-T130a/b (defaults built) |
| T131 | spot pins look better and open the spot on tap | DONE | 00398a3b, fafcd21a, 64b96b55 | Q-T131-1..5 (defaults built); native tap with T50 |
| T132 | curation review: salmon (DFO) pass | OPEN | — | later, after the first pass |
| T133 | curation review phase 2: one vintage on screen | DONE | 94e06e52, 5052824f | digests live from r-b518a2b507fa (reach rerun, no binding changed) |
| T134 | curation review phase 3: review screen separate from fixing | DONE | 18ad4bf8, 35f6e2db, b66105e6, 32f5654f, 5a9a2243 | leftovers → T142; row_boxes sidecar run by Claude |
| T135 | curation review phase 4: queue, pre-checks, ticket export | DONE | 8d0f5d76, 19dbc1ee, 9b8de2e3, 0a2be2da | no rebuild |
| T136 | curation review phase 5: parity gate + pilot | DONE | b42fc8cd, 12739188, 9b080497, 371f1a85, 0634891b | parity 3,405 rules, 0 unexplained; scratch E2E false-stale 0; pilot (Q114) DROPPED by the owner 2026-10-25 — the real review replaces it |
| T137 | reader shooter: TALL cards shot drops the Other fish list (re-shoot + re-run the gate) | DONE | be8b4693, 111dcea3 | — |
| T138 | reader shooter: chilliwack-p2-row-rb frame misses the rainbow row | DONE | d8997fbd | — |
| T139 | third headless one-kind pool: Liard River watershed (7-53) | DONE | 8fb5f1ac, 85288617, 67d72c8e, c4a3da48 | delivered r-b518a2b507fa |
| T140 | bundle regions of a water + area families ask the one region membership (G3) | DONE | 41f541a9, 35dd2ff1, 82c59195 | latent: no bundle row changes; delivered r-012fb43aa175 |
| T141 | carve-out walk seeds from the extent's cuts; braid cut reports side-channel crossings | DONE | 8c05e725, 35dd2ff1, d5899459, 82c59195 | no binding moved; delivered r-012fb43aa175 |
| T142 | review screen leftovers: focus after click, error wording, partial mark (Q96), draft-guard test | DONE | 9c141d24, 8d0f5d76, 9b8de2e3, 19dbc1ee | Q96 not built (default no) |
| T143 | Conditions horizons say "no forecast" (Level: publisher stopped at the first model) | DONE | ce46639f, 90edf4ae, fd584a83, 92d47b1c, 3d17b1df, 8d3de7e0 | feed republish (local done; R2 owner) |
| T144 | Change: CLEVER + COFFEE combined, ELF only last resort with a low-water caveat | DONE | 7b4d941f, b4fd96a9, a522525f, 89ee4423, a156e1c2 | feed republish (local done; R2 owner) |
| T145 | spot markers: small capped squares, deselect on tap off | DONE | fcbd8d0f, de300379, 43300623 | app; native guard needs the device run (T50) |
| T146 | no-access colour apart from base (light spruce, dark ice cyan) | DONE | 01f76c76, 41ace93b, 38e6628b, df1c8bf1 | app |
| T147 | "no public access" leads the regulations page | DONE | 01f76c76, e5903a77, 38e6628b, d5d032d8, 1e663952 | app |
| T148 | sloughs/canals second sweep by FWA feature codes + inside line codes, calibrated on the owner's marks | DONE | 5b8066d7 | (data: handoff/PLAN/sloughs/v2); 2,424 candidates; 275 not-fishing waters drawn as lakes |
| T149 | review page: slough complexes marked in bulk; intermittent-lake spot check | DONE | e5ba7972 | page retired 10-28 → review app Sloughs tab (T55) |
| T150 | drop the regs header's "Parts" button; the part list lives in the Map view | DONE | c3eafff5, 56c68c19, 4f978f5f, 08b842d0, 410dbc63 | app; delivered r-9cf1fef180a8 |
| T151 | water Map view: one line width, selected part by dimming; labels in the loaded face | DONE | c3eafff5, 56c68c19, 4f978f5f, 08b842d0, 410dbc63 | app; delivered r-9cf1fef180a8 |
| T152 | sewage basins, tailing ponds, dumps (275) drawn as fishable lakes | DONE | ef91524b, c321204d | live since the 10-27 atlas/tiles build (r-c706f5eaa2a3): 245 bindings removed, 275 muted fills |
| T153 | a river's rules on unnamed lake polygons on its course (Q124 default, one flag) | DONE | 52462f99, 62e9cb7c | reach rerun: 569 polygons on 154 rivers, 284 rules gain, 0 lose; delivered r-9cf1fef180a8 |
| T154 | a dams layer (BC WRIS, OGL-BC) matched to FWA; T148 R2 reads it | DONE | 23be8123, d8fab861, 216753d7 | fetch layer `dams`; evidence only (Q-T154a); delivered r-9cf1fef180a8 |
| T155 | Still Creek and Nelson Creek: book entries with no water (named ditch/canal lines) | DONE | 83463771 | measured: all three entries bind (99, 6, 9 sections); no change |
| T156 | test hygiene: hydat sandbox stub release; temp gate fixtures outside the app tree | DONE | 9c6cbd46, 7714418a, 23cee506 | tests; delivered r-9cf1fef180a8 |
| T157 | Conditions follow-ups: switcher states, no record to compare, ELF caveat on map, handover drawn | DONE | 9c6cbd46, 4b82d910, 7714418a, 2c845cf2 | app; delivered r-9cf1fef180a8 |
| T158 | depth contours vs no-access fills: contours held off lake fills, deep navy | DONE | 9c6cbd46, 7714418a, 3d5f19de | app style; delivered r-9cf1fef180a8 |
| T159 | gauge auto-promote checks every name the matcher accepted (Arrow Reservoir / Lower Arrow Lake) | DONE | ce1cf83c, 06b83665 | owner promoted 7 (10-28); 08MH079 proposed as Kilgard Creek (FWA-unnamed blk 356314888, a review.json bind) — a note, not promoted |
| T160 | 08HB074's standing Comox Lake backwater gets its own Level sentence | DONE | 52462f99, fd99643c | deliver; delivered r-9cf1fef180a8 |
| T161 | the pipeline ships each fish's leading origin (rows 33); mainRes only reads it | DONE | 23be8123, d0bc72fd, 3bf1e1e1, 3e89b917 | deliver + fixture re-cut (rows 33); delivered r-9cf1fef180a8 |
| T162 | salmon/steelhead run timing: PSF + DFO parsed to `timing.json`; integration plan (handoff/PLAN/runtiming/PLAN.md) | PARTIAL | 24879b00 | app integration → T199 (owner said go 2026-10-25; size gate gone with T182); Q144 open |
| T163 | McKinley Creek r1x (reported catch-and-release on the upstream pond) | DROPPED | — | not a defect: r1x is the spring-closure lift (RULINGS L13), whole by design; r1 (the C&R) already stops at the lake |
| T164 | salmon (DFO): finish the dossier, place the 96+7 splits, review the extents, fetch + place current quotas | OPEN | — | umbrella over T95/T132; DFO review mode built in the local app (queue, map, anchors, new-split proposals in the mark); ★ owner reviews the 346 extents + confirms splits |
| T165 | no public access: streams dotted grey (owner), warning amber only in the page banner; own keeps amber | DONE | f438d417, bfd82ad6 | app style |
| T166 | tidal areas cut: DFO tidal polygon → one clean cut across each stream (fix partial-channel blobs) | PARTIAL | — | Phase 1 built: tidal marks → tidal splits → the piece below each out of every binding after the walk (tidal table, K7 note); `tidal_preview` lists rules that would lose all water; next: apply the owner's marks, atlas rebuild, binding diff, release |
| T167 | More tab: simpler details, references on their own page, data freshness compact on the main tab, a real desktop layout | DONE | 3af0dbaa, de06c3ae | app; tiles date needs a manifest stamp (follow-up) |
| T168 | "N gauges in view": split gauges on this water from donor gauges; not-reporting gauges under a closed drop-down (owner 2026-10-24) | DONE | bfee90d9, fcddee67 | app |
| T169 | one gauge panel for one side-channel water (Nicomen grey middle: connector vs adopted lines) | DONE | 89b8e1a1, d42f0741 | delivered r-46595c88e76e: Nicomen all 9 sections one panel (4740) |
| T170 | a spot gets the flow of its own day (historical) on refresh or on request | DONE | ef762537, 298cee48, a09374b0 | app; GeoMet direct from the browser (no proxy) |
| T171 | "Like your visit?": every forecast day uses the flow forecast (no weather-only gaps) | DONE | 3f43c1dd, ef762537, a09374b0 | feed republish (local; R2 owner) + app |
| T172 | Android smoke on the emulator (builds, installs, opens, main screens) — before cleanup; full parity stays T50 | DONE | 9cc8724f, 04c7da78 | 9 findings → T50 "Found by T172" (status-bar insets, pin press, stale-install error) |
| T173 | fish obstacles layer (falls, dams, culverts) from the BC Data Catalogue | DONE | c27db165, 88b60317, f4923e1f, PSCIS: see ticket | FISS + WRIS + PSCIS (Q165: OGL-BC, owner), 27,267 marks, atlas layer `obstacle` z10-13 +5.7 MB; reaches readers with the next tile build (commands in the ticket); shots T173-shots/ |
| T174 | remaining answered → grey → answered gaps on main rivers (321/84 runs; Gravel Slough connectors) | DONE | 00153bf6 | r-bda3fea28838: +407 Flow / +327 Level, 0 changed/lost; Q154 measured, off |
| T175 | a water's own line shows at every zoom on its Conditions / water map (copy from tiles, clamp the opening zoom) | DONE | e8999343, 1f46e968 | app; native not run (T172/T50) |
| T176 | the Conditions legend's "N gauges in view" counts sections, not gauges | DONE | e8999343, 1f46e968 | app: "N reaches coloured" |
| T177 | combine CLEVER + COFFEE on every forecast day, weighted by measured skill (owner Q160) | DONE | b039349e, 73dd133e, af119f2c | feed republish; equal weights until the archive decides (T178); + spots record the trip day |
| T178 | re-score CLEVER vs COFFEE from the run archive (~6 weeks) and set the weights by the verdict | OPEN | — | due ~2026-12-05 |
| T179 | one cut-placement pass: auto cuts merge into names, real cuts close together stay apart (order-independent) | DONE | deef0cec, 53f3819e, b93c95a6, 5c1ddc90, 6d7fd98e, cb858630 | promoted r-9b51c28ce0fe; Koocanusa cut verified at the inlet; authored lake-anchor ends named again (r-46a602244714) |
| T180 | move the remaining ~105 component strings into core words (T60's KNOWN_COPY list empties) | DONE | 71179834, b54c69c1, bc7bc016, 462f8488 | KNOWN_COPY 95 → 0, no wording change; map gaugeDots attribution noted |
| T181 | gauge pill keeps the theme the map was built with (check, then fix) | DONE | 7c43151a | confirmed (pill, pin, hatches kept the build theme); fixed + render test |
| T182 | release file splits for the web (section_down, part_geo, nearest town in core, conditions) | DONE | afbd6d79, 715ecc46, 00ad9565, be778427 | per-file gates + cold_load (2.98 of 3.35 MB); total → 20 MB offline backstop (14.90); salmon on the first Salmon press (web), method after paint; feed headers → T86 |
| T183 | straddling pieces carry BOTH regions' rules (reverses T115, owner 2026-10-24) | DONE | 64f92990..1ba63301, 1ca8bfda | promoted r-98c43091c440; stricter wins, 5 m minimum, Skeena 2 from Exchamsiks; card gate 0 |
| T184 | keep the local curation review app and clean it up: entries only (splits by proxy on the map), human-reviewed state, artifact path removed | DONE | 2bacfc7d..dabd3b8b | — (pilot dropped by the owner 2026-10-25; the app serves the real review) |
| T185 | marks → proposals (one per mark, one per new split, each split with its confidence) → read-only "waiting to apply" list (the marks ARE the decisions, 2026-10-27) → Claude's `apply_proposals`: binding check, side rebuild where a cut changes, one commit each; fix mode lost its writers | DONE | 8d9383df, c76a9fd8 (writers removed), e32eb08e, 7b63f660 | Claude runs side chains + applies; owner changes a mark to change a proposal |
| T186 | web prefetch after first paint (region's regs file; core + wasm on idle) + core-words chevrons (Q164, Q162) | DONE | d54842ec, b8b5b357, 0a129a14 | first tap still fetches water + access → T182 |
| T187 | a braid piece between one window's two cuts is inside it (Peace Site C closure, 359005110:3454) | DONE | 13027245, 525ab99e, 0bc22c70 | found by the post-braid check; 1 rule changes |
| T188 | get found in search: per-water static pages, sitemap, structured data, Search Console, outreach plan (no spam bots) | PARTIAL | plan | plan done in handoff/PLAN/seo/PLAN.md; build → T192–T198 (domain canifishthis.ca; full rules per water page) |
| T192 | SEO: stable slug map + per-water page model from the bundle (presenter code shared) | PARTIAL | 1c4dd582, 435ce2d8 | committed; first real build + v1 redirects: see ticket Outcome; seo/PLAN.md §1; staging live 10-28 (seo/staging.md) |
| T193 | SEO: static water pages + About, built in the release job | PARTIAL | 1c4dd582, 435ce2d8 | committed; first real build + v1 redirects: see ticket Outcome; after T192; staging live 10-28 (seo/staging.md) |
| T194 | SEO: sitemap, robots, canonicals, JSON-LD + build gates | PARTIAL | 1c4dd582, 435ce2d8 | committed; first real build + v1 redirects: see ticket Outcome; after T193; staging live 10-28 (seo/staging.md) |
| T195 | SEO: region hubs, weekend/changes pages, share images | PARTIAL | 1c4dd582, 435ce2d8 | committed; first real build + v1 redirects: see ticket Outcome; after T193; staging live 10-28 (seo/staging.md) |
| T196 | SEO: page performance budget + Lighthouse CI gate | PARTIAL | 1c4dd582, 435ce2d8 | committed; first real build + v1 redirects: see ticket Outcome; after T193; staging live 10-28 (seo/staging.md) |
| T197 | SEO: outreach kit (drafts only, no sending; no bots or link schemes) | PARTIAL | 1c4dd582, 435ce2d8 | committed; first real build + v1 redirects: see ticket Outcome; owner sends; staging live 10-28 (seo/staging.md) |
| T198 | SEO: .com to .ca 301, HSTS, Search Console/Bing wiring, measurement | PARTIAL | 1c4dd582, 435ce2d8 | committed; first real build + v1 redirects: see ticket Outcome; owner: DNS/accounts; with T86; staging live 10-28 (seo/staging.md) |
| T189 | curation review from the pilot: source pinned, each rule's extent in words (tributaries, carve-outs), entry extent on top, tributary + lake-split calls in the mark, one pre-check per fact | DONE | 106dbc83 | — (pilot dropped 2026-10-25; the screen serves the real review) |
| T190 | a two-region piece's decision ladder heads both regions' rules with one region | DONE | 5a6cb255 | app only (decided.ts) |
| T191 | curation review: one-row top bar (secondary filters fold), "fix extent" per rule + entry (op, 1-2 points: map split / any split / NEW confluence, lake inlet/outlet, map click; items; tributaries; read-only preview) as Mark.extent_fix, in tickets for T185 | DONE | 031eeb0c | — (pilot dropped 2026-10-25; the owner uses it in the real review) |
| T199 | run timing in the app: implement and refine (T162 data + plan) | OPEN | — | last band, with T200/T201; go given (owner 2026-10-25); Q144 first |
| T200 | stocked lakes: feed the prepared Stocked mode | OPEN | — | last band; source + licence (owner) |
| T201 | bathymetry: feed the prepared Depth mode and draw contours | OPEN | — | last band; source + licence (owner); tile budget |
| T202 | tile size optimizations (basemap z13 options, attributes, precision, simplification) | DONE | d4589cb5 (basemap), aeba83ca b17dfa1f (atlas) | basemap 918.4 → 274.5 MB (z13 + roads rebuild, wetland dropped, fills/coast on a coarser grid); atlas 239.3 → 176.1 MB (MVT-id only, lake attrs, 2048/1024 grids, mu z12 overzoom, watershed + sign_zone drawn, wma Creston only), release r-c706f5eaa2a3 |
| T203 | app UI: jumpy "Flows into" chip over navigation, search always lands on the map, amber highlight | DONE | 969f8ff3 | items 1-3 built; 4-5 (Kanaka end labels, non-adjacent stretches) need release data: one mark per meeting point + part stretches - see ticket Outcome |
| T204 | a part's separate stretches and a mark at every place two parts meet (T203 items 4–5) | DONE | 9e0960ec | built in r-8b43fb267199 (Kanaka: bridge + falls marks, 610 stretch rows); owner phone check |
| T205 | curation review: zone entries without a map, species + sizes per rule, pilot removed, compact T/R/P/A widget, bigger show-on-map | DONE | 7a55e651 | owner tries it |
| T206 | app golden parity after the retired overlapping rules AND the zone daily-quota rename (page v36 oracle) | DONE | 3797095c 66a0aa70 | order-only amendment (rename flips), ORACLE_RETIRED + Lois fold, on-parts, handle re-pins; check green bar release-budget (T203) |
| T208 | show rules that hold on an undrawn part of a lake ("area at the outlet … map 1") clearly, linked to the synopsis map | PARTIAL | 3fc83a0f | app shows area rules as "Outlet area only (see map N)" with the synopsis page linked, named on the fold head and open on a day the rule holds; the drawn outline waits on lake splits |
| T207 | "on parts" is a note; unused-cut false positives; split confidence end to end; White/Fulton/Chuckwalla/Cranberry cuts | DONE | 04315ec5 0e605022 48239620 c127eff6 | built in r-d32f814feac0; 8 DFO scopes bound; Burton/Kootenay decided by the owner 2026-10-26 (445643c2) and bound; 18 left as high severity |
| T209 | vessel restrictions in the app: boats line (electric only, no powered boats, power cap, speed, towing, no vessels) on the water page + map sheet | DONE | ffe7e7e2 | boats line on the page (6 kinds, golden test); 162 electric_only rules store max_power_kw 7.5 (owner 10-28, pipeline/tools/electric_kw.py) |
| T210 | DFO review mode: proposed splits shared along the river, Place y without a new split, simpler new-split UI | DONE | 0805e479 | proposals deduped across all marks (2 distinct; 1 duplicate pair folded), server reuses ids, Place y with existing-cut fix, 3-step new-split UI; owner tries it |
| T211 | DFO salmon regs in the app (options), the DFO feed plan, historic extent matching, new-location alert | DONE | 491c9089, 4b18a372 | card + toggle; live feed `regs.dfo_salmon.live` (offline = last capture), new-location alert → DFO mode via `live queue`; part 3 moved to the DFO-locations lane; owner: feed cron + R2 sync; 10-28: region header notes (feed + release, card "Region rules (DFO)", scoped waters via curated bindings), gear marks on the Salmon map + card, `salmon` split per bundle region (b16b7a88, 9aa0ad11) |
| T212 | tidal boundaries: a tidal split on every ocean-entering stream (research + proposals, no data change yet) | PARTIAL | — | 1,163 candidates (916/966 named ocean streams met by the polygon) in handoff/PLAN/curation-review/tidal/; curation review Tidal tab, accept = a mark; owner reviews, then T185/T166 build the cuts and remove the tidal piece (T98 folded in, 10-28); trib check: tidal/tributary-impact.md; pass 2 (DFO area pages, 10-28): tidal/pass2/ — 15 agree, 1 moves (Tlell, conflicts with the mark), 4 new; DFO confirms mouth: 39 upgraded, 23 conflicts, 1 new (bulk accept in the Tidal tab) |
| T213 | paint bait-ban and fly-fishing-only waters distinctly from general restrictions | DONE | 74caddda, 898fd9e6, 0c203100 | release file `method` (308 kB gz, strip projection); 10-28 rework: marks on Standard (ink edge under the status line: solid fly only, dashed bait ban; lake ring), Bait & fly mode removed; Standard / Salmon under Layers; day strip in the corner; shots T213-shots/gear-*, variants/ |
| T214 | app polish: quota sub-cap wording names its total, desktop no-swipe + Esc back, clearer part highlight, steady up/downstream buttons | DONE | 555e7ee9 33775c57 | items 1, 2, 4 (subcap lead + golden amendment, desktop no-swipe + Esc Back, reserved exit slot); item 3 part halo at full outline, others dim 0.25 (web light + dark) |
| T215 | open the PDF on the page with the entry's section highlighted | DONE | 3e5ef46f 15d19068 7c513588 | full PDF + highlight annotation, web verified (Cowichan p.15); phone unverified; owner uploads the release to R2 |
| T216 | better "How this was decided": interpretation, what overrode what, sources | DONE | 4e39f3fb | story.ts: layers base → water → exceptions, why + printed page per step; tests on Fraser two-region, R5 spring-closure lift, sub-cap, closed water |
| T217 | iOS: the per-tile event and stretchable Images in the binding patch (T74 is Android only) | DONE | 41f15021 | simulator: 26 pans / 49 frames 0 grey; pill + hatches + At my location seen; first-view replay + pending states (iOS-only); pill content = stretch box on iOS |
| T218 | bathymetry: ship depth figures + contours for the 28 already-digitised lakes (OGL-BC layer) | OPEN | — | not yet (owner 2026-10-27); after T201/T158 |
| T219 | bathymetry: trace contours from the open scanned charts for the top-fished lakes | OPEN | — | not yet (owner 2026-10-27); raster scans, georef works, depth labelling needs a review UI; ★ tier size |
| T220 | braid channels close: no channel line ends where the water goes on (prune closure + one zoom per side-channel run) | DONE | 4c90fe68, 64c28a2b | owner 10-28 FIX complete; 1,041 -> 97 dangling ends, no existing binding moved; live r-46be3c135e68 |
| T221 | solo waters open on regs (no Map), URL follows the water, one salmon table, plain size rule, count line before its size box, at-a-glance species table (kept, no counts) | DONE | 73625e35, 9784a7b0 | owner 2026-10-28 web feedback (Dala River) |
| T222 | Stave spawning channels (Ruskin/Northrop): add the FWA side channels so Stave r1/r2 can bind | OPEN | — | added_streams + atlas rebuild; no owner call |
| T223 | user accounts shared between the web app and the phone app (sign-in, what syncs, offline) | OPEN | — | far future; default plan: Firebase Auth + Worker + R2 users/<uid>/, no DB |
| T224 | name variants from local sources (municipal, ECCC, DFO, synopsis, OSM, BC GNIS), each with its real source | IN PROGRESS | 009aeec0 | owner answers 10-28 applied: sources end to end, 183 accepted (mined_names.json), Names tab; needs the next atlas build |
| T225 | feeds: where they live, how they're triggered, alerting (gauges, DFO, HYDAT) | OPEN | — | design first; host = Cloudflare Workers cron (owner 10-28), alerts in Cloudflare |
| T226 | bait-ban / fly-only marks fade in with zoom (gentle default, `late` variant) | DONE | 6db63d0e | paint.ts gearPaint ramps; shots T226-shots/ |
| T227 | closed-to-game-fish waters open for salmon: the salmon year bar at the top, region closure, species notes, plain closed row | DONE | 4f2e22a3, 6af1ffa5, f35fdc75, 3dd41b02 | owner 2026-10-28 (Cowichan); shots T227-shots/; `salmonTimelineKey` for the merge lane |
| T228 | no-access land and salmon break a water's pieces (live salmon merge on the map); dormant gauges attach to nothing; trout includes char (C4) | DONE | a757756b | live r-c331a7a3c2cf: 34 waters gain a part (Coquitlam 4), no rule set moved; 1,618 dormant matched stations out of Conditions; owner re-cuts sync.mjs + regs-fixture |
| T229 | DFO salmon cascade on the Skeena: plain explanation + Cascade map (review app, DFO mode); the order read from the Region 6 page | DONE | 808784f1, e20796648, 29326f97f | release r-6e96911055ce: more specific place wins within a tier (Kitsumkalum mouth closed Jul 1–Aug 31, Morice Forks), tidal-Area rows section E only, eulachon its own line; Q3/Q6 to T164 |
| T230 | provincial in-season changes: curated matches, banners on matched waters, unmatched notice + badge, review In-season tab | IN PROGRESS | — | owner 2026-10-28; feed = T225 `feeds/inseason/index.json` |

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
3. **Waiting on the owner:** T101 (Q44), T55 (candidate marks), T67 (braid Q1/Q2/Q4), T97 (curation plan), T93.
4. **Cleanup, once features settle:** T60 (consistency auditors) → T70 leftover → T61 → T62 → T63; T72 and T73 in
   this band (T73 alone, not concurrent with T61's normaliser).
   Unification tickets (latent or plausible): T117 (after T102 part B), T118 (with T108), T112 → T119, T121, T122,
   T123, T124, T125.
5. **Tiles window (one tile build):** T64 + T85, then T75; T129 (web request budget) alongside.
6. **Native and shipping:** T50 (alone, heavy) → T74 → T77 ★ → T86 ★ → T88 ★. T65 (determinism, heavy) in a quiet gap.
7. **Last:** T95 ★, T87 ★ (sign-off walk, then delete app/design). T98 is folded into T212/T166.
8. **Last band (owner 2026-10-27: start with T202 tile sizes + the parked basemap z13):** T199 run timing, T200 stocked lakes, T201 bathymetry / depth maps (then T218 digitised lakes, T219 scan tracing), T202 tile size optimizations.

## Owner questions

Every open owner call, with the default we build with until answered (owner call 11: Claude decides the smaller ones
and records them; T81 records them in decisions.md). Answer here or in decisions.md; answered rows move to Answered.

| # | ticket | question | default (built with) | kind |
|---|---|---|---|---|
| Q2 | T54 | Conditions map: keep the flow colour in no-access areas, or mark them there too? | keep flow colour; Tier A gets only the banner on Conditions | owner |
| Q4 | T55 | Which finder candidates are sloughs/canals to reclassify? 7 grey polygons (Nation, West Road, Honna)? | nothing reclassified without owner review (dry-run + confirm); the 7 stay grey | owner |
| Q9 | T77 | When to do phone testing D1a / D1b | D1a when the phone is at hand; D1b after the R2 bucket | owner |
| Q10 | T79 | Imagery go/no-go; SPEC §12 (10 m pack, buffer, sources, licence, server, caps) | no offline imagery before the first native release; skip 10 m; 250 m buffer; online-only on web; pools a study | owner |
| Q26 | T86 | R2 bucket, contact address (domain decided: canifishthis.ca, .com redirects; name "Can I Fish This?") (no ArcGIS key: owner 2026-10-21) | keep the name; staging bucket first; publisher every 30 min | owner (credentials) |
| Q27 | T87 | Sign-off: the app looks right; delete app/design | hold until the owner walks it | owner |
| Q28 | T88 | When to build iOS | after the Android ship | owner |
| Q31 | T93 | Gauge representativeness policy — now a one-line choice (`shed.GAUGE_POLICY`: drainage / same_watercourse / ungated, measured in T93); also: admit `up_branch` across a `water_continue` link so the Vedder hears 08MH001? | the current drainage gate stays default | owner |
| Q42 | T98 | Lift the tidal-spec hold? | ANSWERED 2026-10-28: tidal pieces are removed from the freshwater map with the T212/T166 cuts (decisions.md) | owner |
| Q47 | T78 | Tidal sentence: closed "Notes and sources" fold, or open under the Level headline? | in the fold | owner |
| Q79 | T116 | Regs page says "river" for a stream ("this river's rule", "Whole river") while search rows say "Stream" — switch the page to "stream"? | keep "river" on the page | owner |
| Q91 | T105 | The 54,386 matched-only sections now say "No gauge on this water is close enough in size" — wording ok? | as built | owner |
| Q103 | T117 | The gauge shed climbs the way the walk does: the Theodosia (08GC005) and Quinsam (08HD034) diversion gauges no longer speak for water above their canal (the river's own gauge takes over on 174 + 107 sections); 24 sections lose every gauge; a gauge ON a canal reads only its own piece? | as built | owner |
| Q113 | T122 | Gauge cuts follow the roster's realtime snapshot, sheds follow ECCC Active: 38 active-but-silent stations have a shed and no cut. Make cuts follow Active (atlas rebuild, moves section ids)? | unchanged | owner |
| Q123 | T151 | How the water Map view marks the selected part without bold lines? | dim the other parts; one line width | owner |
| Q124 | T153 | Does a river row's rule apply to an UNNAMED lake polygon its main flow runs through (book "river" includes its widenings unless it names the lake)? | yes for unnamed polygons covered on both sides; a named lake never takes the river's row | owner |
| Q126 | T143 | Were you on Level when +1/+3/+5/+10 said "no forecast"? (Flow answered at every horizon on today's feed) | yes, assumed | owner |
| Q130 | T144 | Under a combined Change, also draw each model's own line (quieter)? | yes | owner |
| Q132 | T144 | The +1/+3/+5/+10 chips keep the old model order (CLEVER then ELF) — or follow the Change source too? | keep | owner |
| Q139 | T147 | The no-access block also leads Conditions for a tapped no-access section at full size? | keep | owner |
| Q144 | T162 | Run timing layer: default species (steelhead?), show passage reaches (dashed) as well as spawning, and its own map layer vs inside Conditions? | steelhead; both, passage dashed; its own layer | owner |
| Q146 | T150 | Opening the parts list shortens the map but does not re-centre it (the Fraser's long lower part gets cut off on a phone) — re-centre on open? | no (keeps the reader's pan) | owner |
| Q147 | T153 | A pond a steelhead row now takes also gets the zone's lake wording of the steelhead rule (zp:steelhead r1b/r2b/r4b +68 polygons each, z5 r6b +44)? | accept | owner |
| Q148 | T154 | Promote "dammed widening = lake" (R2 with the dams layer: 26 candidates → LAKE; catches Quinsam and South Nanaimo, not Bonaparte) into the slough candidates, or keep dams as evidence only? | evidence only | owner |
| Q149 | T154 | Bonaparte (329055141) has no dam in the provincial data — mark it lake by hand on the slough page? | yours on the page | owner |
| Q151 | T166 | Unnamed streams: assume their tidal limit is at the mouth (no crossing work)? | yes | owner |
| Q152 | T153 | A pond a steelhead river takes as its widening is ANADROMOUS (a rainbow over 50 cm in it is a steelhead, p.80) — 68 ponds; found by the card gate on the Kitimat's pond? | yes (built; one flag) | owner |
| Q153 | T174 | A river's own side piece is adopted onto the LARGEST same-code section — use the nearest ANSWERED one instead (84 Flow gaps)? | measured: largest ANSWERED in reach, +148 side pieces, 0 changed — SHIPPED (`ADOPT_ANSWERED`) | owner (revert = one switch) |
| Q154 | T174 | Walk a river's own unowned double-line polygon as the river (as T101 does for stream items' polygons)? | measured: they are small FWA LAKES, not river polygons; ON moves 96,590 sections (+95k Flow) — kept OFF (`RIVER_WIDENINGS`). 2026-10-28 owner "lakes stay lakes unless a pool labelled a lake": all 136,080 unnamed, 75% < 1 ha (mostly beaver/muskeg ponds); proposed cut < 1 ha AND avg width ≤ 2× the river's → 10,650 polygons, 11,423 sections — curation-review/q154-examples.md; owner 10-28 wants to SEE them: review app Sloughs tab → "Small ponds (Q154)" (20 md examples + 60 stratified sample, satellite + outline + river, pool / real lake / unsure marks of scope q154, tally vs the cut) | owner |
| Q155 | T174 | Gravel Slough's 1425 connector lines take the answer of the polygons around them? | yes — built as `water_line_gaps` (any stream water; Gravel 2 of 3, the 3rd has two answers) | owner |
| Q158 | T168 | A renamed continuation's gauge counts as "on this water" (the Vedder lists the Chilliwack's 08MH001 as its own)? | yes | owner |
| Q159 | T168 | "Reporting, not ranked" gauges fold with the silent ones ("not reporting or not ranked")? | yes | owner |

### Answered (recently, for the record)
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
