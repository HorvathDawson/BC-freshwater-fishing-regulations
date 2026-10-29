# T93 — The data records how a gauge relates to each section it speaks for

Status: DONE (0e31959c, b4ca30d9, Q31 default: see below) — relationship + pluggable policy built; owner 2026-10-28 (Q31)
made the default "drainage, plus tributary gauges across a continuation link". Reaches readers with the next deliver (bundle + release).

Source: memory `gauge-representativeness-is-policy`; owner 2026-09-02: the gauge rule should be a tunable policy.
`section_gauge` is still `(sid PK, station, trust, mag)` — no up/downstream, channel distance or same-watercourse;
`pipeline/hydro/shed.py`'s drainage gate is the only rule.

## Work
A table of each section's relationship to each candidate gauge; today's gate stays the default behind a pluggable
policy; prove two policies on the Vedder/Chilliwack and Slesse Creek cases.

## Acceptance
Schema + tests; today's answers byte-identical under the default policy.

Depends: pairs with T76; needs a deliver. Size: L. Owner: yes. Default: keep the current gate as the default.

## As built (2026-10-28)

**Pipeline** (`pipeline/gauges/consume/shed.py`). Each (section, gauge) pair the shed walk reaches is now a
candidate. A candidate records `rel`, which says where the gauge stands as seen from the section:
- `on`: on the section itself.
- `down` / `up`: downstream or upstream on the same watercourse (same FWA code).
- `down_trunk`: downstream on the river this water flows into.
- `up_branch`: on a tributary of this water. This is the Slesse case, refused by the gate.
- `apart` / `unknown`: the codes cannot say.

It also records `dist_m`, the channel distance from the section's near end to the station (a station stands at the
downstream end of its own section). `choose()` applies a policy from `POLICIES`; the ranking is the same for every
policy. `GAUGE_POLICY = "drainage"` is today's `drains_through` gate: `relate(...) in DRAINS_THROUGH` equals
`drains_through` for every code pair (pinned in the tests). The other two policies:
- `same_watercourse`: only `on`, `up` and `down`.
- `ungated`: the behaviour before the gate.

**Bundle.** `gauge_relation` (bundle only) holds every candidate: `rel` as text, exact `dist_m`, `hops`, `ratio` and
`trust`. Changing the policy is a bundle rebuild with no re-match and no atlas rebuild. `section_gauge` gains `rel`
and `dist_b`.

**Release** (conditions, additive, still conditions/1). Shipping `rel` as text with `dist_m` to 100 m cost
+359 kB gzip and broke the conditions gate, so both are encoded:
- `rel` is an index into `GAUGE_RELS`.
- `dist_b` is a bucket: 0 means under 500 m, 1 means under 1 km, and b ≥ 2 means about 1 km × 1.5^(b-2).

`GAUGE_RELS` and `DIST_STEP` are emitted into gauge-policy.generated.ts. Measured against r-c331a7a3c2cf's
conditions file: **797,647 → 870,919 B** (gate 950,000), and conditions_open 983,326 (gate 1,150,000).

**App.** `GaugeLink.rel` / `.metres` are decoded (`relOfCode`, `metresOfBucket`) and fill the trace panel's
`metres`, which was always null before:
- The distance row says "about 11 km downstream", "under 1 km upstream", "about 3.4 km downstream, on the river this
  flows into", or "at the top of this reach".
- The sentence for a station upstream says "08MH001 is about 5.1 km upstream, on this water: water here has already
  passed it." It no longer says "drains through".
- A release cut before T93 reads null for both and gets the old words. The query is `SELECT sg.*`.
- Saved spots freeze `rel`.

## Proof: the same gauge on every section
- **On the province:** the check bundle built with this code (the coordinator's deliver) has a `section_gauge`
  identical to the live bundle's (sid, station, trust, mag): 107,799 rows, 0 differences either way. Re-choosing
  `drainage` over that bundle's `gauge_relation` (174,791 candidates) reproduces `section_gauge` exactly.
- **On the atlas:** the old `shed.py` and the new one give identical links on the same inputs.

## The two policies on the province (from `gauge_relation`)
| policy | sections answered | lost | gained | station changed |
|---|---:|---:|---:|---:|
| drainage (default) | 107,799 | — | — | — |
| same_watercourse | 4,310 | 103,489 | 0 | 12 |
| ungated | 109,508 | 0 | 1,709 | 599 |

Candidates: down_trunk 150,923, up_branch 16,190, down 4,286, up 2,691, on 423, apart 278. Under the default, 96% of
answered sections are tributaries told their trunk gauge (`down_trunk`).

**Slesse Creek, 08MH056.** It reaches 14 Chilliwack River sections as `up_branch`. `drainage` refuses all of them;
`ungated` would admit them where the ratio wins. Its own creek (on, up, down) speaks under every policy.

**The Vedder and the Chilliwack (owner finding).** FWA codes the Chilliwack as a branch of the Vedder. Seen from the
Vedder, 08MH001 (Vedder Crossing) and 08MH016 are therefore `up_branch`.
- Under `drainage`, 4 of the Vedder's 5 sections take 08MH024 (Fraser at Mission) as `down_trunk`, and one has no
  gauge. The donor panels' T76 continuation rule, not this table, is what puts 08MH001 in front on the Vedder.
- Under `ungated`, the Vedder takes 08MH001 on 4 sections and 08MH016 on 1.
- Under `same_watercourse`, no Vedder section has a gauge.

A policy of "drainage, plus `up_branch` across a `water_continue` link" would fix the Vedder without reopening Slesse.
It is a one-line `GaugePolicy` but needs the continuation set passed in, and it is the owner's call (Q31).

Tests: `pipeline/tests/test_t93_gauge_relation.py` covers the relationships and distances, the three policies on a
Chilliwack/Slesse/Tamihi/Vedder graph, gate equivalence and the encoding. Also `core/trace.test.ts` (decode and
words), `capture.test.ts`, and conformance "a gauge link says where the station stands, or nothing".

**Release lane.** The next deliver writes the new columns, and nothing extra needs to run.
`test_release.py::test_a_built_release_is_exactly_the_contract` fails on a pre-T93 bundle and passes after the
rebuild.

## Q31 answered (owner 2026-10-28): the default is `drainage_continued`

A candidate is `joined` when the section's water and the gauge's water are DIFFERENT waters of one `water_continue`
system. `water_of` is the bundle's section → water owner; `continues` is `continuation.item_pairs`, read through
`panels.systems` exactly as the donor panels read T76.

What the policy admits:
- **The drainage gate.**
- **`up_branch`, but only when joined.** It is never admitted otherwise, so Slesse Creek's `up_branch` candidates are
  not joined and stay off the Chilliwack.
- **Own gauge first.** A joined gauge never outranks an admitted gauge on the section's own watercourse
  (on/up/down). Measured: without this rule, 5 Bella Coola side reaches swapped 08FB011 (on the Bella Coola) for
  08FB006 (Atnarko, 72 km) on ratio alone.

What the app is told: `section_gauge.rel` carries such a gauge as `up` (`shed.shown_rel`), while `gauge_relation`
keeps `up_branch` and gains a `joined` column (bundle only).

**Province-wide diff** against the T93 default. These are the check bundle's 174,791 candidates, re-chosen; the
re-chosen `drainage` result equals the bundle's `section_gauge`, 107,799 rows.

| water | sections | before → after |
|---|---:|---|
| Vedder River | 4 | 08MH024 Fraser at Mission (`down_trunk`, ratio 0.007) → 08MH001 Chilliwack at Vedder Crossing (ratio 0.99–1.0, 0–8 km) |
| Vedder River | 1 | none → 08MH016 Chilliwack outlet of Chilliwack Lake (weak, 36 km) |
| Bella Coola River | 7 | none → 08FB006 Atnarko near the mouth (weak, ratio 0.001, 24–72 km) |

8 sections gain a gauge, 4 change and 0 lose: 107,799 → 107,807. Only 17 `water_continue` links exist, and just these
two systems have a gauge across the link. No Chilliwack or Slesse section changes.

**Size:** conditions is about 871,236 B gzip (gate 950,000) and conditions_open about 983,600 (gate 1,150,000).

**Tests:** `test_t93_gauge_relation.py` covers:
- the Vedder taking 08MH001 across the link, while the gate alone gives it the Fraser;
- Slesse staying off the Chilliwack;
- a water not being joined to itself;
- own gauge first;
- `shown_rel`.

### Q31 refinement (owner 2026-10-28): a continuation speaks only with a large share
A joined gauge is admitted only when the symmetric magnitude ratio is at least `panels.LARGE_SHARE` = 0.5. That is
Q44's "large share of the river's flow" floor (`GaugePolicy.joined_min_ratio`); no new number.

Re-run on the province: 107,799 → 107,799 sections answered (0 gained, 4 changed, 0 lost). The 4 changes are all on
the Vedder River: 3 sections and its lake node move from 08MH024 (Fraser at Mission) to 08MH001 (ratios 0.985–1.0,
0–8 km). The floor drops the earlier 08MH016 on a Vedder side reach (0.002) and 08FB006 on 7 Bella Coola side reaches
(0.001). Size is unchanged from T93 (conditions about 871 kB, gate 950,000). Tests:
`test_a_continuation_speaks_only_with_a_large_share`.

