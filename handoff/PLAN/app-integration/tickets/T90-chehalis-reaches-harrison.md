# T90 — The Chehalis "downstream of the bridge, May 1-31" closure still reaches 2 Harrison sections

Status: DONE (code, commits 04d8fb85 + review 32aa5a12) — awaits the batch's reach rerun + deliver; no atlas rebuild. Left: verify the diff below on the rerun (rules AND licensing_section).

Source: memory `receiving-river-not-a-tributary`. The Strahler guard in `_mouths_at` (pipeline/atlas/graph/
tributaries.py) is in, yet the 2026-10-22 bundle binds `chehalis_river.r2` via `trib` to Harrison sids 59983, 59984 —
both side-channel pieces (`section_span.off_stem=1`).

## Work
Trace why the walk reaches them; extend the guard to side channels whose water is the receiving river; a stub test on
`_mouths_at`; binding diff (subtract old-vs-old noise, atlas is not deterministic).

## Acceptance
No Harrison section carries a `chehalis_river.*` rule; other bindings move only where the diff explains why.

Depends: a reach rerun + deliver (atlas rebuild via heavy.sh if the fix is in the atlas). Size: S–M. Owner: no.

## Result (2026-10-22)

**Root cause — not the Strahler guard, and not at the mouth.** The 2 Harrison sids (59983 = `355992873:0`,
59984 = `355992877:0`) are Harrison SIDE CHANNELS, order 2 (the Chehalis is 5), so `_breaks_strahler` passes them.
`_mouths_at_lower_bound` with a window called `_mouths_at` for EVERY reach piece on the window's line with the
window's lower measure (0, the mouth), and `_mouths_at` compared that measure with edges on whatever line the piece
flowed into. Chehalis `356364028:3592` (not the mouth piece) has a distributary into `355992873:0` (at 865 m on that
channel); `355992877:0` joins `355992873:0` at THAT channel's measure 0.0 = the window's 0 → seeded; the walk then
climbed `355992873:0` and three unnamed Harrison creeks feeding the channels (sids 161089 `356183555:0`, 161769
`356184678:0`, 196577 `356238811:0` — also bound live, not noticed in the ticket).

**Fix** (`pipeline/atlas/graph/tributaries.py`, `_mouths_at`), two flags, each mutation-pinned:
- `MOUTH_SEEDS_ONLY_AT_THE_CUT`: seed only from the reach piece AT the cut, only through the piece below it on the
  reach's OWN line (never a distributary), and never at the reach's natural mouth (the no-window path already
  skipped a piece with no lower bound).
- `THE_RECEIVING_RIVER_IS_NEVER_A_SEED`: a water whose code is a strict ancestor of the reach's (the receiving river or
  its side channels) is never seeded, whatever its order.
- `cache.POLICY_VERSION` 21 → 22, both flags pinned in `test_policy_version_is_bumped_when_classify_changes`.

**Rebuild needed: reach rerun + deliver ONLY.** The walk runs in the reach step (`reach/build.py` → `tributaries`),
over the existing `graph.pkl`; the atlas is untouched, so section ids do not move (the memory note's "atlas rebuild
required" is wrong for this fix).

**Binding diff plan.** Reach-only is deterministic, so no old-vs-old noise to subtract. Expected (measured by
re-walking every live trib rule's bundle reach old vs new over the live graph, every per-line window, 393 of 457
trib rules have a reach): **exactly one rule moves** — `r2:chehalis_river@2-19 / chehalis_river.r2` loses trib sids
**59983, 59984, 161089, 161769, 196577** (402 → 397 trib sections); nothing gained, no other rule moves. On the
rerun, diff per-rule bound sets (`ruleset`×`section_ruleset` by `(entry_id, rule_id, via)`) live vs new bundle and
`licensing_section.jsonl` live vs new (licensing designations walk with `expand` too; the probe did not cover them).
Acceptance: no Harrison (`gnis:15333`) section carries `chehalis_river.*`; any other change is a defect to explain.

**Tests**: `test_reach_tributaries.py` `test_t90_*` (5: the live shape whole-value, three mutation pins, a
distributary-at-cut pin) and `test_tributaries.py` (stub `_mouths_at` now a cut; new mouth test, mutation-pinned).

## Owner question (new)

- **Q-T90: does a creek joining the RECEIVING river at the same point as a reach's mouth count as a tributary of the
  reach?** The old seeding said yes (pinned by `test_a_side_creek_at_the_mouth_is_seeded`, now inverted), but it only
  ever fired by measure coincidence (the window's 0 vs the receiving river's own measures). **Default built: no** — it
  is the receiving river's tributary (book p80: tributaries contribute to "a larger stream"), and the no-window path
  never seeded at a mouth. Live effect: none beyond the Chehalis rule.

## Review (2026-10-22, adversarial)

- **Defect fixed (32aa5a12):** 04d8fb85 had DELETED the "walk never enters the entry's own water" section of
  `test_reach_tributaries.py` (the `vedder` fixture + 5 tests, incl. the `THE_WALK_NEVER_ENTERS_THE_ENTRYS_OWN_WATER`
  mutation pin) — the T90 block replaced it instead of sitting beside it. Restored verbatim; all pass.
  Targeted: `test_reach_tributaries` + `test_tributaries` + `test_reach_builder` 97 passed.
- **Reproduced on live:** bundle binds `chehalis_river.r2` via `trib` to 402 sids incl. 59983, 59984, 161089, 161769,
  196577. On the live `graph.pkl` (heavy.sh probe), the Chehalis window seeds `355992877:0` with the flags off and
  nothing with them on.
- **Seed-level probe, whole graph:** the no-window path (carve-out "everything upstream", designations without a
  window) — 148,621 cut pieces, **0 change**. A window running to a line's MOUTH (lo 0) — 1,229,328 mouth pieces,
  **241 would have seeded under the old code** (all drops, nothing gained). Only rules/designations whose window
  reaches a mouth at one of those 241 can move; the builder's re-walk says only the Chehalis among trib rules.
  Licensing designations were not re-walked — the `licensing_section.jsonl` diff on the rerun is required, and
  Q-T90's "no live effect beyond the Chehalis" covers rules only until that diff is read.

