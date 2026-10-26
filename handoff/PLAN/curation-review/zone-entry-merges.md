# Zone entry merges — proposal (owner to accept; catalogue untouched)

Owner, 2026-10-26: can zone regulations be combined into fewer entries with more rules each?

**Method.** Every `z<region>:*` entry (132, incl. `zp:`) was set against its zone page in the book
(pp. 13, 21, 28, 34, 42, 49, 58, 64, 68). A pair counts as a candidate when it has the same topic, the
same scope (entry extent) and sits under the same or the next heading. Impact was counted from the
working tree on 2026-10-26: exempt targets in the catalogue, `marks.jsonl`, `pipeline/tests` (py files /
fixtures), app files (`app/packages`, `app/conformance`, `app/fixtures`), pipeline code.

**Marks: none move.** `marks.jsonl` holds 9 marks, all on water entries (Hatzic, Dragon, Iskut,
Sproat, Trepanier, Halfway, Peachland). No zone entry has a mark, so no merge below loses review state.

## Two hard constraints

1. **A zone default is lifted by ENTRY, not by rule.** `Exempts.default_id` (`rule.py`
   `EXEMPTABLE_DEFAULTS`: spring/summer/steelhead stream closure, trout/char winter release, bait ban,
   single barbless hook) lifts *every rule of that zone entry*, and `DEFAULT_LIFTS` (`gear_omission.py`)
   classifies the phrase per slug. 104 water rules lift these by slug (R3 spring 26, R5 19, R7 17,
   R8 14, R4 8+5+1, R1 summer 5 + bait 4, R7 bait 5). If the "General Regulations" block were one
   entry, "exempt from spring closure" would also lift the barbless hook and bait ban. **These six
   entries cannot merge** unless `default_id` becomes rule-level first. That is a code change, not a curation one.
2. **Rule keys are `entry_id::rule_id`.** A merge changes every key of the merged rules: the bundle,
   the release rule list, the app goldens and the harness fixtures. Old `rule_id`s can be KEPT inside the
   new entry: `rule_id` only has to be unique within its entry (AGENTS 8), and `trout_char_quota.rN`
   and `species_quotas.rN` don't collide. That leaves every bare-rule-id test, `within` and
   exempt `target` alone. Only `entry_id`s change. The proposals below assume this.

## Candidates, region by region

### Daily quotas (every region): trout/char quota + "Other daily quotas"

The book prints these as ONE block, "Region N Daily Quotas (See tables for exceptions)". The trout/char lines
come first, then the species lines (bass … yellow perch). The scope is the same in all ten (`area:region:N`; Region 1 both
`outside_area` Haida Gwaii). The topic is the same: daily quotas.

| New entry | Merges (rules kept as is) | Rules | Exempt `entry_id`s to rewrite | Tests py / fixtures / app files | Code |
|---|---|---|---|---|---|
| `z1:daily_quotas` | `z1:trout_quota` (9) + `z1:species_quotas` (6) | 15 | 0 | 8 / 0 / 5 | `export/cases.py` |
| `z2:daily_quotas` | `z2:trout_char_quota` (11) + `z2:species_quotas` (7) | 18 | 13 (Khartoum, Lois, …) | 24 / 4 / 14 | `dfo_salmon/typed.py` (docstring), `export/cases.py`, `export/examples.py`, `ANSWERS-SPEC.md` |
| `z3:daily_quotas` | `z3:trout_char_quota` (9) + `z3:species_quotas` (8) | 17 | 2 | 16 / 2 / 4 | `export/cases.py`, `examples.py`, `codec.py`, docs 18 |
| `z4:daily_quotas` | `z4:trout_char_quota` (4) + `z4:species_quotas` (11) | 15 | **79** (22 + 57) | 16 / 0 / 12 | `export/cases.py`, `examples.py` |
| `z5:daily_quotas` | `z5:trout_char_quota` (9) + `z5:species_quotas` (5) | 14 | 8 | 11 / 3 / 6 | `export/cases.py`, `examples.py` |
| `z6:daily_quotas` | `z6:trout_char_quota` (10) + `z6:species_quotas` (8) | 18 | 14 | 17 / 2 / 6 | `export/cases.py`, `examples.py`, docs 18 |
| `z7a:daily_quotas` | `z7a:trout_char_quota` (9) + `z7a:species_quotas` (6) | 15 | 14 (12 + 2) | 7 / 2 / 6 | `export/cases.py`, `examples.py` |
| `z7b:daily_quotas` | `z7b:trout_char_quota` (9) + `z7b:arctic_grayling` (5) + `z7b:species_quotas` (11) | 25 | **48** (4 + 6 + 38) | 12 / 3 / 9 | `checks.py` (two keyed rows), `export/cases.py`, `examples.py` |
| `z8:daily_quotas` | `z8:trout_char_quota` (6) + `z8:species_quotas` (9) | 15 | 2 | 9 / 2 / 3 | `export/examples.py` |

The Haida Gwaii quota (`z1:hg_quota`, its own box and its own area) stays separate.

What else it touches:
- **Source / heading.** `name` is the review's heading (`review.py`: "THE RAW NAME: the heading").
  It becomes "Region N Daily Quotas", which is the book's heading. Today the two halves are named "Trout and char daily
  quota" and "Other daily quotas", and neither is printed. `regs_verbatim` = the two verbatims joined
  in book order. `source_pages` doesn't change (same page).
- **Card groups.** `rows/groups.py` pools a total with its sub-caps by `within` + same `entry_id`. Every
  `within` today stays inside one half, so no pool changes. The badge's `entry_name` ("the region's"
  count, `badge_of`) would read the new name. The wording changes, the numbers don't.
- **Answers.** These should be neutral: `ladder.effective_rules` caps by every binding rule regardless of entry. Prove it with
  a binding diff (only entry-id renames) + `card_check` + `test_q44` + the RULINGS Q16 check.
- **Curation review.** 9 zone entries instead of 19. The per-bundle rule-section count is cached (aff6039d), so the
  bigger entries cost nothing.
- **Tests.** Most pins use a full key (`z2:trout_char_quota::trout_char_quota.r1`). That's a mechanical
  `entry_id` rename, about 120 test-file hits across regions, plus regenerated fixtures (`answers28_*`, `t183_two_regions`,
  `t57_trib_sweep`, taps / harness / regs-fixture).

**Recommend: yes, as one change set for all nine** (one rename map, one rebuild, one diff). It mirrors
the book's box and halves the zone entries the reviewer pages through. The cost is mechanical, and the
biggest rewrites (Region 4's 79 and Region 7B's 48 exempt `entry_id`s) can be done by script and are proved by the binding
diff. If only some are wanted: R1, R3, R5, R8 are the cheapest, and R4 / R7B carry the most rewrites.

### Possession quota: next heading after Daily Quotas (R1–R6, R8; R7A/R7B `possession_exceptions`)

Same topic (quotas) and an adjacent heading. Scope is the same except **R1**, where possession covers all of Region 1
including Haida Gwaii but the daily quotas exclude it. Impact is small: 1 rule each, 0 exempts, 0–2 tests, 2–9
app files. `glossary.py` names `z1:possession_quota`, and the app's `standing.ts` possession row
reads the rule by `period`, not by entry.

**Recommend: no.** The book prints it under its own heading. It's a `period` rule for every daily quota
(including Haida Gwaii's), not a member of one table, and in R1 it would need a rule-level extent that is wider
than its entry's. That gives one fewer entry per region for a scope exception.

### General Regulations block: closures, barbless, bait ban, set lining

| Region | Same-heading entries |
|---|---|
| 1 | `summer_stream_closure` (MUs 1-1–1-6), `single_barbless_hook` (R1), `bait_ban_streams` (R1 excl. HG), `hg_bait_ban_streams` (HG) — four scopes |
| 3 | `spring_stream_closure`, `single_barbless_hook` |
| 4 | `spring_stream_closure`, `trout_char_winter_release`, `single_barbless_hook` |
| 5 | `spring_stream_closure` (Fraser watershed only), `single_barbless_hook`, `no_minimum_size_lakes` (lakes) |
| 6 | `skeena_nass_bait_ban`, `steelhead_stream_closure`, `skeena_nass_winter_closure`, `iskut_fraser_closure`, `single_barbless_hook`, `set_lining` — five scopes |
| 7A | `set_lining` (lakes), `spring_stream_closure`, `single_barbless_hook`, `bait_ban_streams` (all streams of Zone A) |
| 7B | `single_barbless_hook`, `bait`, `set_lining` |
| 8 | `spring_stream_closure`, `single_barbless_hook` |

**Recommend: no.** Constraint 1 applies: closure, barbless and bait-ban entries are lifted whole by
`default_id`, and merging them would make "exempt from spring closure" lift the hook and bait rules too. The
remaining pairs mostly differ in scope (streams vs lakes, watershed vs region). The one clean same-scope pair
without a default is **7B `bait` + `set_lining`** (both Zone B, both method/bait rules). It saves one entry,
has 0 tests and 1 fixture, and isn't worth a separate pass.

### Notices and advisories

| New entry | Merges | Same scope / adjacent | Impact | Recommend |
|---|---|---|---|---|
| `z6:skeena_information` | `skeena_quality_waters` + `skeena_fisheries_management` (both advisory URLs, Region 6, top of the right column) | yes / yes | 0 tests, 1 fixture + 1 app file each, 0 exempts | Optional. Harmless and small. |
| `z7b:daily_quotas` (above) | + `bull_trout_dolly_varden` (NOTE printed in the quota block) | yes / yes | 0 tests, 1 fixture | Yes if 7B is merged. It's a note of that table. |
| `z5:daily_quotas` | + `no_minimum_size_lakes` | no (lakes; General Regs heading) | 0 tests | No. Different heading. (R2 keeps the same note in its quota as `r9`, R1 as `trout_quota.r8`. The book differs by region.) |
| — | `z4:kootenay_annual` + `z4:kootenay_lake_boundaries` | lake vs whole; different headings | `kootenay_annual` has no rules (a `see`) | No |
| — | `z6:atlantic_salmon_alert` + `z6:salmon_quotas_notice` + `z6:first_nations_notice` | region-wide but three headings, three columns | 0 tests | No. Nothing in common but "notice". |
| — | `z7a:tagging_program` + `z7a:stocked_lakes` / `ice_fishing_huts` | different topics | — | No |

### Not merges, but overlaps worth a look (same idea as overlapping-rules.md)

`z3:bass_perch_illegal`, `z4:invasive_species_notice` and `z5:bass_illegal` restate the species-quota
closures ("Bass: 0 quota, CLOSED TO FISHING", perch, pike, walleye) under their own NOTICE heading.
They're printed separately and `z4:invasive_species_notice` carries 52 exempt targets of its own (the
"with the exception of certain waters" rows), so they stay separate entries. They are a candidate for an
overlap review, not for a merge.

### Provincial (`zp:`), for completeness

The pp. 6–10 entries are already one per book heading (Terminal tackle, Bait, Spear fishing, …). The only
adjacent same-topic pair is `zp:preparing_catch` + `zp:transporting_catch` (p.10, handling, everywhere).
Each has 11 app files and 0–1 tests. **No.** Separate headings, and the app's conduct sheet already groups them.

## Summary

| Merge | Entries saved | Worth it |
|---|---|---|
| Daily quotas, 9 regions (+ 7B grayling + 7B NOTE) | 11 | **Yes**, one change set |
| Possession into daily quotas | 9 | No (own heading; R1 scope) |
| General Regulations block | up to ~15 | **No.** Blocked by entry-level `default_id` lifts. |
| 7B bait + set lining | 1 | No (too small) |
| Skeena advisories | 1 | Optional |

Apply as the overlapping-rules flow did: script the rename (keep `rule_id`s, rewrite exempt `entry_id`s),
rotate `reaches/full`, run reach + deliver, binding diff (expect only renamed keys), `card_check` / `test_q44`,
re-cut taps + harness + regs-fixture, re-pin key-pinned tests, one full suite.
