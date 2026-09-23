# Reach builder — structure sketch

**Not built.** This is the shape, so it can be reviewed before code exists. Prerequisite: step 1 of
[`13-build-plan.md`](13-build-plan.md) (lifting the resolver into `pipeline/resolve/`), currently in
another agent's hands. Diagnosis of today's resolver: [`RESOLVER-HANDOFF.md`](RESOLVER-HANDOFF.md).

## What it is

A regulation says *"between the Tamihi Bridge and Vedder Crossing"*. The map is made of numbered
pieces of river. This turns the first into the second, for all 3,038 rules, and reports what changed
since the last build.

It **does not cut rivers** — it only selects among sections the sectionizer already made.

---

## Parts

```
pipeline/atlas/reach/
  __init__.py
  models.py      RuleBinding · Unresolved · Diagnostic · BuildReport      (pure dataclasses)
  build.py       the pass: entries + ResolveContext -> bindings           (orchestration only)
  classify.py    Reach -> bound | empty | unresolved(reason)              (the decision table)
  cache.py       sha256(entry ‖ build_id) -> cached result                (incremental)
  diff.py        two runs -> what changed, ranked by blast radius
  io.py          read entries · write tables · write report.json
  cli.py         python -m pipeline.atlas.reach.cli
```

Six small modules, one job each. `build.py` orchestrates and holds no rules; `classify.py` holds every
policy decision in one readable table so the answers to §"open policy" below live in a single file.

---

## Data flow

```
  entries/region-*.json ─┐
                         ├─▶ build.py ──▶ classify.py ──▶ bindings + unresolved + diagnostics
  ResolveContext ────────┘       │                              │
   (graph + registry,            │                              ▼
    from one --build)         cache.py                     io.py ──▶ data/generated/reaches/<build>/
                                                                      ├── rule_section.parquet
                                                                      ├── rule_unresolved.parquet
                                                                      ├── rule_extent.parquet
                                                                      ├── rule_diagnostic.parquet
                                                                      └── report.json
                                                                            │
                              diff.py ◀── a previous run ───────────────────┘
```

Per entry (entries are independent):

```
1  covered_ids      matched[] + also[]                     which items this row regulates
2  entry scope      resolve_extent × scope[]               clip set, or scope_unresolved
3  per rule/extent  resolve_extent -> Reach
4  union extents    per rule
5  clip to scope
6  classify         bound | empty | unresolved(reason)
7  emit             rows + diagnostics
```

---

## Outputs

| table | columns | why it exists |
|---|---|---|
| `rule_section` | `entry_id, rule_id, section_id` | the bindings — what the app reads |
| `rule_unresolved` | `entry_id, rule_id, reason, detail` | **every** failure, typed. Never a silent empty |
| `rule_extent` | `entry_id, rule_id, seq, op, splits, item_id, …` | the authored form kept beside the resolution, so any binding can be explained and rebuilt |
| `rule_diagnostic` | `entry_id, rule_id, kind, payload` | straddlers and ambiguous cuts preserved rather than dropped (㊴) |
| `report.json` | counts, reason histogram, timings, diff | what a human reads after a build |

`rule_id` is unique **within an entry only** (49 collide corpus-wide), so every table is keyed
`(entry_id, rule_id)` — never `rule_id` alone.

---

## Five properties, each load-bearing

| property | what it means | why it matters |
|---|---|---|
| **Pure** | no clock, no network, no env | same inputs → same outputs, always |
| **Deterministic** | sorted iteration throughout | **precondition for ⑬** — a non-deterministic reach builder makes a byte-identical bundle impossible no matter what the packager does |
| **Total** | every rule lands in exactly one of bound/empty/unresolved | ⑪ + ㊳ enforced at the point of production, not audited afterwards |
| **Incremental** | `sha256(entry ‖ build_id)` cache | confirming one entry re-resolves in ms — which is what lets the **review app call this** instead of keeping its own path. That is the structural fix for ㊲ |
| **Auditable** | authored extent stored beside its resolution | a binding can be explained without re-deriving it from geometry |

**Do not build parallelism.** Measured: the whole corpus resolves in **42 s**, 5 s of which is the
graph load. There is no performance problem to solve.

---

## Diff mode — the reason it is next

```bash
python -m pipeline.atlas.reach.cli --build data/generated/atlas/full     --out data/generated/reaches/full
python -m pipeline.atlas.reach.cli --build data/generated/atlas/full_new --out data/generated/reaches/full_new \
                               --against data/generated/reaches/full
```

> **It is `cli`, not `build`.** This page said `-m pipeline.atlas.reach.build` for a long time.
> `build.py` is the library — it holds `build_reaches` and no `__main__` — so running it does not
> error. It prints nothing and **exits 0**, which reads exactly like a build that had nothing to do,
> while the stale tables underneath it stay the ones every later step consumes.

Reports, per rule, the sections added and removed, grouped by entry, ranked by blast radius, with each
entry's `confirmed` flag shown.

One routine rebuild changed content on **2,950 items**. The curator's question is not *"which items
moved"* — it is **"which of my 108 confirmed entries now mean something different?"** Nothing today can
answer that. This is also where step 2's ⑥ reopen gate and the ⑬ determinism check naturally live.

---

## Open policy — decide in `classify.py`, not scattered

Every one of these is small, and each is currently undecided. Counts from the live corpus.

| # | question | affects | note |
|---|---|---|---|
| 1 | A **straddling** piece (attached inside at one end, outside at the other) — ship it or drop it? | **40 rules / 46 pieces** | Likely rule-type dependent: include for a closure (safe), exclude for an opening (safe). ㊴ |
| 2 | An **ambiguous cut** (one landmark, two measures) — use the lower silently, or refuse? | **63 rules** | The Mitchell bug proves the lower can be flat wrong. Leaning: refuse, and surface it |
| 3 | A rule with **several extents** — union, and what if one fails? | **9 rules** | Currently 0 partially resolve, so this is cheap to decide now |
| 4 | **Zero sections after the entry-scope clip** — real answer, or curation error? | **3 rules** | The Peace case: the rule describes a reach outside its own row |
| 5 | **Tributary rules** before the walk exists | **176 bounded + 554 total** | Emit direct sections + an explicit `tributaries_pending` marker, or withhold the rule entirely? |
| 6 | `no_registry` entries | **95 rules** | No item, so nothing to bind. Needs the text-only path (㊶) |
| 7 | `matched` entries with no extent | **12 rules** | ⚠️ **Do not default to `whole`.** All 12 carry a review flag (then `needs_review`; the catalogue says it with `review_reason`); 11 have `unresolved_locators`. They need curated splits authored |
| 8 | Items with **zero sections** | **14 rules** | Graph-build fix; here they just need the reason `no_sections_for_items` |

---

## Reason vocabulary

`rule_unresolved.reason` — closed set, extended only deliberately:

```
area_scope              within(area), not geometry              ㉔
area_id_dangling        area_id names nothing                   ㉔
no_sections_for_items   every scoped item has zero sections     ㉕
cut_not_found           a bound split is not on the scoped water
cuts_collapsed          between() whose two cuts resolve to one measure   (Mitchell)
parallel_branches       between() whose halves do not order
no_extents              nothing authored                        ㊶ / the 12
no_registry             entry has no matched item               ㊶
tributaries_pending     direct part bound; walk not built yet
```

---

## Acceptance criteria

- **A1** Deterministic: two runs over the same `(build, entries)` are byte-identical.
- **A2** Total: `bound + empty + unresolved == 3,038`; zero rules absent.
- **A3** No `empty` without a `rule_unresolved` row; no `unresolved` with `reason == None` (㊳).
- **A4** Full pass ≤ 5 min after graph load (measured baseline: 42 s).
- **A5** Incremental: re-resolving one changed entry touches only that entry, asserted by count.
- **A6** Diff reproduces `full` → `full_new` and reports the changed-rule set; the curated-cut survival
  figure (**270/271**) is derivable from it.
- **A7** The review app's `/reaches` endpoint is served **by this builder** — one implementation.
- **A8** Every `unclassified` and `ambiguous_cut` from the resolver reaches `rule_diagnostic` (㊴).
- **A9** Every table is keyed `(entry_id, rule_id)`; a test asserts no table keys on `rule_id` alone.
