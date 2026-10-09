# 19 — The DFO salmon pipeline

How a federal salmon page becomes a rule on a reach, and why it is built in two halves that meet
only at the end.

Read this with `pipeline/regs/dfo_salmon/README.md` (what each module does) and
`pipeline/docs/archive/HANDOFF-dfo-curation.md` (how to run a curation sitting).

---

## 1. The one measurement the design follows

Archived versions, re-parsed with today's parser so any difference is the source moving:

| Span | Waters | Still present | Rules |
|---|---|---|---|
| 2017-07 → now (9 yr) | 45 → 76 | **45 / 45** | rewritten many times |
| 2024-04 → now (2.3 yr) | 77 → 77 | **77 / 77** | +110 / −97 |

**The geography never moves. The rules turn over about half a year.** So the expensive half —
which registry item a name is, where the cut-points sit, whether tributaries are in — is curated
once by a human and kept forever. The cheap half is thrown away and re-scraped every run.

Everything below is that sentence, mechanised.

```
                         ┌─────────────── scraped, disposable ───────────────┐
  page ──▶ fetch ──▶ parse ──▶ untangle ──▶ locations ──┬──▶ typed ──▶ feed
                                                        │
                                                        │ (fingerprint)
                                                        ▼
                              entries seed / reconcile ──▶ CURATED locators
                         └─────────────── curated, durable ──────────────────┘
                                                        │
                                                   join at read time
                                                        ▼
                                                    the bundle
```

---

## 2. The stages

| # | module | in | out | trust |
|---|---|---|---|---|
| 1 | `fetch` | the live page | `cache/dfo_salmon/raw/` + `manifest.json` | machine |
| 2 | `parse` | raw HTML | rows — the five printed columns, verbatim | machine |
| 3 | `untangle` | rows | waters → reaches → rules, cascade resolved | machine |
| 4 | `locations` | untangled | **locators** + **rule records**, split apart | machine |
| 5a | `entries` | locators | `data/curated/…/entries/dfo_salmon/` | **human** |
| 5b | `typed` + `feed` | rule records | `data/generated/regs/dfo_salmon/typed/` | machine |
| 6 | *(not built)* | both | the bundle | machine |

Stage 4 is the fork. Everything before it is common; nothing after it crosses over until stage 6.

### 2.1 fetch — never overwrite good with bad

The server sends no `ETag` and no `Last-Modified`, so conditional GET has nothing to bind to.
`manifest.json` keeps a per-page `sha256` instead, and bodies are byte-identical across repeat
fetches (measured 32/32), so the hash is a sound change signal. A 200 that is a WAF interstitial
or a truncated body is a *failed attempt*, not a snapshot — a good file on disk is never replaced
by a bad fetch, and writes are atomic.

### 2.2 parse — transcription, and nothing else

Every `<tr>` becomes one row carrying all five columns. **It reads nothing out of them.** The
`Limits/Gear` and `Dates` cells are handed over exactly as printed.

That rule is new and was paid for. Derived flags (`no_fishing`, `daily_limit`, …) used to be
computed here and carried through two more dataclasses to reach their only consumer. Nothing else
in the repo ever branched on them. Splitting the decode across two modules meant one wording had
to be fixed in two places — which is how ten `Finfish closure` rows came to state nothing at all,
publishing an open-looking water the page closes outright.

The one derivation that stays is `fishery_notices`, scraped from `<a>` hrefs: only the HTML can
see a link.

> **Never hand the whole page to a DOM parser.** Regions 4, 7 and 5a carry an unterminated `<!--`
> above the table, so `soup.find("table")` returns `None` — silently, no exception. The table is
> sliced out of the raw HTML by regex. Pinned by tests.

### 2.3 untangle — the cascade

Region 6 is not a list, it is eight lettered scope bands where a broad default is progressively
narrowed. Read flat it says the opposite of what it means. `untangle` regroups rows into
waters → reaches → rules plus the defaults, and `verify()` asserts every parsed row lands in
exactly one rule, so the readable view can never quietly lose one.

### 2.4 locations — where the two halves separate

`extract()` returns `(locators, rule_records, structure)` from one untangled region.

* A **locator** is a place a rule can attach to: a named water's reach, or a cascade default
  (section A, "tidal water Area 5"). It carries the scope text, the decomposed `op`, landmark
  triage, tributary scope, and its **fingerprint**.
* A **rule record** is `species × dates × limits`, carrying **no identity of its own** — only the
  fingerprint of the locator it sits on. They are replaced wholesale every run: no ids, no
  diffing, no merge.

---

## 3. How a locator is parsed, and how it keeps its identity

```python
fingerprint(section, water, specific_area)      # sha256[:16] of the normalised triple
```

Identity is the thing this pipeline is careful about, and each rule below was bought:

* **`location_id` is assigned once and derived from nothing.** Rules and geometry hang off it.
  Deriving it from text would orphan a binding the day `"Highway 37 Bridge"` became
  `"Highway 37 bridge"`.
* **`section` is an attribute with history, never part of the id.** Section B became B(i)/B(ii)
  between 2018 and 2020. Every Skeena binding had to survive that, and would not have if the
  section were in the key.
* **`fingerprints` is a list, not a value.** DFO flipped the Kispiox sign count between "three
  white triangular" and "the 4 triangular" *and back*. The second flip must cost nothing.
* **`status: dormant` instead of deletion.** These pages list *openings*, so a locator leaves when
  its fishery closes and returns later — the Kispiox River Resort reach has cycled out and back
  four times since 2024. Absence is not deletion; the binding is kept.

### 3.1 The curated payload

What a human supplies, per locator, and what nothing can re-derive:

| field | what it is |
|---|---|
| `WaterBinding.item_ids` | **the only place a name becomes geometry.** Set by an exact registry hit or by a curator — never heuristically. A near-spelling is a *suggestion*, never a binding. |
| `Binding.extents` | the cut-points: `whole`, `upstream_of`, `between`, … validated by the shared provincial `Extent` model |
| `Binding.tributaries` | three-valued; `None` inherits the water |
| `Binding.notes` / `spatial_caveat` | radius closures and lake lines kept as text, because they are not geometry — and the app must not draw them as a plain fill |

### 3.2 seed and reconcile

```bash
.venv/bin/python -m pipeline.regs.dfo_salmon.entries seed        # add locators new to the page
.venv/bin/python -m pipeline.regs.dfo_salmon.entries reconcile    # what changed, and how bad
```

`seed` **never edits an existing record** — a locked, hand-bound locator is untouched, so this is
simply how a new season's locators enter the file. `reconcile` classifies every locator and never
mutates:

| outcome | severity | meaning |
|---|--:|---|
| `ok` | 0 | fingerprint resolves, nothing moved |
| `dormant` / `revived` | 0 | absent from the page / published again — binding kept either way |
| `drift` | 1 | wording changed; a near-match exists, confirm the rebind |
| `new` | 2 | no known locator on this water — needs a curator |
| `section_moved` | 3 | re-scopes everything bound beneath it |
| `structural` | 4 | banner set changed, or the locator count moved >20%, or >25% unbound — **hold the region** |

---

## 4. How the feed connects

```
rule record ──[fingerprint]──▶ EntryFile.by_fingerprint() ──▶ location_id ──▶ Binding.extents
```

**The join key is the fingerprint** — the scrape's own key, the way a gauge reading carries its
station id. Deliberately *not* `location_id`, for two reasons:

1. `location_id` exists only on the curated side, so keying the feed on it would make the feed
   unbuildable without curated state. `feed.py` reads none, and a test asserts it.
2. A curator merging two locators would silently re-key every rule underneath them.

Wording drift is absorbed on the curated side, where `fingerprints` is a list. So a reworded
scope re-binds there and the feed never notices.

This is the same shape as the gauges: `data/curated/gauges/matches.json` holds the
station-to-node match, written once by a human, while readings arrive on a schedule and are
joined at build time. **A reading is never written into the curated file, and neither is a quota.**

### 4.1 typed — the whole decode, in one place

`typed.py` owns every reading of a row: the limits cell (`decode()`), the dates cell
(`interpret_dates()`), and the conversion to `CatalogueRule`. One wording, one place to fix it.

Because the pages are a *table*, this is a pure function of the row — **no model, no credits**,
unlike the synopsis reparse it mirrors.

Three things it must get right, each one a defect the repo has already paid for once:

* **`take=0` does not mean closed.** `may_target` separates "do not fish for this" from "fish for
  it and release it". `no_fishing → False`, `non_retention → True`. A bare `"0 per day"` says
  neither, so it publishes the conservative way and is flagged for review rather than guessed.
* **A size sub-limit is its own rule**, pointing at its parent with `within` — the convention
  `z2:trout_char_quota` already uses. `"4 per day, only 2 over 50 cm"` is two rules; one rule
  carrying both numbers is the shape that loses the 4.
* **A gear rule is scoped by what you fish FOR, not what you may keep.** `species` on a bait ban
  reads narrower than the law; `when_targeting` is the scope the tables state.

It also restores a guard the catalogue dropped: `CatalogueRule` does **not** check that a window
parses (the retired prose `Rule` did), so `windows=["Smarch 40 to Bluneteen 99"]` builds without
complaint. An unreadable season goes to review; an unambiguous source typo is published repaired
and recorded. An *empty* dates cell stays a fact — no window means the rule applies all year.

### 4.2 Precedence stays on the curated side

Region 6 is the only cascade, and it is the case that proves the split. **Precedence is a
property of the locator, not the rule** — "chinook 4/day" is a section-A statement because of
*where* it sits, not *what* it says — so the ranking lives with the curated locators and the feed
carries none of it. Resolution is `EntryFile.chain_for(section)`, narrowest first:

```
Kispiox River (B(i), precedence 3)   coho 4/day, Jul 15 to Aug 23 — max 2 over 50 cm
        falls back to B(i)  (prec 1) no fishing for coho, Apr 1 to Mar 31
        falls back to B     (prec 1) — a parent header, no rules of its own
        falls back to A     (prec 0) coho 4/day, Apr 1 to Mar 31
```

Read flat, section A says the Kispiox is open for coho all year. It is not: B(i) closes it, and
the water reopens a five-week window. That inversion is why the ranking may never be dropped in
the join, and it is pinned by a test.

### 4.3 Current numbers

```
438 scraped rows  ->  517 typed rules   (a bundled row states more than one)
247 locators      ->  247 join to a curated record, 0 unbound
                       243 of those have no extent yet
 12 rules flagged: 11 "to be determined", 1 bare "0 per day"
```

---

## 5. The scheduled run (designed, not built)

```
fetch → validate → hash-compare → [unchanged? stop] → parse → untangle → locations
      → reconcile (curated)  +  typed → feed (scraped)
      → publish, and open a review queue for anything held
```

* **Early exit on the hash.** Not on `dateModified`: the 2017 and 2018 snapshots have no such
  element at all, and in-season Fishery Notice edits do not reliably move it.
* **Cadence** follows the measured change pattern — daily for Regions 6/1/3 in season, weekly
  otherwise. Nine pages at ~0.5 s is a ~15 s job; **review effort is the constraint, not cost.**
* **Publishing is never blocked by a hold.** A held locator keeps its last-known-good binding and
  only that water is withheld. A scraper that goes dark because one new creek appeared is worse
  than one that carries 76 waters and flags the 77th.
* **No credits, no secrets** — safe to automate, unlike the LLM parser.

---

## 6. What is not built

1. **Bundle ingest.** `ingest_catalogue.py` and `build_regs_v3_data` read the synopsis catalogue
   only. Nothing consumes the DFO feed yet.
2. **The geometry.** 243 of 247 locators have no extent. That is the cut-point curation, not a
   gap in the format — see the handoff.
3. **The cron job and the reconciler's publish step** (§5 is a plan).
4. **Overlay semantics.** DFO and the province are different authorities; a reach can carry a
   provincial trout rule and a federal salmon rule at once and neither overrides the other. How
   the app shows that is undecided.

---

## 7. The invariants, and where they are enforced

| invariant | enforced by |
|---|---|
| every parsed row lands in exactly one rule | `untangle.verify()` |
| the transcription reads nothing out of the limits cell | `test_the_transcription_reads_nothing_out_of_the_limits_cell` |
| the feed takes no curated input | `test_the_feed_takes_no_curated_input` |
| every scraped locator joins to a curated record | `test_no_scraped_rule_lands_on_a_locator_nobody_curated` |
| every rule's `verbatim` is a span of its own row | `test_every_scraped_rule_types_and_keeps_its_chain_of_custody` |
| `take=0` always says whether you may fish | `test_take_zero_says_whether_you_may_fish_at_all` |
| an unreadable season never publishes as "no season" | `test_an_unreadable_season_goes_to_review_rather_than_publishing_as_none` |
| a region that restructures holds rather than rebinding | `entries.reconcile` severity 4 |
| a quota the closed vocabulary cannot read fails loudly | `test_a_row_that_says_per_day_always_yields_a_quota` |
| the same page always produces the same feed | `test_the_feed_is_deterministic` |
| **the Skeena cascade resolves through the join** | `test_the_skeena_cascade_resolves_through_the_join` |
