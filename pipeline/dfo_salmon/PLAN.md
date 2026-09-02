# Plan — keeping the DFO salmon data fresh on a schedule

**Status: plan only. Nothing here is built.** The scraper, parser, untangler, stability
harness and churn tool exist; the curated bindings, the cron job and the reconciler do
not. Numbers below are measured — reproduce with `python -m pipeline.dfo_salmon.churn`.

---

## 1. What the history actually says

The design follows from one measured asymmetry. Archived versions, re-parsed with the
current parser so any difference is the source moving and not us:

| Span | Waters | Still present today | Structure | Rules |
|---|---|---|---|---|
| 2017-07 → now (9 yr) | 45 → 76 | **45 / 45 (100%)** | B split into B(i)/B(ii) | rewritten many times |
| 2018-05 → now (8 yr) | 28 → 76 | **28 / 28 (100%)** | ” | ” |
| 2024-04 → now (2.3 yr) | 77 → 77 | **77 / 77 (100%)** | unchanged | +110 / −97 |

**No Region 6 water name observed in 2017 or 2018 has ever disappeared.** The list only
grows. Section A's baseline is substantively identical across all nine years (only
`Apr 01` → `Apr 1` formatting drift). Meanwhile roughly half the rules turn over yearly.

Four behaviours that the schedule has to survive:

1. **Append-mostly waters.** New waters appear; old ones do not vanish permanently.
2. **Seasonal presence.** A water or reach leaves the table when its fishery closes and
   returns later — the Kispiox "near Kispiox River Resort" reach was gone 2024-08, back
   2025-04, gone 2025-09, back 2026-04. *Absence is not deletion.*
3. **Text drift without substantive change**, in every free-text column:
   * scope — "Highway 37 Bridge" → "Highway 37 bridge"; the sign count at the Kispiox
     confluence has flipped between "three white triangular" and "the 4 triangular"
     **and back**;
   * species — "Sockeye, Pink & Chum" (2017) → "Sockeye, pink and chum" (2026);
   * dates — "Apr 01 to Mar 31" → "Apr 1 to Mar 31".

   Nothing may be keyed on any of these strings. All three are pinned by tests.
4. **Rare structural change.** One in nine years: section B became B(i)/B(ii), split at
   the CNR Railway Bridge at Terrace. Rare, but it re-scopes every water beneath it.

### The design correction this forces

My earlier suggestion — key entries on `(slug, section, name)` — **is wrong**, and the
decade of data is what shows it. Behaviour 4 would orphan every Skeena-watershed binding
the day B became B(i)/B(ii), which is exactly the failure `registry-change-entry-resync`
already cost this repo once.

> **Identity is `(region_number, normalized_water_name)`. Section is an *attribute* of
> the entry, versioned, never part of its id.** A section change then becomes a diff to
> review, not a re-derivation.

---

## 2. Proposed shape

```
cache/dfo_salmon/                 gitignored — machine state
  raw/regionN-eng.html            latest good snapshot
  history/regionN_<ts>.html       archived versions (churn tool)
  manifest.json                   sha256 + dateModified + fetched_at per slug

pipeline/dfo_salmon/entries/      COMMITTED — the curated half
  region-6.json                   waters -> reaches -> bindings; locked flags

output/dfo_salmon/                generated, disposable
  regionN.json                    faithful rows (parse.py)
  untangled/regionN.json          waters -> reaches -> rules (untangle.py)
  reconcile/regionN.json          what changed this run, and what needs a human
```

### The split that makes it work

| | Curated (entries file) | Scraped (regenerated every run) |
|---|---|---|
| water identity, aliases | ✔ | |
| reach key + resolved extents/geometry | ✔ | |
| `locked`, `reviewed_by/at`, `dormant` | ✔ | |
| `scope_history[]` — every wording seen | ✔ | |
| species, dates, limits, gear | | ✔ |
| fishery notices (FN####) | | ✔ |
| section membership | attribute, versioned | ✔ observed |

**Rules are never curated.** They are replaced wholesale on every run and joined to the
curated binding by `(entry_id, reach_key)`. That is the half that turns over ~50%/year;
curating it would be throwing the work away annually.

**Reach identity is a curator-assigned key, never the scope string** (behaviour 3). The
scope text is stored as evidence in `scope_history[]`, not as the key.

---

## 3. The scheduled run

```
fetch → validate → hash-compare → [unchanged? stop] → parse → untangle
      → reconcile against entries → classify → publish or open a review queue
```

### 3.1 Cadence

Driven by the measured `dateModified` cadence, not a guess:

| Regions | Observed change pattern | Proposed |
|---|---|---|
| 6, 1, 3 | in-season, days apart in Jul–Oct | **daily** Jun 1 – Oct 31, weekly otherwise |
| 2, 5a, 5b, 7 | around Apr 1 + occasional in-season | **weekly** |
| 4, 8 | annual (2025-04-01 unchanged since) | **weekly** (cheap; catches surprises) |

Nine pages at ~0.5 s each is a ~15 s job. Cost is not the constraint; **review effort
is**, so the schedule should be tuned to keep the review queue small, not to save requests.

### 3.2 Early exit

`manifest.json` already holds a per-page `sha256`. If every page's hash is unchanged,
the run stops before parsing. Measured: bodies are byte-identical across repeat fetches,
so the hash is a sound signal. Do **not** use `dateModified` as the trigger — the 2017
and 2018 snapshots have no `dateModified` element at all, and in-season Fishery Notice
edits do not reliably move it.

### 3.3 Reconciliation — the only interesting step

For each region, diff the freshly untangled output against the entries file:

| Outcome | Test | Action |
|---|---|---|
| **unchanged** | reach key resolves, scope text identical | publish |
| **drift** | scope text changed, similarity ≥ 0.75 on the same water | auto-match, append to `scope_history`, publish, **note** in the report |
| **new reach** | no near-match on that water | **hold** — needs a curator to bind geometry |
| **dormant** | curated reach absent from the page | mark `dormant: true`, keep the binding, publish without it |
| **revived** | dormant reach reappears | un-dormant, reuse the existing binding, publish |
| **new water** | name not in entries | **hold** — needs curation |
| **section change** | a curated water's section attribute moved | **hold + alarm** — re-scopes everything under it |
| **structural** | banner set changes, table missing, row count moves > 30% | **hold everything, alarm** |

`churn.compare_reaches()` is already the drift matcher and is unit-tested. Everything
else above is new.

**Publishing is never blocked by a hold.** A held item keeps its last-known-good
binding; only the affected water is withheld. A scraper that goes dark because one new
creek appeared is worse than one that carries 76 waters and flags the 77th.

### 3.4 What runs it

GitHub Actions on a cron, matching the archived in-season scraper's precedent. Two
notes carried from that job:

* It exists because BC-gov's Akamai WAF dropped plain `requests` from Actions runners.
  DFO does not do this today (measured: 8/8 plain fetches valid), but the runner IP is a
  different risk profile from this laptop. `fetch.py` already impersonates Chrome.
* The job needs no secrets and no `claude` CLI — **this pipeline spends no credits**, so
  unlike the parser it is safe to automate.

Failure handling: a region that fails all 4 attempts is skipped, not fatal; the run
reports partial success. Two consecutive failed runs for the same region should alarm.

---

## 4. Testing strategy

The value of the history cache is that it is a **regression corpus, not just evidence**.

### 4.1 Already in place (53 tests)

Committed fixtures pin the two silent-failure modes: the unterminated HTML comment in
Regions 4, 7 and 5a, and the `colspan="6"` on a five-column table.

### 4.2 Sourcing the corpus — do not depend on Wayback

Measured 2026-08-29: the CDX index lists **67 distinct-content Region 6 snapshots since
2016**, but a single patient pass sampling one per half-year cached **3 of 21** — the
other 58 attempts returned HTTP 503. The CDX endpoint itself 504s under load. Wayback is
fine for a one-off archaeology run and unusable as a scheduled dependency.

Two consequences:

* **The corpus grows from our own snapshots.** Once the scheduled run exists, every
  execution appends to `cache/dfo_salmon/history/` for free, at a real cadence, with no
  third party in the path. Wayback is only needed for the years before we started.
* **Backfill opportunistically, never blocking.** A `--backfill` mode that fetches a few
  older versions per run and gives up quietly will assemble the decade over weeks.
  Nothing should ever fail because the archive is busy.

The current corpus is 2017, 2018 and 2024-04 → 2026-08. **The 2019–2023 gap is real**:
the append-only property is verified across that window's endpoints, not through it. A
water that appeared and vanished entirely within those years would not be visible.

### 4.3 To add — replay the decade

* **Parse every cached historical snapshot; assert no exception and `verify()` passes.**
  Already true for 2017 and 2018 markup, which predates the `dateModified` element
  entirely. This is the cheapest guard against a parser change that only works on the
  current page.
* **Assert the monotonic property**: no water name present in an older snapshot may be
  absent from the entries file. If that ever fails, either DFO broke a 9-year pattern or
  we broke the parser — both need a human.
* **Golden reconcile fixtures**: 2025-08-14 → 2025-09-18 is a real drift-and-vanish pair
  (Kispiox reach disappears); 2024-04 → 2024-08 is a real add pair. Freeze both as
  reconciler test cases so the classification table above is executable, not prose.
* **A synthetic structural break** — delete a banner, rename a section — asserting the
  run *holds and alarms* rather than silently rebinding.

### 4.4 The check that matters most

Re-parsing history with today's parser must be **stable over time**: if a parser change
alters the 2017 output, that is a regression even though nobody reads 2017 data. The
churn tool's numbers are the assertion — they should not move unless the source did.

---

## 5. Open questions for you

1. **Reach keys**: auto-assign (`babine-lake-r1`, stable by insertion order) or have the
   curator name them (`babine-lake-excl-tribs`)? Auto is less work; named survives
   reordering and reads better in a diff.
2. **Where do bindings live** — a new `pipeline/dfo_salmon/entries/`, or do DFO reaches
   become rows in the existing `splits.json` / registry machinery? Reusing the registry
   gets anchor resolution for free but mixes two legal authorities in one file.
3. **Review surface**: does DFO reconciliation belong in `curation-review/`, or is a
   generated Markdown report in the PR enough for ~5 held items a year?
4. **Does the app overlay DFO on the provincial rules, or keep them on separate layers?**
   This decides whether a held DFO water blocks anything user-visible.
