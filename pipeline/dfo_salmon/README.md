# `pipeline/dfo_salmon` — DFO salmon limits, openings and closures (Regions 1–8)

Scrapes and structures the federal **salmon** regulations for BC's eight freshwater
regions from `pac.dfo-mpo.gc.ca`.

> **This is a different authority from the Synopsis.** Salmon in non-tidal BC water is
> managed by DFO (federal); everything else is the provincial Synopsis this repo is
> built on. Every DFO page says so itself: *"This table refers to fishing for salmon
> only. For information on other species please refer to the B.C. Freshwater Fishing
> Regulations Synopsis."* The two rule sets **overlay**, they do not merge — a reach can
> carry a provincial trout rule and a federal salmon rule at the same time, and neither
> overrides the other.

```bash
.venv/bin/python -m pipeline.dfo_salmon.fetch                    # snapshot -> cache/dfo_salmon/
.venv/bin/python -m pipeline.dfo_salmon.parse                    # faithful rows -> output/dfo_salmon/
.venv/bin/python -m pipeline.dfo_salmon.untangle --regions 6 --print   # waters -> reaches -> rules
.venv/bin/python -m pipeline.dfo_salmon.stability --burst --compare-clients
.venv/bin/python -m pytest pipeline/tests/test_dfo_salmon.py
```

Three stages, deliberately separate:

| module | job |
|---|---|
| `fetch.py` | snapshot the page, validate it, hash it, never overwrite good with bad |
| `parse.py` | faithful transcription — every `<tr>` becomes one row, nothing merged |
| `untangle.py` | regroup into waters → reaches → rules + the defaults, for consumption |

`parse.py` output is what you store; `untangle.py` output is what you read and what a
resolver should bind to. `untangle.verify()` asserts every parsed row lands in exactly
one rule, so the readable view can never quietly lose a rule the transcription had.

---

## What's there (snapshot 2026-08-29)

Pages are keyed by **slug**, not region number, because Region 5 is three pages:

| Slug | Name | `dateModified` | Rows | Sections | Waters | Reaches |
|---|---|---|---:|---:|---:|---:|
| `1` | Vancouver Island | 2026-08-25 | 78 | — | 27 | 55 |
| `2` | Lower Mainland | 2026-04-01 | 56 | — | 22 | 26 |
| `3` | Thompson-Nicola | 2026-08-14 | 20 | — | 6 | 7 |
| `4` | Kootenays | 2025-04-01 | 1 | — | 0 | 0 |
| `5` | Cariboo (index) | 2016-10-18 | — | — | — | — |
| `5a` | Cariboo Part A, Fraser River Watershed | 2026-04-01 | 3 | — | 2 | 2 |
| `5b` | Cariboo Part B, Coastal Watershed | 2025-08-11 | 38 | — | 9 | 13 |
| `6` | **Skeena** | 2026-08-28 | 236 | **8** | 77 | 128 |
| `7` | Omineca-Peace | 2026-04-01 | 2 | — | 1 | 1 |
| `8` | Okanagan | 2025-04-01 | 5 | — | 3 | 5 |

**`region5-eng.html` is a 2016 stub** carrying no table — it exists only to announce
that the Cariboo is split into **5A** (Fraser watershed, MUs 5-1…5-5 and 5-12…5-16) and
**5B** (coastal watershed, MUs 5-6…5-11). The rules live on `region5a-eng.html` and
`region5b-eng.html`, which the stub does not link prominently. Both are fetched; the
stub is marked `is_stub` and skipped by default. Three slugs, one `region_number: 5`.
Region 8 (Okanagan) also exists and is included.

Region 4 is a single blanket non-retention row; Region 7 is two rows on the Nechako.
The real content is Regions 1, 2, 5b and 6.

---

## Why the fetch is stable

Measured by `stability.py`, 2026-08-29 — 3 polite rounds + 1 burst round × 8 regions:

* **32/32 requests OK (100%)**, mean 0.46 s, zero retries (8 pages; 5a/5b added after).
* **Byte-identical bodies across rounds** for all 8 regions, so `sha256` is a sound
  change signal. (It has to be: the server is IIS and sends **no `ETag` and no
  `Last-Modified`**, so conditional GET has nothing to bind to.)
* **No rate limiting observed** — a zero-throttle burst scored the same 100% as the
  1.5 s-throttled run. The 1.5 s default stays anyway; nothing here is time-critical.
* **TLS impersonation is not required today** (8/8 plain-`requests` fetches valid), but
  `curl_cffi` + `impersonate="chrome124"` is used regardless, matching the archived
  in-season scraper. That scraper exists because BC-gov's Akamai WAF dropped plain
  `requests` from datacenter IPs; the cost of insurance here is zero.

The fetcher additionally:

* **validates before writing** — a 200 that is a WAF interstitial, a maintenance page
  or a truncated body is a *failed attempt*, not a snapshot. A good file on disk is
  never replaced by a bad fetch.
* **writes atomically** (`.tmp` + `os.replace`), so a killed run leaves no half page.
* retries 4× with exponential backoff + jitter, honours `Retry-After`, and lets one
  region's failure not abort the other seven.
* keeps `cache/dfo_salmon/manifest.json`: per region the `sha256`, byte count, HTTP
  status, the page's own `dateModified`, and when we last looked. Re-running only
  rewrites regions whose hash moved.

### The trap: never hand the whole page to a DOM parser

Regions 4 and 7 carry an **unterminated `<!--`** above the table (18 opens, 17 closes).
`BeautifulSoup(page).find("table")` therefore returns `None` — the comment swallows the
rest of the document. **No exception, no warning, just zero rows.** Both regions parsed
empty until the table was sliced out of the raw HTML by regex instead.
`test_dfo_salmon.py` pins Region 4, 7 and 5a row counts and asserts the fixtures still
contain the unbalanced comment, so a future rewrite can't quietly reopen the hole.
(Region 5a has it too — 18 opens, 17 closes.)

---

## Region 6 (Skeena) — what the table is actually saying

Every other region is a flat list of waters. **Region 6 is a cascade**: eight lettered
scope bands where a broad default is progressively narrowed. Read flat, it says the
opposite of what it means — section A alone reads "4 chinook per day everywhere in the
Skeena", when in practice most of the watershed is closed to chinook outright.

### The eight bands

| Key | Scope | How it binds |
|---|---|---|
| **A** | All Region 6 waters | The region-wide default: chinook 4/day (only 1 over 65 cm), coho 4/day (only 1 over 50 cm), **sockeye/pink/chum closed**. |
| **B** | Skeena River Watershed | A parent header only — no rules of its own. Splits into B(i)/B(ii). |
| **B(i)** | Skeena **upstream** of the CNR Railway Bridge at Terrace | Everything closed Jan 1–Jun 15, plus year-round closures on coho, sockeye, chinook and chum — then 19 named waters reopen specific windows. |
| **B(ii)** | Skeena **downstream** of that bridge | Chinook/sockeye/coho/pink closed by default; 14 named waters reopen. |
| **C** | Nass River Watershed | Coho 4/day Jan 1–Oct 31 then closed; chinook closed. 10 named waters. |
| **D** | Queen Charlotte Islands (Haida Gwaii) watersheds | Chinook closed; coho 4/day Apr 1–Oct 31; **single barbless hook** throughout. 5 named waters. |
| **E** | Other mainland watersheds, except the Fraser | Coho closed Nov 1–Dec 31, then narrower Area-scoped defaults, then 31 named waters. |
| **F** | Fraser River watershed portions of Region 6 | **No fishing for salmon.** Stated in the banner with no rows under it. |

The banner text on B, C, D and E says it explicitly:

> *Section "A" applies if stream, specific area, time period, quotas or other species
> restrictions are not listed in the following sections.*

That sentence is the whole model. It is captured per section as `falls_back_to_a`.

### The precedence ladder

Every emitted row carries `precedence`. To resolve *(water, species, date)*, take the
**highest-ranked** row whose water, species and date window all match:

```
3  named water        "Babine Lake" / "Kitimat River (including tributaries)"
2  area catch-all     "All streams flowing into tidal water Area 5"      ← section E only
1  section catch-all  "All waters in section B(i) ... unless otherwise stated below"
0  region default     section A, "All Region 6 waters"
```

Section E is the only place all four ranks are live at once, and it is the one that
punishes a flat read: it stacks a section-wide coho closure, then a chinook closure for
*Areas 3, 4, 5 and 6*, then a tighter one for *Area 5* alone, then *Area 6*, then 31
named rivers. Collapse those and you will hand an angler an opening that does not exist.
`areas` on each row holds the tidal Area numbers a scope names.

Regions 1–5, 7 and 8 publish no sections, so every row there is rank 3 and the cascade
is a no-op — the same resolver works for all eight regions.

### Three more things that bite

* **Banner-only rules.** Section F closes the entire Fraser portion of Region 6 in its
  header and publishes **zero rows**. Anything reading only `rows` loses a
  whole-watershed closure. It is synthesised as a row with `source: "section_banner"`,
  distinguishable from the 235 real ones. This repo has already been burned by rules
  that live in prose and are invisible to the resolver — this is that failure mode.
* **Exclusion lists are `<ul>`, not sentences.** Babine Lake's sockeye opening excludes
  a 400 m radius around 12 named creeks, each its own `<li>`. Flattened into prose they
  are unresolvable against a reach, so they are kept as `specific_area_bullets`.
* **In-season amendments.** 17 Region 6 rows cite a Fishery Notice (`FN0679`,
  `FN0919`, …) linking to `notices.dfo-mpo.gc.ca`. Those rows are **live variation
  orders that supersede the printed season** and change mid-season without the page's
  `dateModified` necessarily moving in step. They are kept in `fishery_notices` and
  should be treated as the volatile part of the dataset.
* **`colspan="6"` on a five-column table.** The B(i) banner is authored wrong. The grid
  builder clamps spans to the real width; trusting the attribute shifts every later
  column by one.

---

## The untangled view (`untangle.py`)

`parse.py` is faithful; that makes it unreadable. Region 6's 236 rows are one river
repeated across eight reaches and four scope widths. `untangle.py` regroups them:

```
defaults                 what applies where nothing more specific matches
  [A]     region         the Region 6 baseline
  [B(i)]  section        "All waters in section B(i) ... unless otherwise stated"
  [E]     area           "All streams flowing into tidal water Area 5"   → falls back E → A
  [F]     closure        banner-only: the Fraser portion, closed outright

waters                   one entry per named waterbody, per section
  name, aliases[], name_note, tributaries
  inherits: B(i) → B → A
  reaches[]              the distinct spatial scopes on that water, in source order
    scope, kind, anchors[], excludes[], mainstem_only, tributaries
    rules[]              species × dates × limits binding on that reach
```

Read it as text with `--print`; `--out` writes both `.json` and a `.txt` render:

```
Bulkley River
    inherits: B(i) -> B -> A
    · reach 1 [downstream_of] downstream of the Morice River confluence excluding tributaries.
        anchor downstream_of: the Morice River confluence
        Chinook     Apr 1 to Mar 31    No fishing for chinook
        Pink        Jun 16 to Oct 15   2 per day
        Coho        Jul 15 to Oct 15   4 per day, only 2 over 50 cm.
    · reach 3 [sign_bounded_zone] Bulkley and Morice River waters within the four white
                                   triangular boundary signs at "the Forks" ...
        All         Jun 16 to Aug 15   No fishing for salmon
```

### Reach kinds

`kind` says what the `specific_area` sentence is *doing*, which decides how it becomes
geometry later:

| kind | meaning |
|---|---|
| `whole_water` | no scope given — the rule binds to the whole waterbody |
| `tributaries_only` | "including / excluding tributaries" and nothing else |
| `whole_water_excluding` | whole water minus listed tributaries or creek mouths (Babine Lake) |
| `upstream_of` / `downstream_of` | a single directional cut at one anchor |
| `between` | two anchors, "from X to Y" |
| `sign_bounded_zone` | a closure polygon inside boundary signs, usually at a confluence |
| `tributary_set` | "all tributaries of the Bulkley other than Morice, Suskwa and Two Mile" |
| `named_tributaries` | the scope names tributaries directly (Tatshenshini's Blanchard Creek) |
| `described` | anything else — read the sentence |

`anchors` keep the boundary phrase **verbatim** (`{"relation": "upstream_of", "phrase":
"Highway #16 bridge"}`). Turning those into geometry is `pipeline/splits/anchors.py`'s
job; a half-normalised anchor is worse than the sentence it came from.

### Name untangling

One Waters cell can glue four things together, and all four had to be separated before
grouping worked:

| source | name | alias | note | scope |
|---|---|---|---|---|
| `Zymoetz (Copper) River — Note: The section of river from Hwy 16 bridge…` | Zymoetz River | Copper River | ✔ | — |
| `Zymagotitz River [also known as Zymachord River] (including tributaries)` | Zymagotitz River | Zymachord River | — | +tribs |
| `Kitsumkalum River (including tpinributaries) Note: The mouth is…` *(sic)* | Kitsumkalum River | — | ✔ | +tribs |
| `Tatshenshini River (upstream of the BC/Yukon border)` | Tatshenshini River | — | — | **a reach** |
| `Tseax R.` | Tseax River | — | — | — |

Grouping is on the **cleaned** name, so `Morice River` and `Morice River (including
tributaries)` become one river with all 8 of its reaches, and the two Tatshenshini
entries become one river with an upstream and a downstream reach. The parenthetical in
`Tatshenshini River (upstream of…)` is a scope, not an alias, and moves down onto the
reach — getting that wrong splits one river into two.

### What is deliberately *not* merged

The **Skeena River appears twice**, once in B(i) and once in B(ii), and stays twice.
The section boundary — the CNR Railway Bridge at Terrace — is a real cut in the river
with different rules on each side. Waters are keyed on `(section, name)` for exactly
this reason.

---

## Output shape

`output/dfo_salmon/regionN.json`:

```jsonc
{
  "region": 6, "region_name": "Skeena",
  "date_modified": "2026-08-28", "table_found": true,
  "preamble": [ "...size limits, aggregate limits, closures stated above the table..." ],
  "sections": [ { "key": "B(i)", "letter": "B", "part": "i",
                  "title": "...", "falls_back_to_a": false } ],
  "cross_references": [ { "from": "Dewdney Slough", "to": "Nicomen Slough" } ],
  "notes": [ { "section": "D", "text": "..." } ],
  "rows": [ {
    "section_key": "B(i)", "precedence": 3,
    "waters": "Babine Lake", "waters_bullets": [],
    "specific_area": "Babine Lake excluding tributaries and those waters within a 400 m radius of...",
    "specific_area_bullets": ["Morrison Creek", "Six Mile Creek", "..."],
    "species": "Sockeye", "dates": "Aug 1 to Aug 27", "limits_gear": "2 per day — FN0679",
    "areas": [], "fishery_notices": [ { "text": "FN0679", "href": "https://notices.dfo-mpo.gc.ca/..." } ],
    "source": "table",
    "no_fishing": false, "non_retention": false, "hatchery_marked_only": false,
    "bait_ban": false, "single_barbless_hook": false, "daily_limit": 2
  } ]
}
```

The five source columns are always kept **verbatim**. The booleans and `daily_limit`
are best-effort conveniences derived from `limits_gear`; never treat them as the rule.

---

## Not done yet — resolving to reaches

Nothing here touches the reach graph. Before it can:

* **`preamble` is unstructured.** It holds real binding rules — Region 2's per-watershed
  aggregate limits, the "adult chinook is over 62 cm on these four rivers" carve-outs,
  the annual chinook cap. Those bind alongside the table and are currently prose.
* **Waters names are not matched to anything.** They need the same treatment as the
  Synopsis names (`pipeline/name_variants.json`, the matcher), and Region 6 uses forms
  the Synopsis does not — "Zymoetz (Copper) River", "Suskwa (Bear) River",
  "Queen Charlotte Islands" for Haida Gwaii.
* **`specific_area` is a boundary description, not geometry** — bridges, fishing
  boundary signs, lat/lon pairs, "100 m below the falls". This is exactly what
  `pipeline/splits` already does for the Synopsis; the same anchors machinery applies.
* **Section scopes need to resolve to watersheds**, not names: B(i)/B(ii) split the
  Skeena at one bridge, F is "the Fraser watershed within Region 6", E is "everything
  else on the mainland". Those are graph queries, not string matches.
* **Region boundaries themselves** must come from the provincial region polygons, since
  DFO reuses the provincial region numbering.
