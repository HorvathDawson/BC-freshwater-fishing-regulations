# Design — data types, matching, and the curation workflow

**Plan only. Nothing in §2–§5 is built yet**; §1 and §6 describe changes to existing
code. Supersedes the matching described in CURATION.md.

---

## 0. The one idea

Two things move at completely different speeds, and they measured out like this:

| | changes | who authors it | where it lives |
|---|---|---|---|
| **where a rule applies** | ~never (Region 6: 0 of 76 waters in 9 years) | a human, once | the **bundle** |
| **what the rule says** | ~50% per year | nobody — re-scraped | a **feed** |

So they become two artifacts with two lifetimes, joined by one stable key. Everything
below follows from that.

---

## 1. Dates — one canonical form, no fuzzy matching

`interpret_dates` currently repairs a typo by asking `difflib` for the nearest month.
That is the same class of guess as the name ladder and goes the same way.

**Replace with an explicit variants table, then normalise to one shape.** Both steps are
auditable; neither can invent a month.

```
"Aprl 1 to Jun 15"
  1. lowercase, strip
  2. month variants table — explicit, hand-authored, NEVER fuzzy
       aprl→apr · jaunary→jan · sept→sep · febuary→feb · futher→further
  3. one separator:  "to" | "through" | "–" | "—"  →  "-"
  4. collapse whitespace · collapse repeated dashes · drop a trailing calendar year
  5. canonical:  "apr 1 - jun 15"
  6. parse with pipeline.parsing.dates
```

Anything the table does not cover stays **unparsed and reported** — a new misspelling
adds a row, it is never guessed.

```python
MONTH_VARIANTS: dict[str, str]        # "aprl" -> "apr". One line per sighting.
OPEN_ENDED = re.compile(r"until\s+fu\w*\s+notice")

@dataclass(frozen=True)
class DateWindow:
    verbatim:      str          # exactly as published — always kept
    canonical:     str | None   # "apr 1 - jun 15"
    start:         str | None
    end:           str | None   # None when open-ended
    open_ended:    bool
    normalised_by: list[str]    # ["month variant aprl→apr", "separator to→-"]
```

`normalised_by` replaces `date_repaired_from`: it names *which rules fired*, so a
surprising window traces back to the rule that produced it.

Measured on the live corpus: 427/438 parse untouched, 11 need steps 2–4, **0 need a
guess**.

## 2. Matching — exactly like the provincial regs

**Delete the ladder. Delete the suggestions.** A DFO name matches the way a synopsis row
matches and no other way:

```
name_verbatim ──► pipeline/matching/overrides.json      (THE SHARED FILE — see §3C)
                  ├─ gnis_ids / waterbody_keys / …  → explicit, possibly several
                  ├─ variant_of                     → the current gazetted name
                  └─ skip                           → do not match this name
                          │
                          ▼
                  registry name/variant index  (EXACT only)
                          │
                          ▼
              matched · ambiguous · unmatched      ← there is no fourth outcome
```

`pipeline.matching.matcher.match_row` implements every step. The DFO side calls it and
adds nothing.

### Measured, with the shared overrides file loaded

161 waters, against `pipeline/matching/overrides.json` (482 rows):

| outcome | n | % |
|---|--:|--:|
| exact registry name/variant | 117 | 73% |
| **override, explicit ids** | **19** | **12%** |
| ambiguous — several items | 4 | 2% |
| unmatched — needs a curator | 21 | 13% |

**136 bound (84%)**, of which 19 come free from work already done for the synopsis —
Skeena, Kitimat, Kitsumkalum, Zymoetz, Suskwa, Bear, Fraser, Cowichan, Harrison,
Thompson. Several carry notes worth having: *"FWA has MU 6-11, regulation has MU 6-10
(boundary issue)"*.

### `skip` does not belong in an override — DFO ignores it

A `skip` means "this synopsis ROW is a pointer to another row that carries the real
regulations" (*"VEDDER RIVER: See Chilliwack River"*). That is a fact about how the
synopsis is laid out, not about which blue line a name means. DFO's table is laid out
differently, so honouring it only suppresses good matches — measured, all three skips
DFO hit are names the registry already resolves on its own:

| DFO name | resolves to | how |
|---|---|---|
| `Ishkheenickh River` | `gnis:4069` Ksi Hlginx | the registry carries the old name as a variant |
| `Tseax River` | `gnis:3828` Ksi Sii Aks | same |
| `Little Campbell River` | `gnis:7250` (MU 2-4) | a variant of the Region 2 Campbell River; the region gate keeps it off the Island one |

**An old name and its current gazetted name are the same water, so an override should
link them, never refuse them.** `dfo_salmon.match.drop_skips()` filters skips out before
the match. The legitimate negative outcome — *"we looked and there is no registry item
for this name"* — is recorded in the **entry file** as
`WaterBinding.resolution = "not_found"` with a reason, because that is a fact about
this table, not about the name.

> **A second bug this found:** `Proposal.bindable` required `len(item_ids) == 1`, which
> silently refused every curated multi-water override — Fraser River in Region 2 names
> **13** items, Nicomen Slough names 7. "Ambiguous" means the matcher could not choose;
> a curated list is the opposite of that. Now `bindable` needs a non-empty list.

### Bind to the typed id, not the item id

`RegistryItem.id` is already `gnis:…` / `wbk:…` / `wsc:…` / `blk:…`, and `ref_ids`
bridges a curated id to whatever item owns it after a rebuild. So:

* **the override file is the source of truth** — it stores `gnis_ids`, which survive a
  registry rebuild;
* **`WaterBinding.item_ids` is a resolved cache** — re-derived by `match apply` on every
  rebuild, never hand-edited.

## 3. One key, three files

### The key

Every row of the DFO table gets a **fingerprint** — a hash of the row's own location
text. That is the only identifier that crosses a file boundary.

```python
def fingerprint(region: str, key: str, scope_text: str) -> str:
    """sha256(...)[:16] of one normalised join. Nothing else goes in."""
    return sha256(f"{region}|{normalize(key)}|{normalize(scope_text)}".encode()).hexdigest()[:16]

#  named water row   key = the Waters cell        scope_text = the Specific area cell
#  cascade row       key = the scope id           scope_text = the banner text
#                          "A" · "B(i)" · "B(ii)" · "E" · "E:areas-5" · "F"
```

`normalize()` collapses only drift that has actually been observed — lowercase, strip
punctuation, collapse whitespace, and a fixed synonym list (`Hwy`→`Highway`, `#16`→`16`,
`metres`→`m`, `Ck.`→`Creek`, `&`→`and`). **Numbers are never normalised**: "three signs"
versus "4 signs" is a real difference in what the source claims and must reach a human.

**The section is deliberately NOT in a named water's fingerprint.** Measured: across 235
live rows and 321 rows in the archive, no (water, specific area) pair ever appears under
two sections — so including the section buys nothing and costs everything, because the
one structural change in nine years (B splitting into B(i)/B(ii)) would have broken
every Skeena fingerprint at once. For a *cascade* row the section key **is** the
identity, so those keep it. That is what makes E, B(i) and B(ii) work like any other
row.

### The three files

```
pipeline/dfo_salmon/entries/region-6.json     CURATED · committed · rarely changes
   location  →  fingerprints[]  +  extents  +  tributaries  +  notes
                        │
                        │  build_reach()
                        ▼
output/dfo_salmon/link/region-6.json          BUNDLE
   fingerprint  →  section_ids[]
                        ▲
                        │  joined on the fingerprint, by the app
                        │
output/dfo_salmon/feed/region-6.json          FEED · re-scraped, never curated
   fingerprint  →  species, dates, limits, gear, fishery notices
```

The feed carries **no match information at all** — no `location_id`, no `matched_by`, no
status. It is the scrape, keyed by the row it came from. The link is the curated half,
keyed the same way. `location_id` stays inside the entries file as a readable handle for
curation and commits, and never leaves it.

### What happens when the website changes

| what changed | what breaks |
|---|---|
| a limit, a date, a gear rule | **nothing** — the feed carries the new value under the same fingerprint |
| a row is reworded | **that one row**, until its new fingerprint is added to the location's `fingerprints[]` — a one-line edit |
| a row is added (a new boundary) | **that one row** — it needs curating, like any new reach |
| a row disappears | nothing — the location stays, with no rules under it this season |

Everything else on the page keeps working throughout. A row that does not resolve is
listed, not dropped:

```jsonc
// output/dfo_salmon/feed/region-6.json
{ "region": "6", "scraped_at": "…", "source_sha256": "…",
  "rules":     [ { "fingerprint": "d0ece960…", "species": "Sockeye",
                   "dates": {…}, "limits_gear": "2 per day", … } ],
  "unmatched": [ { "fingerprint": "7c31…", "waters": "Kispiox River",
                   "specific_area": "downstream of the new weir",
                   "species": "Coho", "dates": "…", "limits_gear": "…" } ] }
```

`unmatched` non-empty is the front-end notice: *"1 rule on the Kispiox could not be
placed — check the DFO page."* Everything else still serves.

### How the feed is built — the whole of it

```python
def build_feed(slug: str) -> FeedFile:
    page   = fetch(slug)                       # or the cached snapshot
    known  = {fp for ef_loc in load(slug).locations for fp in ef_loc.fingerprints}
    rules, unmatched = [], []
    for row in untangle(parse(page)):          # every table row, named + cascade
        fp = fingerprint(slug, row.key, row.scope_text)
        (rules if fp in known else unmatched).append(row.as_feed_rule(fp))
    return FeedFile(region=slug, scraped_at=now(), source_sha256=sha256(page),
                    rules=rules, unmatched=unmatched)
```

That is the entire scheduled job: **a set membership test per row.** No registry, no
graph, no matcher, no network beyond the fetch. Nine pages, ~440 rows, milliseconds.

A row is in `rules` or in `unmatched`. There is no third outcome and nothing is dropped.

### The archive already absorbs most rewordings

The entries file was seeded from the **superset of every archived version**, so a
location already carries every wording DFO has published — 354 locations against 247 on
the live page today. A reword to a form used in a previous season resolves immediately;
only a genuinely new wording needs the one-line edit.

### Reading a rule, for (section_id, date, species)

1. every feed rule whose fingerprint maps to a section containing `section_id`;
2. drop rules whose date window excludes the date, or whose species does not match;
3. highest precedence wins — **named water › area catch-all › section catch-all ›
   region default**;
4. attach `fishery_notices` (live variation orders, the most perishable part);
5. if the location has `spatial_caveat`, show its `notes[]` and do not draw it as a
   plain fill;
6. tag `authority: "dfo"` — a section can carry a provincial and a federal rule at once.

---

## 3A. The data types

Three files. Types below are the actual shapes, and every one reuses an existing model
where the repo already has one.

### `pipeline/dfo_salmon/entries/region-<slug>.json` — CURATED, committed

```python
@dataclass
class EntryFile:
    region: str                 # "6", "5a" …
    region_number: int          # 5 for both 5a and 5b
    region_name: str
    scopes:    list[Scope]          # the cascade tree; [] outside Region 6
    waters:    list[WaterBinding]   # name -> registry item, once per waterbody
    locations: list[EntryLocation]  # reach -> extent, many per water
    structure: dict                 # last accepted page shape (structural-change guard)

@dataclass
class WaterBinding:
    water_id:      str            # "6:babine-lake"  — readable, never leaves this file
    name:          str            # verbatim, as DFO publishes it
    region: str; region_number: int
    aliases:       list[str]
    sections:      list[str]      # which cascade scopes it appears under
    item_ids:      list[str]      # RESOLVED CACHE of the name/override hit — gnis:… / wbk:…
    tributaries:   bool | None    # when the NAME says so ("… and tributaries")
    match:         dict           # {status, via, reason, candidates} — informational
    locked:        bool; reviewed_by: str; reviewed_at: str; note: str

@dataclass
class EntryLocation:
    location_id:  str             # "6:babine-lake:including-tributaries"
    water_id:     str | None      # None for a cascade row
    fingerprints: list[str]       # EVERY wording ever confirmed → THE LINK KEY
    section:      str | None      # observed attribute, versioned; not identity
    kind:         str             # water | section_default | area_default | region_default | closure
    precedence:   int             # 3 named water · 2 area · 1 section · 0 region
    source_text:  dict            # verbatim waters/specific_area/op/anchor_types
    extents:      list[Extent]         # ← pipeline.parsing.entry_models.Extent
    tributaries:  Tributaries          # ← pipeline.parsing.entry_models.Tributaries
    notes:        list[str]            # un-modelled spatial qualifiers
    spatial_caveat: bool               # notes NARROW the extent → app must show them
    status:       str             # active | dormant
    duplicate_of: str | None; duplicate_confirmed: bool | None
    locked:       bool; reviewed_by: str; reviewed_at: str
```

**`Extent` and `Tributaries` are the provincial models, imported — not re-declared.**
That is what `build_reach` consumes, so there is one definition of "upstream of split s"
in the repo and the DFO side cannot drift from it.

### `output/dfo_salmon/link/region-<slug>.json` — BUNDLE, generated

```python
@dataclass
class LinkFile:
    region: str
    bundle: str                 # registry/bundle version the sections came from
    built_at: str
    links: list[Link]
    scopes: list[ScopeLink]

@dataclass
class Link:
    fingerprints: list[str]     # every wording that resolves here
    section_ids:  list[str]     # what build_reach resolved
    precedence:   int
    scope_id:     str | None    # which cascade scope this sits in
    spatial_caveat: bool
    notes:        list[str]
```

`section_ids` is valid **only for the named `bundle`** — never reuse a resolved section
list across a rebuild (AGENTS rule 6).

### `output/dfo_salmon/feed/region-<slug>.json` — FEED, generated

```python
@dataclass
class FeedFile:
    region: str
    scraped_at: str
    source_sha256: str          # the page hash this came from
    date_modified: str | None   # the page's own stamp
    rules:     list[FeedRule]
    unmatched: list[UnmatchedRow]

@dataclass
class FeedRule:
    fingerprint: str            # THE ONLY KEY. No location_id, no match info.
    species:     str
    dates:       DateWindow     # verbatim + canonical + start/end/open_ended
    limits_gear: str            # verbatim — this is what gets displayed
    daily_limit: int | None
    no_fishing: bool; non_retention: bool; hatchery_marked_only: bool
    bait_ban: bool; single_barbless_hook: bool
    fishery_notices: list[dict]   # [{text: "FN0679", href: "…"}]

@dataclass
class UnmatchedRow:
    fingerprint: str
    waters: str; specific_area: str; section: str | None
    species: str; dates: str; limits_gear: str
```

---

## 3B. How each file is produced

```
                    pac.dfo-mpo.gc.ca
                           │
        fetch ─────────────┴──────────────► cache/dfo_salmon/raw/*.html
                                                     │
        parse → untangle → locations ────────────────┤
                                                     │
                 ┌───────────────────────────────────┴──────────────────────┐
                 │                                                          │
    (a) entries seed --history                              (c) feed build
        every location ever published,                          rules keyed by
        unbound. Run once, then on demand.                      fingerprint; any
                 │                                              fingerprint not in
    (b) match apply                                             the entries file
        name/override → waters[].item_ids                       → unmatched[]
        (exact hits only)                                            │
                 │                                                   ▼
    (d) CURATION — the human pass (§4)                     output/…/feed/region-N.json
        extents, new splits, overrides                             FEED
                 │
                 ▼
    pipeline/dfo_salmon/entries/region-N.json      ← the only hand-edited file
                 │
    (e) link build   (needs registry + graph)
        build_reach(entry, rule) per location
                 │
                 ▼
    output/dfo_salmon/link/region-N.json           BUNDLE
```

| step | command | when | needs |
|---|---|---|---|
| fetch | `dfo_salmon.fetch` | scheduled | network |
| a. seed | `dfo_salmon.entries seed --history` | on demand | cached pages |
| b. match | `dfo_salmon.match apply` | after a registry rebuild | registry + overrides |
| c. feed | `dfo_salmon.feed build` | **every scheduled run** | entries (for fingerprints) |
| d. curate | the §4 workflow | until done | you |
| e. link | `dfo_salmon.link build` | after a bundle rebuild | registry + graph (9.6 GB) |

Only **(c)** runs on the schedule. It needs no registry, no graph, no network beyond the
fetch — it is a dict lookup per row.

---

## 3C. Overrides live in `pipeline/matching/overrides.json` — the existing one

**Do not add a DFO overrides file.** The shared file already answers DFO's questions,
and this was measured, not assumed:

* 25 of DFO's 159 water names already have an override row;
* six of the seven waters DFO could not disambiguate were **already answered** there —
  `Bear River` in Region 6 → `gnis:15535`, plus Lakelse, Yakoun, Pallant, Hope Slough,
  Long Lake;
* two more carry current gazetted names DFO's page has not caught up with —
  `Ishkheenickh River` → *KSI HLGINX RIVER*, `Tseax River` → *KSI SII AKS RIVER*.

An override states a fact about a **name in a region**, not about which authority
published it. A bridge is a bridge; the Nass is the Nass. Splitting the file would mean
curating the same fact twice and letting the two copies drift.

DFO-authored rows go in the same file with a `note` recording the pass. If a genuine
conflict ever appears — a name DFO means differently from the synopsis — add a
`sources: ["dfo"]` discriminator then, not now.

> **A bug this found:** `dfo_salmon.match` was calling
> `load_overrides("__default__")`. That sentinel is resolved by
> `reach.covered.make_matcher`, not by `load_overrides`, which treated it as a path,
> found no file, and returned `[]`. Every match run so far used **zero overrides**.
> Fixed to `reach.covered.DEFAULT_OVERRIDES`.

---

## 4. The curation workflow — you and me

**Unit of work is one waterbody.** 161 waters, ~2 reaches each. Top-heavy: Skeena 15,
Stamp 14, Morice 9, Somass 8 — four sittings cover 46 reaches.

Per water I hand you a card. I do the parsing; you make the calls I cannot.

```
────────────────────────────────────────────────────────────────────────────
BABINE RIVER            region 6 · section B(i) · water_id 6:babine-river
registry : BOUND  gnis:17687   (exact name)

splits already on this water
  s1  juvenile counting weir
  s2  Nilkitkwa River → Babine River
  s3  fishing boundary signs 100 m upstream of adult fish counting fence
  s4  signs 80 m downstream of adult fish counting fence
  s5  adult counting fence
  s6  Fort Babine Bridge
  s7  Nichyeskwa Creek → Babine River

L1  op=between · needs 2 · anchors: confluence, boundary_sign
    "from the Nilkitkwa River confluence with the Babine River downstream to the
     Skeena River confluence"
    my read: from = s2 ✓ · to = NEW (Skeena confluence)

L2  op=whole_water · needs 0
    "(whole water)"
────────────────────────────────────────────────────────────────────────────
```

You answer one line per location. That is the whole "vocabulary" — the shorthand you
type back so I do not have to ask a paragraph of questions per reach:

```
L1: s2, NEW "Babine River → Skeena River" confluence
L2: ok
```

| you write | means |
|---|---|
| `s2` | use the existing split labelled s2 |
| `NEW "<label>" <kind>` | author a new split; kind = `confluence` \| `point` \| `lake_outlet` |
| `ok` | nothing to cut — the rule covers the whole water |
| `skip <why>` | leave it unbound and record why |
| `same as L1` | a duplicate wording — I fold it, and it can never be auto-regrouped |
| `item = gnis:12345` | the registry binding is wrong or missing; use this |
| `yes` / `no` | answering a guess I offered (see below) |

I then write the `extents` into the entry file, any new splits **straight into
`pipeline/splits.json`** under that waterbody's block, and an override if you corrected
a name. One commit per water, so each is reviewable and revertible on its own.

### The same pass builds the overrides

When a name has no registry match I put my guess on the card and you rule on it. That
is how the override file gets written — no separate session for it:

```
────────────────────────────────────────────────────────────────────────────
CAYEGHLE RIVER          region 1 · water_id 1:cayeghle-river
registry : **UNBOUND** — no exact name or variant match
  my guess: "Cayeghle Creek"  gnis:30524   (DFO says River, the FWA gazetteer says Creek)
  → right?

L1  op=described · needs 0
    "including Colonial River."
    note: names a second water — if that is in scope this needs two items
────────────────────────────────────────────────────────────────────────────
```

```
guess: yes
item = gnis:30524, gnis:30525      # Cayeghle + Colonial, both in scope
L1: ok
```

and I write into `pipeline/dfo_salmon/overrides.json`:

```jsonc
{ "type": "override",
  "criteria": { "name_verbatim": "CAYEGHLE RIVER", "region": "REGION 1 - Vancouver Island" },
  "note": "DFO writes 'River'; FWA gazetteer has Cayeghle Creek. Scope names Colonial River too.",
  "gnis_ids": ["30524", "30525"] }
```

Same schema the provincial matcher already reads, so it needs no new code — and because
it keys on `gnis_ids`, it survives a registry rebuild.

### Adding an override — the exact steps

Overrides are written **during** the curation pass, never in a separate session. Three
cases, and the card tells you which one you are in.

**1 — the name is simply different.** DFO says River, the gazetteer says Creek.

```
CAYEGHLE RIVER          region 1 · UNBOUND
  my guess: "Cayeghle Creek"  gnis:30524   → right?
```
`guess: yes` →
```jsonc
{ "type": "override",
  "criteria": { "name_verbatim": "CAYEGHLE RIVER", "region": "REGION 1 - Vancouver Island" },
  "note": "DFO writes 'River'; FWA gazetteer has Cayeghle Creek. [dfo pass 2026-08]",
  "gnis_ids": ["30524"] }
```

**2 — one DFO name is several waters.** The reason `item_ids` is a list.

```
CHILLIWACK/VEDDER RIVER (INCLUDING SUMAS RIVER)     region 2 · UNBOUND
  my guess: Chilliwack gnis:8634 + Vedder + Sumas   → which ids?
```
`item = gnis:8634, gnis:12345, gnis:67890` →
```jsonc
{ "criteria": { "name_verbatim": "CHILLIWACK/VEDDER RIVER (INCLUDING SUMAS RIVER)",
                "region": "REGION 2 - Lower Mainland" },
  "note": "One DFO row covering three waters. [dfo pass 2026-08]",
  "gnis_ids": ["8634", "12345", "67890"] }
```

**3 — the name is right but ambiguous.** Several registry items share it.

```
YAKOUN RIVER            region 6 · AMBIGUOUS
  candidate: Yakoun River  gnis:3485
  candidate: Yakoun Lake   wbk:329163190
```
`item = gnis:3485` → an override with that single id, and a note saying why the lake was
rejected. **This is the case where a heuristic silently picked the lake.**

Then:

```bash
.venv/bin/python -m pipeline.dfo_salmon.match apply     # re-resolves item_ids from the override
.venv/bin/python -m pipeline.dfo_salmon.match report    # the water should now be BOUND
```

`match apply` never overwrites a `locked` water, so a hand-corrected binding survives.
Every override row carries `[dfo pass <date>]` in its note, so the DFO-authored rows are
greppable inside the shared file without a schema change.

### Order

1. **28 overrides first** (§2) — the unbound and ambiguous waters. Pure desk work, no
   geometry, and it unblocks their reaches.
2. **The 4 heaviest waters** — Skeena, Stamp, Morice, Somass. 46 reaches.
3. **96 single-reach waters** with a clean binding — should go fast.
4. **The 13 cascade scopes** — `B(i)`/`B(ii)` are one split (the CNR Railway Bridge)
   used twice.
5. **The 3 tidal-area scopes** — needs the PFMA layer re-fetched first.

---

## 5. Decisions taken

| question | decision |
|---|---|
| where do new splits go? | **straight into `pipeline/splits.json`**, under the existing waterbody blocks. The anchors are shared between the two authorities; a bridge is a bridge. |
| entries file format | its **own** format (§3A) — it carries scopes, waters and locations, which `splits.json` has no place for. |
| how does a scrape link to curated work? | **one key — the fingerprint of the table row.** No second id, no resolution step. |
| what does the feed carry? | the fingerprint and the rule. **No match information.** |
| is the section in the fingerprint? | for a **cascade** row yes (it is the identity); for a **named water** no — measured 0 collisions in 556 rows, and excluding it survives a B → B(i)/B(ii) restructure. |
| what happens to a row that will not link? | it is listed in `unmatched[]`. That single row is stale; every other row still serves. |
| when do overrides get written? | **during the same curation pass** — I guess, you rule, I write. |

---

## 6. Code to remove when this lands

| what | why |
|---|---|
| `match.name_candidates` | the ladder — replaced by overrides |
| `match._spelling_suggestions` | fuzzy — replaced by overrides |
| `Proposal.suggestions` | nothing to suggest; a name matches or it does not |
| `locations._repair_dates` difflib call | replaced by the month-variants table (§1) |
| `dfo_salmon.entries.Binding` | replaced by the provincial `Extent` + `Tributaries` |
| a DFO overrides file | never created — the shared one already answers it (§3C) |

### What DFO reuses rather than reimplements

The duplication is worth naming, because the answer to "is DFO its own code path?" is
mostly **no**:

| concern | who owns it |
|---|---|
| name → registry item | `pipeline.matching.matcher` — unchanged, called directly |
| curator overrides | `pipeline/matching/overrides.json` — the same file |
| extent ops (`whole`/`upstream_of`/`between`/`within`) | `pipeline.parsing.entry_models.Extent` |
| tributary scope + carve-outs | `pipeline.parsing.entry_models.Tributaries` |
| extent → sections, tributary walk | `pipeline.reach.build.build_reach` |
| split anchors | `pipeline/splits.json` — the same file |
| date windows | `pipeline.parsing.dates` |
| species codes | `pipeline.parsing.species` |

What is genuinely DFO-only, and has to be: fetching and parsing *their* HTML
(`fetch`/`parse`/`untangle`), the lettered cascade (`cascade`), and the fingerprint that
links a scrape to curated work (`locations`). Everything downstream of "this reach is
this extent on this item" is the machinery that already exists.

Net: the matching path is *name → overrides → registry*, the same three steps the
provincial regs take, with no DFO-specific cleverness in it at all.
