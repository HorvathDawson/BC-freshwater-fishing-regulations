# 18 — The bundle builder

**Status: plan, not built.** The next build stage, after `16` (delivery) and `17` (regulation model).

V1 called this the *reach builder*. In v2 there are no reaches left to build — `build.py` already
produces the sections and the registry binds names to them. The real job is narrower and harder than
it sounds:

> **Decide what applies to any water a user can touch — named or not — and emit it so a web client
> range-reads it and a mobile client carries it offline.**

Everything below is driven by four measurements, taken against the built graph. They are not
estimates, and three of them killed a design I had already written down.

---

## 1. The four numbers that constrain the design

```
graph nodes                          2,030,222
registry sections                       49,639     ← 2.4% of the network
```

**① 97.6% of BC's stream network is not in the registry.** A tap on an unnamed creek is the normal
case, not the edge case. Any design that indexes "waters we have names for" answers 2.4% of taps.

```
Fraser River      own 37 sections  →  tributary expansion 419,164  (20.6% of BC)
Kootenay River    own 47           →                       63,978  ( 3.2%)
Iskut River       own 240          →                       28,328  ( 1.4%)
Chemainus River   own 18           →                          361  ( 0.0%)
```

**② `includes_tributaries` expands enormously.** 111 rules carry it, across 83 items including the
Fraser, Kootenay, Peace and Iskut. Materialising these as `rule_section` rows means one Fraser rule
produces 419,164 rows. Enumeration is off the table.

```
WSC prefix vs flow walk:   BFS-only = 0 in every case  (prefix never misses)
                           prefix-only = 955 / 2,563 / 5  (prefix over-includes)
```

**③ The obvious cheap fix — FWA watershed-code prefix — is wrong in an instructive way.** It never
*misses* reachable water, but the Kootenay's 2,563 extra sections include **Moyie River, Yahk River,
3,205 km** of named water. Those are in the Kootenay's watershed code and join the Kootenay *below*
the regulated reach.

> **Tributary scope is relative to the rule's EXTENT, not to the named river.** "Kootenay River
> upstream of X, includes tributaries" does not include tributaries entering below X. A watershed-code
> test cannot express that, because it does not know where on the river the rule applies.

`reuse.py` already gets this right (reach-scoped tributary reachability). A builder that reimplemented
it with a prefix test would silently over-apply regulations to 3,205 km of the Kootenay system.

```
sqlite of registry + entries + FTS5:  9.6 MB   ·   2.8 MB gzipped   ·   2,462 pages
```

**④ The regulation data itself is small.** Geometry and tributary scope are the only things with scale
problems.

---

## 2. How each kind of "what applies here" is represented

The unifying principle: **test, don't enumerate** — except where the set is small, where enumeration
is simplest and safest.

| what applies | how it is answered | size |
|---|---|---|
| Rules on a **named water** | `rule_section` rows | 2,525 `whole` + 415 bounded extents → small |
| **Tributary inheritance** | per-rule **bitmap** over dense section ids | 111 rules; Fraser's 419k ids as a roaring bitmap is ~100s of KB |
| **Area closures** (parks, eco reserves) | **point-in-polygon on the client** against a small polygon set | a few hundred polygons |
| **Regional / MU defaults** | point-in-polygon against ~225 MU polygons | tiny |
| **Unnamed water with nothing else** | falls through to MU defaults | zero rows |

### 2a. Tributary scope — bitmaps, computed by the resolver

Per rule, the pipeline walks the flow graph **from that rule's resolved sections** (not from the
river) and stores the result as a compact bitmap over dense integer section ids. Roaring bitmaps are
the standard tool (Lucene, Druid); membership is O(1) and the encoding exploits exactly the clustering
these sets have.

This keeps the *exact* semantics — reach-scoped, barrier-aware, EXCEPT-aware — instead of
approximating them with a code prefix. It also means `tributaries_only` (4 rules: applies to the
tributaries but **not** the mainstem) is just a bitmap that omits the item's own sections, rather than
a special case in client logic.

The dense-id map is itself an artifact: `section_id → int`, needed by both clients, and **it must be
stable across versions** (see §6).

### 2b. Areas — the client tests geometry, the pipeline does not enumerate

`in_areas` already exists on graph nodes (midpoint-in-polygon, `splits/border.py`) — but it lives on
the **graph**, not the registry, so nothing downstream can see it today. Two options:

- **Export `in_areas` per section** — only meaningful for the 2.4% in the registry, and useless for
  the unnamed creek inside an ecological reserve, which is the case that matters.
- **Ship the polygons and test on the client** — works for every water, named or not, and is the only
  one that answers ② and ①.

**Ship the polygons.** They are few and small. Note `base_regulations.json` uses `buffer_m: 500` on the
provincial park/reserve rules, so the test is *buffered* containment — pre-buffer the polygons at
build time rather than asking the client to buffer.

### 2c. Region and MU for unnamed water

Same shape: ~225 MU polygons, tested client-side. This also handles the case where **one water spans
several regions** — the Fraser crosses four, and its defaults differ along its length. A per-item
region field would be wrong; a point test is right by construction.

---

## 3. The artifact set

```
core.sqlite       items · sections · boundaries · species · base_rule ·
                  section_index (dense id map) · search (FTS5)
region-N.sqlite   entries · rules · rule_section · rule_date · rule_species · rule_exemption
scopes.bin        per-rule tributary bitmaps
areas.geojson     pre-buffered park / eco-reserve / MU polygons
waters.pmtiles    geometry; feature properties carry section id + dense id
manifest.json     the only mutable object: versions, per-shard sha256, sizes
```

**Format: single-file, internally-indexed, range-readable archives.** SQLite via a page-aligned VFS
(`sql.js-httpvfs`, `wa-sqlite`) on web and `expo-sqlite` on device; PMTiles for geometry, already used
here. Web transfers ~100–200 KB to answer a tap; mobile downloads the same bytes once. One artifact
set, two access modes, no second code path.

Alternatives rejected: sharded JSON needs a bespoke layout *and* a client router (that is how
`tier0.json` reached 24.9 MB — no index inside a blob); Parquet/DuckDB-wasm is strong on scans and
weak on point lookups and FTS; a Worker query API puts resolution on a server the mobile app cannot
reach offline, which means two implementations of the most error-prone logic in the system.

---

## 4. Answering a tap — the full path

```
tap → PMTiles feature → { section_id, dense_id, wsc, item_id? }
   │
   ├─ item_id?          → region shard: rules on this water          (2.4% of taps)
   ├─ dense_id          → scopes.bin: which tributary rules contain it
   ├─ lat/lon           → areas: park / eco-reserve closures (buffered)
   ├─ lat/lon           → MU polygon → base_rule for (region, MU, feature_type)
   └─ minus exemptions  → any exempts_from lifted by the rules above   (17 §1)
```

Note the tile carries the identity. **The tile is the index for unnamed water** — the alternative is a
2-million-row lookup table, which is the dominant cost in any offline budget. This is what vector
tiles are for.

---

## 5. Updates

**Mobile** — `16 §3` unchanged: content-addressed shards, region sharding so an in-season change in
Region 3 moves one shard, manifest diff → download → verify sha256 → staging → atomic swap → previous
generation retained for rollback. Push carries a *version*, never data.

**Web, on the fly** — the same manifest, polled and on `visibilitychange`. Because reads are
range-reads against immutable, content-addressed URLs, an update is: fetch the new manifest, swap the
URL the VFS reads from, drop the page cache. No reload, no re-download of anything the user has not
looked at. A user with the map open when a closure lands sees it on their next tap.

The one hard rule: **tiles and data are versioned together.** A section id present in `waters.pmtiles`
but absent from `core.sqlite` — or vice versa — produces a tap that resolves to nothing. The manifest
must pin both, and the client must refuse a mixed pair.

---

## 6. Stable section ids — the quiet blocker

Section ids are `{blk}:{int(down_m)}`. **They change when splits change.** Adding one curated split to
the Chilliwack renumbers its sections; braid simplification this session changed thousands.

That breaks three things at once: delta updates (every shard's hash churns), deep links
(`?s=<section>` from a shared URL), and the dense-id map that the tributary bitmaps are indexed by.

Options, none free:

- **Accept churn**, and treat any split change as a full re-download. Simple, and wastes the delta
  mechanism precisely when regulations change.
- **A stable surrogate key** minted per section and carried forward by matching `(blk, measure range)`
  build over build, with births and deaths recorded. More machinery, but it is what makes deltas and
  deep links survive a rebuild.
- **Key deep links by `(blk, measure)`** rather than section id, and let the client resolve to whatever
  section currently covers that measure. Solves links, not deltas.

**This needs deciding before the schema is written**, because the dense-id map is baked into the
bitmaps.

---

## 7. Indexing parity with the current webapp

What v1's `tier0.json` provided, and where it comes from now:

| v1 | v2 |
|---|---|
| `search_index` loaded at startup | FTS5 `search` in `core.sqlite`, range-read |
| `regulations` map | `rule` + `rule_section` in region shards |
| `reaches` map | sections are the reaches; no separate structure |
| fid → reach shard chain | tile feature properties carry the section id directly |
| `/api/resolve` Worker | client-side, same code as mobile |

Search ranking is the one place range-reads cost more than a point lookup — FTS5 ranking touches more
pages. Worth measuring before assuming web search is free; a prefix-index of the ~19.7k display names
is a small fallback (names alone are well under a megabyte).

---

## 8. Failure modes to design against

Ordered by how quietly they fail.

1. **Silent under-application.** A rule that resolves to fewer sections than it should — the Kootenay
   prefix case. Nobody notices until someone fishes a closed reach. *Guard: resolver parity against
   the review app, asserted per confirmed entry.*
2. **A rule that vanishes.** Present in the review app, absent from the bundle. *Guard: every rule
   lands in `rule_section` or in `rule_unresolved` with a reason; the count is asserted.*
3. **Unknown dates rendered as open.** `17 §3`: 81% of rules have no dates and the 19% are prose.
   *Guard: an explicit `unknown` status that the UI must render distinctly.*
4. **Species filter hiding a closure.** `17 §4`. *Guard: filtering applies to `harvest` only.*
5. **Mixed tile/data versions.** §5. *Guard: manifest pins both; client refuses a mismatch.*
6. **Interrupted update leaving a half-written DB.** *Guard: staging + atomic swap + retained previous
   generation.*
7. **Non-deterministic builds** churning every hash and turning deltas into full downloads. *Guard: a
   byte-identical rebuild test — fixed page size, no timestamps, deterministic ordering, `VACUUM`.*
8. **Device eviction.** iOS/Android can evict app data, and web OPFS is evictable and unavailable in
   some Safari configurations. *Guard: the client must survive a cold cache and re-fetch; never assume
   the bundle is there.*
9. **Unconfirmed data shipping as fact.** Only 108 of 1,392 entries are confirmed. *Guard: a
   confidence flag per entry, and a policy decision (§9) about what unconfirmed data does in the UI.*
10. **Two rules disagreeing on one section.** No precedence model exists today. *Guard: detect and
    surface conflicts at build time rather than letting the client pick.*

---

## 9. Sequencing

1. **Lift `resolve_extent` out of the review app** into `pipeline/`, called by both. It is the
   most-tested logic in the project, and ③ shows what reimplementing it would cost. Refactor only.
2. **Decide section-id stability** (§6) — it is baked into everything after it.
3. `core.sqlite` + region shards from today's data, with determinism and resolver-parity tests.
4. Tributary bitmaps + the dense-id map, validated against the flow walk on the Fraser and Kootenay.
5. Areas and MU polygons, pre-buffered; the client-side containment path.
6. Manifest, verify/swap/rollback, exercised against a fabricated second version.
7. Measure regulable-waters PMTiles — the gate on "fully offline" (`16 §2b`).
8. Base regulations (`17 §1`), then structured dates (`17 §3`).

1–4 can start now; none of them waits on curation.

## Open questions

1. **Section-id stability** (§6) — accept churn, mint surrogates, or key links by `(blk, measure)`?
2. **Does the bundle ship unconfirmed entries?** At 108/1,392 confirmed, shipping only confirmed data
   means shipping almost nothing. Shipping everything means shipping unreviewed LLM output as
   regulation. A confidence flag plus a UI treatment is the likely answer, but it is a product call.
3. **Precedence** when two rules cover one section and disagree.
4. **Do the tiles carry the dense id**, or does the client map `section_id → dense_id` through
   `core`? Tile properties are cheaper at query time and larger on disk.
5. **Region shard by entry region or by geography?** A water regulated from four regional rows (the
   Fraser) lands in four shards under the first, one under the second.
