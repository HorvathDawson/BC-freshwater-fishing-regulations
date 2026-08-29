# 16 — The bundle, and the clients that read it

**Status: plan, not built.** Written 2026-08-28 against the state in `15-current-state.md`.

The goal in one line: **one dataset artifact, produced once, that a web app and an offline mobile app
both read through the same queries.** Not two pipelines that happen to agree.

---

## 1. The single decision everything else follows from

> **The bundle is a SQLite database. Both clients query it locally. There is no server-side
> resolution.**

Mobile already proves the shape — v1's mobile app bundles `regulations.sqlite` and answers taps with
local SQL. The change is to make the **web** do the same thing instead of loading `tier0.json` and
calling a Worker's `/api/resolve`.

Why this, and not a JSON bundle plus an API:

- **The React Native port stops being a port.** If web and native run the same SQL against the same
  file, the shared layer is everything except rendering. A new UI "built to translate to React Native"
  is achieved by construction rather than by discipline.
- **Offline is the default, not a mode.** The mobile app has no second code path to keep correct.
- **One thing to make rock solid.** One schema, one builder, one set of golden queries.
- **In-season changes are a data push, not a deploy.**

Web reads it with wa-sqlite over OPFS; native with `expo-sqlite`. Both get the same `@app/core`
query functions.

> **Refined in [`18`](18-bundle-builder.md):** the web does not download the whole database — it
> **range-reads** it, transferring ~100–200 KB to answer a tap. The "slower first load" cost noted
> below therefore mostly goes away. `18` has the measured sizes and the sharding scheme.

**Cost, stated plainly:** the web app downloads a few MB before it can answer anything, where today it
loads `tier0.json` and asks a Worker. First load gets slower; every later interaction gets faster and
works on a boat with no signal.

---

## 2. What ships

Regulation data is small — the sizes in `15` make that clear. Geometry is the problem, and it is a
separate artifact with a separate answer.

### 2a. `regs.sqlite` — the regulation spine

Everything derived from the registry + confirmed entries:

| table | holds |
|---|---|
| `item` | registry item: id, name, kind, mus |
| `section` | section id → item, blk, measures, display name |
| `boundary` | curated cuts, refs, labels |
| `entry` / `rule` | the synopsis rows and their rules, with dates and species |
| `rule_section` | **the resolved join** — which sections each rule actually covers |
| `search` | FTS5 over names and variants |
| `meta` | schema version, dataset version, build id, source hashes |

`rule_section` is the important one: reach resolution happens **in the pipeline**, once, not on the
client. `curation-review/backend/reuse.py:resolve_extent` already does exactly this — the builder
lifts that logic out of the review app so both use one implementation.

Estimate: 12 MB registry + 4.2 MB entries + the join → **~5–8 MB compressed**. Fine to download.

### 2b. Geometry — the 4.1 GB problem

`bc.pmtiles` is every stream in BC. It cannot be bundled, and this is the one place where "all data
offline" needs a decision rather than a design. Three options, in order of my preference:

1. **Regulable-waters-only tiles.** Only 19,413 items / 49,639 sections can ever carry a regulation.
   A PMTiles built from just those, simplified by zoom, is a small fraction of 4.1 GB. Everything the
   app can *say something about* is offline; unregulated creeks are absent rather than wrong.
   **Needs measuring before committing** — the number is not yet known.
2. **Region packs.** Full detail, downloaded per region (8 of them). Matches how people fish, but adds
   "which regions do I have?" to the UI and to the update logic.
3. **Online tiles, offline regulations.** Cheapest, and not what was asked for.

The basemap is a separate question again: Protomaps BC extract, or ship nothing and degrade to lines
on a blank ground when offline.

**Open — needs your call.** Option 1 is my recommendation, but only after the size is measured.

---

## 3. Updates and push

The delivery model is the part that has to be boring and correct.

```
manifest.json          <- the only mutable object
  schema_version       <- client refuses what it does not understand
  dataset_version      <- monotonic; what a push notification carries
  shards[]             <- {name, sha256, bytes, url}
```

- **Content-addressed shards.** A shard's filename contains its hash, so every shard is immutable and
  cacheable forever. Only `manifest.json` is ever overwritten.
- **Sharded by region.** An in-season change in Region 3 changes one shard's hash. The client
  downloads that shard, not the dataset.
- **The client update loop:** fetch manifest → diff hashes against local → download changed shards →
  **verify sha256** → write to a staging DB → atomic swap → keep the previous generation for rollback.
- **Push carries a version, not data.** `dataset_version` in the payload; the client pulls. A dropped
  or duplicated push cannot corrupt anything, and a client that has been offline for a month converges
  the same way as one that got every push.
- **Rollback is a first-class path**, not an incident procedure: previous generation stays on disk
  until the new one has answered a query successfully.

---

## 4. What "rock solid" has to mean, concretely

These are gates in CI, not aspirations:

1. **Determinism.** Same inputs → byte-identical bundle. Without this, every build churns every shard
   hash and the delta mechanism is a lie.
2. **Schema versioned and enforced.** Client refuses an unknown `schema_version` and keeps its old
   data rather than half-reading a new one.
3. **Golden queries.** A fixed set of (water, date) → regulations answers, asserted against the
   bundle. These are the parity tests `12-testing.md` asks for, retargeted from v1 to the resolver.
4. **Coverage invariants**, in the spirit of the graph's own checks: every `rule_section` row points at
   a real section; every confirmed entry contributes ≥1 row or is explicitly `reference_only`; no
   section is claimed by two conflicting closures without a recorded precedence.
5. **Size budgets.** Bundle and per-shard ceilings fail the build, so nobody discovers the download
   got 4× bigger from a user report.
6. **Integrity end to end.** Hash on write, verify on read, refuse on mismatch.

---

## 5. What to take from v1, and what not to

| v1 module | verdict |
|---|---|
| `enrichment/reach_builder.py` (40 KB) | **Do not port.** It reconstructs reaches from fids; the section spine already is the answer. `reuse.resolve_extent` replaces it |
| `enrichment/feature_resolver.py` (27 KB) | **Do not port.** Matching + the registry replace it |
| `tiles/tile_exporter.py`, `tiles/layer_manifest.py` | **Port the mechanics** — tippecanoe invocation, layer/zoom config — retargeted to `section_id` |
| `deploy/r2_sharder.py`, `deploy/mobile_sharder.py` | **Port the shape** — sharding + upload + manifest. Content changes, the delivery pattern is sound and proven |
| `base_reg_assigner.py` (24 KB) | **Read it.** Region/MU default regulations are real logic that v2 has not reimplemented, and "which defaults apply here" is still needed |

---

## 6. The new UI

Ground-up and simple, and the architecture above is what makes it portable:

```
@app/core      pure TypeScript. types, queries, regulation resolution, date logic,
               formatting. NO DOM, NO react-native imports. Tested headless.
@app/web       React + MapLibre GL JS + wa-sqlite/OPFS
@app/mobile    React Native + MapLibre Native + expo-sqlite
```

The rule that keeps this honest: **anything that would have to be written twice belongs in
`@app/core`.** If a screen needs logic the core does not have, the logic moves to the core first.

Map layers use the same style JSON and the same PMTiles on both. What differs is the renderer and the
storage adapter, and nothing else.

---

## 7. Sequencing

The bundle can be built against today's data — it does not wait for curation.

1. **Measure the geometry options.** Build regulable-waters-only tiles and see what it weighs. This
   answers §2b, and everything about "fully offline" depends on it.
2. **Lift `resolve_extent` out of the review app** into a shared resolver the builder and the app both
   call. One implementation, or the bundle and the review UI will drift.
3. **Build `regs.sqlite`** + the golden queries + determinism test.
4. **Manifest, sharding, verify/swap/rollback** — with a fake second version, so the update path is
   exercised before it ever matters.
5. **`@app/core`**, extracted against the real bundle.
6. **Web client**, then **mobile** — mobile becomes the renderer swap the architecture promises.

Curation runs in parallel throughout. It gates *launch*, not this work: at 108/1,392 confirmed the
bundle would be honest but thin, and every entry confirmed after that is a data push, not a release.

## Open questions

1. **Offline geometry scope** — §2b. Needs the measurement first, then your call.
2. **Does the web app go offline-first too?** §1 assumes yes. If the web should stay
   server-queried, the shared-core promise weakens and mobile carries a second data path.
3. **Push transport** — Expo Push, or FCM/APNs directly?
4. **Update cadence** — in-season notices are the fast-moving part. Daily manifest poll plus push on
   change, or push only?
5. **Region defaults** — v1's `base_reg_assigner` has no v2 equivalent. Does the bundle carry
   region/MU default regulations, and does the client apply them, or does the pipeline pre-resolve
   them into `rule_section`?
