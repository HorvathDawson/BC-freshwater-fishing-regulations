# Cron Data Flow

How the scheduled jobs keep the live site's data fresh, what each one needs,
and why. All crons are **GitHub Actions** scheduled workflows (they run from the
**default branch**, `main`). They read/write **Cloudflare R2** via the shared
S3/boto3 helper (`pipeline.recurring.r2_storage`); the webapp/worker serve those
R2 objects.

## The crons at a glance

| Workflow | Schedule | Writes to R2 | Purpose |
|---|---|---|---|
| `update-hydro` | every 30 min | `cron/hydro/*` | One unified job: latest readings + gauges.geojson every run; history merged daily; climatology on HYDAT release. NO persisted DB. |
| `update-in-season` | every 6 h | `in_season.json` | In-season regulation changes (closures/openings) |
| `update-stocking` | weekly (Mon 09:00 UTC) | `cron/stocking/records/*`, `stocking_index.json` | FIDQ per-waterbody stocking releases (no reach resolution) |

A free **Cloudflare Worker** (`cron-runner/`) fires punctually and dispatches these
workflows via `workflow_dispatch` (GitHub's own `schedule:` drifts under load; it's
kept as a coarse fallback).

The gauge→reach (`cron/hydro/gauge_matches.json`) and stocking→reach
(`cron/stocking/stocking_matches.json`) **match tables are STATIC**, produced once
per full pipeline build — the frontend joins reach_id → station_id / waterbody_id at
lookup time, so the crons carry no matching logic.

**Deploy env** is chosen per run: `main` → production bucket
(`bc-fishing-regulations`), any other branch → staging
(`bc-fishing-regulations-staging`).

**Required secrets** (repo-level): `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`,
`CLOUDFLARE_ACCOUNT_ID`. Without them the jobs run but fail at R2 I/O.

```mermaid
flowchart LR
    subgraph GHA["GitHub Actions (schedule, from main)"]
        RT[hydro-realtime<br/>15 min]
        NT[hydro-nightly<br/>daily]
        IS[in-season<br/>6 h]
        ST[stocking<br/>weekly]
    end
    R2[(Cloudflare R2<br/>data bucket)]
    APP[Webapp + r2-worker]

    RT -->|reads seed, shards<br/>writes realtime| R2
    NT -->|reads/persists hydro.db<br/>writes history| R2
    IS -->|reads tier0, match_table<br/>writes in_season.json| R2
    ST -->|reads anglerinfo.db release rows<br/>writes records/ + stocking_index.json| R2
    R2 --> APP
```

## Why R2 is the source of truth in CI

GitHub Actions runners are ephemeral and have **no pipeline output** — the full
regulations build (tiles, `tier0.json`, `poly_reaches.json`, the 66 MB
`anglerinfo.db`, the ~10 GB FWA GeoPackage) is far too big/slow to rebuild each
run. So every cron **pulls the inputs it needs from R2**, does a light
transform, and **pushes the result back**. The heavy build happens elsewhere
(locally / the full pipeline) and seeds R2.

---

## Hydro (one unified job)

ONE stateless job, `python -m pipeline.recurring.hydro.jobs run`. There is **no
persisted database in R2** — state lives entirely in the JSON artifacts. Each run
uses an ephemeral `/tmp` sqlite as scratch (to reuse `hydro_poc`'s fetchers +
`export_hydro`'s shapers), seeding the roster/coords from the prior
`stations.json` and merging history at the JSON level.

```mermaid
flowchart TD
    IDX[(stations.json<br/>roster + coords = state)]:::ext
    HYD[(hydat_release.json<br/>+ history_date.json markers)]:::ext

    subgraph run["jobs run (every ~30 min)"]
        A{stations.json<br/>in R2?}
        A -->|no| B[first run: full bootstrap<br/>roster + HYDAT + 18mo history + climatology]
        A -->|yes| C[seed roster/coords from stations.json<br/>+ hydro_poc update readings/forecasts]
        C --> D{HYDAT listing date<br/>> marker?}
        D -->|yes| E[fetch_hydat sync:<br/>refresh coords + rebuild climatology/]
        D -->|no| F[skip HYDAT 266MB download]
        E --> G
        F --> G[export realtime:<br/>stations.json + gauges.geojson + recent/]
        G --> H{new day?}
        H -->|yes| I[pull+merge history/<id>.json by date]
        H --> J[write markers + version.json, upload]
        I --> J
        B --> J
    end

    IDX -.state.-> A
    HYD -.gate.-> D

    classDef ext fill:#eef,stroke:#88a;
```

**What each needs & why**

- **`stations.json` is the roster state.** A non-first run seeds the scratch DB's
  station roster + coords from it, so it never re-pulls the 266 MB HYDAT metadata
  just to know the roster.
- **`hydat_release.json` marker** — compared to the HYDAT index *listing date*
  (a cheap HTTP GET, no download). The 266 MB HYDAT file is only pulled when the
  release advances; that path rebuilds `climatology/<id>.json` + refreshes coords.
- **`history/<id>.json` merge** — the daily-means record is kept by merging the
  freshly-fetched last ~14 days into the existing R2 file (idempotent by date,
  trimmed to ~18 months). The JSON files *are* the persisted record.
- **gauge→reach link is NOT here.** It's the static `gauge_matches.json`
  (`reach_id`/`waterbody_key` → `[station_id]`) built once by the full pipeline
  (`match_fwa` + fid→reach shards → `gauge_matches.py`). The frontend joins at
  lookup time; the cron needs no atlas, no fid shards, no `gauge_fwa_match`.

---

## In-season regulation changes

```mermaid
flowchart LR
    T0[(tier0.json)]:::ext --> SC
    MT[(match_table.json)]:::ext --> SC
    CFG[config.yaml<br/>repo root]:::ext --> SC
    SC[scraper<br/>fetch gov in-season page] --> RS[resolver<br/>reconcile vs match_table]
    RS --> OUT[in_season.json] --> UP[upload to R2]
    RS --> ISSUE[unmatched → GitHub issue]
    classDef ext fill:#eef,stroke:#88a;
```

- Pulls `tier0.json` + `match_table.json` from R2 (its reconciliation inputs).
- Reads defaults from **`config.yaml` at the repo root** (resolved via
  `parents[3]` from `pipeline/recurring/in_season/…`).
- Deps: `requests beautifulsoup4 pyyaml python-dotenv boto3`.
- Any regulation it can't reconcile is filed as a GitHub issue by the workflow.

---

## Stocking — two-tier (fetch vs match decoupled)

All stocking state lives in `anglerinfo.db`. Refreshing it fully needs geopandas
and the ~10 GB FWA GeoPackage, so it is split into a **light CI tier** and a
**heavy manual tier**. R2's `cron/stocking/anglerinfo.db` is the shared state.

The **matching** (which FIDQ waterbody → which reach) is now a STATIC artifact
built once per full pipeline run (`stocking_matches.json`); the light cron only
**fetches + shapes** per-waterbody records. The frontend joins reach_id →
waterbody_id at lookup time.

```mermaid
flowchart TD
    subgraph light["LIGHT — update-stocking.yml (weekly, CI)"]
        L1[pull anglerinfo.db from R2<br/>read-only] --> L2[fetch_stocking update<br/>refresh FIDQ releases, no geopandas]
        L2 --> L3[export_records:<br/>records/&lt;waterbody_id&gt;.json + stocking_index.json]
        L3 --> L4[upload records/ + index]
    end

    subgraph heavy["HEAVY — local, manual (monthly-ish)"]
        H1[build_db --export<br/>fetch_all + full match chain] --> H2[upload-stocking-db.sh]
    end

    subgraph full["FULL PIPELINE build (manual)"]
        P1[build_matches:<br/>match_final × poly_reaches] --> P2[stocking_matches.json]
    end

    GPKG[(FWA gpkg ~10 GB<br/>+ geopandas)]:::ext --> H1
    DB[(R2: cron/stocking/anglerinfo.db)]

    H2 -->|sole writer| DB
    DB -->|read-only| L1
    DB -->|read-only| P1

    classDef ext fill:#eef,stroke:#88a;
```

**Why two tiers**

| | Light (weekly CI) | Heavy (manual, local) |
|---|---|---|
| Runs | `fetch_stocking` + `export_records` | full `build_db` (fetch_all + match chain) |
| Needs | `anglerinfo.db` (R2) — release rows only | geopandas, ~10 GB FWA gpkg, gov WFS |
| Surfaces | **new releases** for existing waterbodies | **new waterbodies** (new `match_final` rows) |
| Writes db to R2? | **No** (read-only — avoids racing heavy) | **Yes** (sole writer) |

- `waterbody_id` is FIDQ's **stable** `TEXT PRIMARY KEY`, so records keyed by it
  stay valid across light-tier refreshes and match the `stocking_matches.json`
  the full build produces.
- `stocking_matches.json` (`reach_id`/`waterbody_key` → `[waterbody_id]`) is built
  by `resolver.build_matches` during a full regulations build (it needs
  `poly_reaches.json`, which only changes on a full build).

**Heavy-tier run (local):**

```bash
python -m pipeline.recurring.anglerinfo.build_db --export   # rebuild db (heavy)
DEPLOY_ENV=production ./scripts/upload-stocking-db.sh        # publish db to R2
```

---

## Limitations (today)

The current design optimises for "cheap to run on free GitHub Actions + R2" and
accepts real trade-offs to get there:

1. **R2 blobs are the database.** State lives in whole-file sqlite DBs / JSON in
   R2 that crons pull → mutate → push. There are no transactions and no
   incremental updates — most runs rewrite whole objects (hydro history is a
   per-station merge). Correctness against concurrency relies on workflow
   `concurrency:` groups plus conventions (the stocking light job is read-only;
   hydro's version.json is written last).
2. **New stocking waterbodies need a human.** The heavy match chain needs
   geopandas + the ~10 GB FWA GeoPackage + gov WFS, which can't run on a CI
   runner, so it's a manual local job (`build_db` → `upload-stocking-db.sh`).
   Until someone runs it, genuinely new waterbodies never appear — a staleness
   and bus-factor risk.
3. **Match tables aren't a cron.** The gauge→reach (`gauge_matches.json`) and
   stocking→reach (`stocking_matches.json`) tables — plus `poly_reaches.json` /
   `tier0.json` / tiles — come from the full regulations build, which is manual.
   The crons deliberately carry no matching logic, so a new gauge/waterbody only
   links to a reach after the next full build.
4. **Weak cross-dataset versioning.** `data_version.json` is a single global
   marker; a partly-completed multi-file upload can briefly serve a mixed state.
   Only hydro uses a per-tree `version.json` fence.
6. **Schedule reliability.** GHA cron is best-effort (the 15-min realtime job is
   often delayed), schedules are disabled after 60 days of repo inactivity, and
   a newly-added workflow may skip its first tick.
7. **Thin observability.** Only in-season surfaces failures (it files a GitHub
   issue for unmatched regs). The others fail silently in the Actions UI — no
   alerting, metrics, or freshness dashboard.

## If rebuilt from scratch

Roughly in order of leverage:

- **Real datastore instead of sqlite-in-R2.** A Postgres/PostGIS (or DuckDB)
  source of truth would give transactions and *incremental* updates: crons
  UPSERT rows, exporters read. This removes the pull-whole-DB / push-whole-DB
  dance and most concurrency hazards.
- **Atomic, versioned snapshots.** Publish each build to a content-addressed or
  timestamped prefix and flip a single manifest pointer last, so clients never
  observe a partial update. Replaces the ad-hoc `data_version.json`.
- **Give the heavy anglerinfo build real compute.** A self-hosted runner /
  scheduled cloud VM / container with the FWA gpkg cached (or streamed from R2)
  would let new-waterbody matching run automatically instead of manually — and
  fold the two stocking tiers back into one scheduled flow.
- **Publish `gauge_fwa_match` as its own scheduled artifact** so hydro stops
  depending on an enrich-time seed and the bootstrap-restore hack goes away.
- **Make the regulations build schedulable/incremental** so `poly_reaches` /
  `tier0` refresh without a manual run — the thing every downstream cron leans on.
- **One orchestrator, not N independent scripts.** A small DAG (or a single
  workflow running ordered steps) with shared input caching removes the repeated
  "pull the same tier0/poly_reaches from R2" work and makes ordering explicit.
- **Uniform alerting + run summaries** for every cron (not just in-season), plus
  a simple freshness check per dataset.

## Adding / changing a cron — checklist

1. The workflow's `schedule:` only takes effect once it's on **`main`**.
2. Pull inputs from R2 (never assume pipeline output exists on the runner).
3. Keep CI light — no geopandas / multi-GB reference data. Push those to a
   manual/local tier that seeds R2.
4. Ensure the three R2/Cloudflare secrets are present.
5. Guard for optional tables/files so a missing input degrades, not crashes.
