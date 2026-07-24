# Cron Data Flow

How the scheduled jobs keep the live site's data fresh, what each one needs,
and why. All crons are **GitHub Actions** scheduled workflows (they run from the
**default branch**, `main`). They read/write **Cloudflare R2** via the shared
S3/boto3 helper (`pipeline.recurring.r2_storage`); the webapp/worker serve those
R2 objects.

## The crons at a glance

| Workflow | Schedule | Writes to R2 | Purpose |
|---|---|---|---|
| `update-hydro-realtime` | every 15 min | `cron/hydro/*` (realtime scope) | Latest gauge readings/forecasts |
| `update-hydro-nightly` | daily 09:00 UTC | `cron/hydro/hydro.db`, `cron/hydro/*` | 18-mo history + HYDAT climatology; persists the DB |
| `update-in-season` | every 6 h | `in_season.json` | In-season regulation changes (closures/openings) |
| `update-stocking` | weekly (Mon 09:00 UTC) | `cron/stocking/stocking.json` (+ legacy) | FIDQ fish-stocking releases → reach IDs |

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
    ST -->|reads anglerinfo.db, poly_reaches<br/>writes stocking.json| R2
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

## Hydro (realtime + nightly)

Two jobs share one persisted database, `cron/hydro/hydro.db`, plus a small
`cron/hydro/hydro_seed.db` (station roster + `gauge_fwa_match`) built by the
enrichment pipeline.

```mermaid
flowchart TD
    SEED[(hydro_seed.db<br/>stations + gauge_fwa_match)]:::ext
    HDB[(hydro.db<br/>readings + history + climatology)]

    subgraph nightly["nightly_job (daily)"]
        N1{hydro.db<br/>in R2?}
        N1 -->|yes| N2[update readings/forecasts<br/>+ fetch HYDAT]
        N1 -->|no| N3[bootstrap:<br/>roster + 18mo history + climatology]
        N2 --> N4
        N3 --> N4[ensure gauge_fwa_match<br/>restore from seed if missing]
        N4 --> N5[pull fid shards for gauge fids]
        N5 --> N6[export history/all scopes]
        N6 --> N7[persist hydro.db + upload out/]
    end

    subgraph realtime["realtime_job (15 min)"]
        R1[pull hydro_seed.db as working DB] --> R2b[pull fid shards]
        R2b --> R3[update realtime readings]
        R3 --> R4[export realtime scope + upload]
    end

    SEED -.seed.-> R1
    SEED -.restore.-> N4
    HDB <--> N1
    N7 --> HDB

    classDef ext fill:#eef,stroke:#88a;
```

**What each needs & why**

- **`hydro_seed.db`** — carries `gauge_fwa_match` (gauge → FWA `fwa_id`, 462 rows)
  and the station roster. Built at enrich time by `match_fwa` (atlas-time,
  geopandas). The realtime job uses it directly as its working DB.
- **fid shards** (`shards/v{N}/fids/{prefix}.json`) — only the buckets the gauge
  fids fall into are pulled, so the exporter can resolve gauge `reach_id`s
  without the full shard set.
- **Nightly `gauge_fwa_match` restore** — `bootstrap`/`update` build a DB that
  has *no* `gauge_fwa_match` (that table is atlas-time only). Nightly therefore
  restores it from `hydro_seed.db` before pulling shards; `_gauge_fids` also
  guards the table's absence so a seedless run degrades instead of crashing.

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

## Stocking — two-tier

All stocking state lives in `anglerinfo.db`. Refreshing it fully needs geopandas
and the ~10 GB FWA GeoPackage, so it is split into a **light CI tier** and a
**heavy manual tier**. R2's `cron/stocking/anglerinfo.db` is the shared state.

```mermaid
flowchart TD
    subgraph light["LIGHT — update-stocking.yml (weekly, CI)"]
        L1[pull anglerinfo.db from R2<br/>read-only: match_final] --> L2[fetch_stocking update<br/>refresh FIDQ releases, no geopandas]
        L2 --> L3[resolver: match_final + fidq records<br/>× poly_reaches → reach_id]
        L3 --> L4[upload stocking.json]
    end

    subgraph heavy["HEAVY — local, manual (monthly-ish)"]
        H1[build_db --export<br/>fetch_all + full match chain] --> H2[upload-stocking-db.sh]
    end

    PR[(poly_reaches.json)]:::ext --> L3
    GPKG[(FWA gpkg ~10 GB<br/>+ geopandas)]:::ext --> H1
    DB[(R2: cron/stocking/anglerinfo.db)]

    H2 -->|sole writer| DB
    DB -->|read-only| L1

    classDef ext fill:#eef,stroke:#88a;
```

**Why two tiers**

| | Light (weekly CI) | Heavy (manual, local) |
|---|---|---|
| Runs | `fetch_stocking` + resolver | full `build_db` (fetch_all + match chain) |
| Needs | `anglerinfo.db` (R2), `poly_reaches.json` | geopandas, ~10 GB FWA gpkg, gov WFS |
| Surfaces | **new releases** for already-matched waterbodies | **new waterbodies** (new `match_final` rows) |
| Writes db to R2? | **No** (read-only — avoids racing heavy) | **Yes** (sole writer) |

- `waterbody_id` is FIDQ's **stable** `TEXT PRIMARY KEY`, so `match_final.source_id`
  stays valid across light-tier `fetch_stocking` refreshes — the light job can
  refresh releases without invalidating the heavy job's matches.
- The resolver maps each waterbody's `waterbody_key` → `reach_id` via
  `poly_reaches.json`, which only changes on a full regulations build.

**Heavy-tier run (local):**

```bash
python -m pipeline.recurring.anglerinfo.build_db --export   # rebuild db (heavy)
DEPLOY_ENV=production ./scripts/upload-stocking-db.sh        # publish db to R2
```

---

## Adding / changing a cron — checklist

1. The workflow's `schedule:` only takes effect once it's on **`main`**.
2. Pull inputs from R2 (never assume pipeline output exists on the runner).
3. Keep CI light — no geopandas / multi-GB reference data. Push those to a
   manual/local tier that seeds R2.
4. Ensure the three R2/Cloudflare secrets are present.
5. Guard for optional tables/files so a missing input degrades, not crashes.
