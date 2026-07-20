# Live Data

Recurring, non-static data feeds — sources that change over time and need
periodic re-fetching, as opposed to `data/` (mostly-static geospatial base
layers, re-fetched occasionally when the underlying government dataset
updates) or `pipeline/` (regulation extraction/parsing/matching, run against
a point-in-time synopsis).

Each subdirectory here is its own proof-of-concept: a standalone fetch
script + local SQLite database, meant to prove out a data source before it
graduates into the production pipeline/app (mirroring how `hydro/` started).

## Current feeds

- **`hydro/`** — hydrometric gauge data (water level / discharge) from ECCC
  Wateroffice, plus BCRFC forecast models (CLEVER/COFFEE/ELF). Moved here
  from the repo-root `hydro-poc/` — same code, same `python hydro_poc.py
  bootstrap|update` workflow, just relocated.

`waterbody_db/` — stocking (FIDQ) + gofishbc markers + bathymetry (WSA
surveys) + WDIC 1:50K reference data, previously here — has graduated into
`pipeline/recurring/waterbody_db/`. Its own matching chain fully superseded
`pipeline/matching/bathymetry_matcher.py`'s bathymetry resolution and now
feeds waterbody enrichment (names, depth-map PDFs, marker amenities) into
the production build, plus a recurring stocking-info pipeline. See
`pipeline/recurring/waterbody_db/README.md`.

## Recurring data *not* here (and why)

`pipeline/recurring/in_season_scraper.py`/`in_season_resolver.py` and
`pipeline/recurring/waterbody_db/` also fetch recurring data (in-season
regulation change notices, and stocking/bathymetry/marker matching,
respectively) but live in `pipeline/recurring/` instead of here, because
neither is a POC waiting to graduate — they're already in production. Every
6 hours, `.github/workflows/update-in-season.yml` runs
`scripts/update-in-season.sh --upload`, which scrapes, resolves, and pushes
`in_season.json` straight to Cloudflare R2 (staging/production); the same
scrape+resolve also runs inline from `pipeline/enrichment/builder.py`'s main
build step. `live-data/` is for things still proving themselves out.

`in_season_scraper`/`in_season_resolver` moved out of `pipeline/matching/`
into their own `pipeline/recurring/` package — matching's actual job is
synopsis→FWA linking, and lumping a scheduled live-site scraper in there
blurred what was static-build-time logic vs. what runs on a cron. The
resolver still imports `BaseEntry`/`MatchTable`/`OverrideEntry` from
`pipeline/matching/match_table.py` (a normal cross-package import, same as
several other pipeline modules already do — nothing special about the
directory boundary here).
