# Curation Review — frontend

Vite + React + TypeScript UI for reviewing/confirming LLM-parsed BC fishing-regulation
entries. Talks to the FastAPI backend (see `../API.md`) over `/api` (proxied to
`http://127.0.0.1:8787`).

## Run

```bash
# 1. Start the backend (in the repo root) — makes NO LLM calls, spends no credits:
bash curation-review/backend/run.sh          # serves http://127.0.0.1:8787

# 2. In another terminal, start the frontend:
cd curation-review/frontend
npm install
npm run dev                                    # http://localhost:5173
```

The Vite dev server proxies `/api/*` to the backend, so the app calls relative URLs.

## Build

```bash
npm run build      # tsc -b && vite build — must compile clean
```

## What it does

- **Filter bar** — region dropdown (`GET /api/regions`) + status filter
  (`no_registry | needs_review | unused_splits | unreviewed | confirmed`).
- **Queue** (`GET /api/entries?region=&status=`) — attention-first (server-sorted); rows show
  name, status badge, lock state, MUs, and an ⚠ marker when `unused_curated_splits > 0`.
- **Detail** (`GET /api/entries/{id}`):
  - identity header with a `registry_status` badge (NO REGISTRY highlighted),
  - side-by-side original `regs_verbatim` ↔ parsed `rules[]`, each rule's binding shown as
    human text ("upstream of Foo Falls") **and** raw (`op[splits]`), plus
    `needs_review` / `review_reason` / `unresolved_locators`,
  - the item's bindable **boundaries** (curated splits flagged with ★),
  - an **unused curated splits** warning block,
  - a **MapLibre** panel (`GET /api/items/{id}/geojson` — currently stubbed/empty; renders an
    OSM basemap without crashing),
  - per-rule extent editor (op dropdown + split multiselect with arity enforcement) and species
    tag input, per-entry tributaries, and the `no_registry` **attach-item** flow
    (`GET /api/items/search?q=`).
- **Actions** — `Save edit` (`PUT`), `Confirm & lock` (`POST /confirm`, stamps a curator name
  remembered in `localStorage`). Validation errors (422) render inline.

## Notes / assumptions

- The curator name is prompted once and stored in `localStorage` (`curation-review.curator`).
- The **attach-item** flow sets `entry.matched = [chosen.id]` **and** flips
  `registry_status` to `"matched"` — a `no_registry` entry rejects extents server-side, so the
  flip is required before the newly-attached item's reaches can be bound. Re-open the entry
  after saving so the backend re-resolves and returns the attached item's boundaries.
- The geojson endpoint is a stub (empty FeatureCollection); the map panel is wired and shows a
  feature count / "no geometry yet" note, ready for real geometry later.
