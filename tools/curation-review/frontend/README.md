# Curation Review — frontend

Vite + React + TypeScript screen for REVIEWING the parsed BC fishing-regulation catalogue entries
against the book. Talks to the FastAPI backend (see `../API.md`) over `/api` (proxied to
`http://127.0.0.1:8787`). It writes one thing, a reviewer's mark (`POST /api/marks`); there are no
editors.

## Run

```bash
bash tools/curation-review/run.sh      # backend :8787 + this screen on http://127.0.0.1:5173 (loopback only)
```

or the two halves by hand: `bash tools/curation-review/backend/run.sh`, then `npm install && npm run dev`
here. The Vite dev server proxies `/api/*` and `/basemap/*` to the backend (`CURATION_API` moves it).

## Check

```bash
./node_modules/.bin/tsc -p tsconfig.json --noEmit    # must compile clean (no unit runner here)
```

The browser test is `pipeline/tests/test_curation_review_browser.py` (`-m slow`, Playwright).

## What it does

- Filter bar: region, status (`reuse.STATUSES`), review state, order and tier, with progress per tier.
- Queue (`GET /api/entries?...&order=risk`): the one queue, entries only, in the server's review order or the book's.
- Entry (`GET /api/entries/{id}`, `/review`, `/reaches`, `/splits`): the evidence, the pre-checks, the T R P A
  checklist and verdict bar, every rule with where it binds, the splits the rules name, and the map (one rule's
  reach at a time, every named split drawn; a wrong one is a "Place: wrong" mark).
- Keys: `t r p a`, `y n ?`, Enter, `j k`, `u`, `o`, `m` (see `../README.md`).
