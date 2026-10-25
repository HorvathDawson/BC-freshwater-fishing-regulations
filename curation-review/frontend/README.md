# Curation Review — frontend

Vite + React + TypeScript screen for REVIEWING the parsed BC fishing-regulation catalogue entries
against the book. Talks to the FastAPI backend (see `../API.md`) over `/api` (proxied to
`http://127.0.0.1:8787`). It writes one thing, a reviewer's mark (`POST /api/marks`); there are no
editors (T185).

## Run

```bash
bash curation-review/run.sh      # backend :8787 + this screen on http://127.0.0.1:5173 (loopback only)
```

or the two halves by hand: `bash curation-review/backend/run.sh`, then `npm install && npm run dev`
here. The Vite dev server proxies `/api/*` and `/basemap/*` to the backend (`CURATION_API` moves it).

## Check

```bash
./node_modules/.bin/tsc -p tsconfig.json --noEmit    # must compile clean (no unit runner here)
```

The browser test is `pipeline/tests/test_curation_review_browser.py` (`-m slow`, Playwright).

## What it does

- **Filter bar** — region (`GET /api/regions`; "all regions" by default), status
  (`reuse.STATUSES`), review (to do / never marked / back for a re-check / changed since marked /
  wrong / unsure / deferred / human-reviewed), order, tier; progress per tier with the ETA.
- **Queue** (`GET /api/entries?…&order=risk`) — THE ONE QUEUE (T135), entries only (T184): tier by
  tier in the review order (server-sorted), or the book's order; rows show the human-reviewed chip
  ("✓ reviewed · X · date", or "re-check"), tier, risk and pre-check chips, status, MUs.
- **Entry** (`GET /api/entries/{id}`, `/review`, `/reaches`, `/splits`): the evidence (the row
  crop, the coverage strip), the pre-checks, the T R P A checklist and the verdict bar; then the
  identity, `matched`, the entry's scope, every rule (label, quote, WHERE it binds with its splits by
  name, the builder's answer), the licensing, the splits the rules name, the bindable boundaries,
  and the **map**: the water, one rule's reach at a time, the tributaries / in-out view, and EVERY
  SPLIT THE RULES NAME drawn and labelled with its name (T184 — the proxy check of a split; a
  wrong one is a "Place: wrong" mark on the entry).
- **Keys** — `t r p a` · `y n ?` · Enter · `j k` · `u` · `o` · `m` (see `../README.md`).
