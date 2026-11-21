# T992 — Explain how the golden pages, fixture re-cuts and the release command work

Status: POSTPONED — owner 2026-11-21: a future ticket, not 0.8.5. Don't think about it now.

Source: owner 2026-11-21, during the 0.8.5 staging release, verbatim: "add a ticket that is about how all this golden
stuff and release works. dont think about it niow just future ticket".

## Context (what prompted it)
The 0.8.5 staging release (`python -m pipeline release --staging`) stopped eight times, each at a different gate:
- the fresh-copy tests;
- fast-fail;
- CI;
- the staging site's app version;
- the app-version gate (`--min-app-version`);
- a size gate;
- the binding diff (`--accept-diff`);
- the golden-page re-cut (`--accept-fixtures`, 9 pages whose source indexes only renumbered).

The owner wants to understand what these are and how they fit together.

## Work (to scope when picked up)
- Write a plain explanation for the owner covering:
  - what a golden page / fixture re-cut is;
  - what the binding diff checks;
  - each preflight gate;
  - when an accept is safe.
- Possibly simplify: for example, the re-cut could tell a renumber-only change from a change in what anglers read, so a
  pure renumber needn't stop the release.

Depends: none. Size: ?. Owner: reads the explanation.
