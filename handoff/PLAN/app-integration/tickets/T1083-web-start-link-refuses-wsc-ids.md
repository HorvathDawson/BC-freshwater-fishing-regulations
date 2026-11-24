# T1083 — The web start link refuses `wsc:` item ids, yet the app writes `?regs=wsc:…` into its own URL (reload and share fail)

Status: OPEN.

Source: the 0.9.1 chat, 2026-11-24, PR #131 lane, web build from main: "start link: regs=wsc:… is not an item id". The app itself writes `?regs=wsc:…` into the URL, so reloading or sharing the page of a `wsc:` water fails. 197 of the parts T1052 touches are `wsc:` waters. Evidence: a Playwright console log of 2026-11-24 20:19Z in the main checkout's git-ignored .playwright-mcp directory. NOT yet checked on production 0.9.0.

CONFIRMED on production 0.9.0, 2026-11-24 15:11 MST (the 0.9.1 chat): opening the site with `?regs=wsc:100-019698-831000`
logs "start link: regs=wsc:100-019698-831000 is not an item id", the URL is rewritten to the bare site and the reader
lands on the bare map. A reload or a shared link of a `wsc:` water loses the water.

## Work
1. Check production 0.9.0 first: open a `wsc:` water on the web, reload, and open its share link on a phone. Write the result here.
2. The start-link reader accepts every id kind the app writes (one list shared by the writer and the reader, so they cannot drift).
3. A test that round-trips every id kind through write then read; a mutation that drops one kind fails it.

## Acceptance
a `wsc:` water's page reloads and its link opens on web and phone; the round-trip test covers every id kind.

Severity: HIGH (production: shared and reloaded links to `wsc:` waters are dead). Release: 0.9.1 under "broken" (the
0.9.1 chat takes it). Model: Opus. Size: S. Owner: no.
