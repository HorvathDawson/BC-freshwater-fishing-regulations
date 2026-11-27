# app-integration: the work list and the pages that describe the app

The Regulations page (regs v33, the prototype) is ported into the app and shipped. The prototype is deleted; its
oracles live in `app/fixtures/golden/`. This folder holds the ticket system and a few pages about what is built.

## What is here

| path | what |
|---|---|
| `tickets/` | the only work list: one file per ticket, its own ledger row inside; rules in `tickets/README.md` |
| `tickets/closed.tsv`, `tickets/archive/` | closed tickets: one line each, full text in the yearly tarball |
| `tickets/OWNER-UNFILED.md` | owner asks not yet in a ticket |
| `decisions.md` | the owner's binding decisions in force, by subject |
| `contracts.md` | `RegsPage` (the view model every component draws), `Line`/`Run`, `RegsInputs`, field to answers map |
| `tokens.md` | prototype tokens (light/dark) against the app theme, type scale, spacing |
| `tests.md` | which test covers which check; the golden parity harness; the reader gate |
| `next-rebuild.md` | how to try a curated change on a side atlas before promoting it |
| `store/` | store-listing and privacy-label drafts |
| `T<id>-shots/`, `T<id>-files/` | pictures and data a ticket points at; kept while its ticket is open |
| `views/` (in `docs/`) | `LEDGER.md`, `OWNER.md`, `DONE.md`: generated views of the tickets |

## Using the ticket system

1. Claim a number: `node app/tools/ledger.mjs --next`. Make `tickets/Tnn-slug.md` with a `Status:` line, the
   one-row ledger table under it, then Source / Work / Acceptance (template in `tickets/README.md`).
2. Work on a branch named `<type>/Tnn-...` from a fresh `origin/main`; commit with `type(Tnn): subject` and
   `git commit -- <paths>` (other agents share the tree; never `git add -A`). PRs land through the merge queue.
3. An owner step is a line of the ticket's `## Owner` block: `- [ ] YYYY-MM-DD — the ask`. His answer is written in
   the same turn, with his words.
4. Close the ticket with the file's `Status:` and its ledger row together (DONE needs a commit hash).
   `node app/tools/ledger.mjs --archive` moves closed tickets into `closed.tsv` and the tarball.
5. `node app/tools/check-tickets.mjs` must pass (it is part of `pnpm check`): it checks status words, the ledger,
   commit subjects against ticket ids, the docs sizes (`app/tools/check-size.mjs`) and the cited paths.

## The port's conventions (still binding for `app/packages/`)

- Layers: pure logic in `core/src/regs/*` (imports only `../answers.generated`, `../statusIndex`,
  `../calendar.generated` and siblings); store reads in `data/src/regs/*`; hooks in `ui/src`; components in
  `ui-native/src/regs/*` with no logic (a component gets a view-model slice and a `Palette` and draws it).
- Words: every string a presenter emits is a data string (frame, static, rule) or a constant in
  `core/src/regs/words.ts`; no English literal in a presenter or component (the `no-hand-facts` render test checks it).
- Tests: core `*.test.ts` beside the file; golden parity tests in `app/conformance/src/regs/*.parity.test.ts`
  (core may not import `@app/data` or `node:fs`); render tests mount frozen view-model fixtures written by
  `UPDATE_REGS_FIXTURES=1`. A ticket is verified by `pnpm -C app check` plus its named vitest filter.
- Oracles are re-cut by command, never by hand: `node app/tools/regs-recut/sync.mjs` (the release runs it on its
  candidate) rewrites `taps.json`, `page-oracle.json`, the fixture release and the view-model fixtures.
- The LLM parser and anything calling the `claude` CLI are human-only (AGENTS A1). Heavy jobs go through `scripts/heavy.sh`.
