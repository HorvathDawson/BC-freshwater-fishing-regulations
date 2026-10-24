# `design/` — reference documents

| file | settles |
|---|---|
| `data-contract.html` | what the data is, and in what format |
| `regs-v11.html` (+ `regs-v11/`) | the regulations prototype the regs screens are ported from |
| `colour.md` | how the palette is chosen |

`data-contract.html` is "One Interface, Two Storages", saved out of
<https://claude.ai/code/artifact/c672479b-fe44-4ddb-9046-6eb2300aeccf>. It settles the
four artifacts (tiles / bundle / feeds / precomputes), the two version domains, the
byte budget, and — §10 — that the data is **SQLite**, one format serving a resident
mobile copy and a web copy. When an implementation disagrees with it, it is right and the
implementation is wrong.

**What the app looks like** is settled by the app itself: the tokens in
`packages/ui-native/src/theme.ts` and `type.ts`, and the components in `packages/ui-native`.
The earlier whole-app prototype (riffle) is obsolete and was deleted (owner 2026-10-21, T61);
git history keeps it.
