# T1131 — The store pictures are made by a GitHub workflow started by hand

Status: OPEN — with the repo move (chat 3), after the transfer; step 1 is a proof on a Linux runner

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1131 | The store pictures are made by a GitHub workflow started by hand | OPEN — with the repo move (chat 3), after the transfer; step 1 is a proof on a Linux runner | | owner 2026-11-25: "yes", and "can be part of new repo transfer" |

Source: owner 2026-11-25 in the store content chat (relayed): he asked "should picture generation be a gha?", the
store chat recommended it, he answered "yes". Not started. Related: T1072 (the picture set), T1076 (`site.mjs`
builds the store-shots export), T1127 (the push list), T1128 (ends with a play-phone frame 3 reshoot, which this
workflow could do), T1113 (the salmon scene tied to live notices), `no-github-schedules`.

Owner, same day, relayed by the store chat: "can be part of new repo transfer". So the repo-move chat (roadmap row
3) builds it after the cutover, as a step after T1120, written for the new address (canifishthis/canifishthis) and
its standard runners; not its own release item.

## What
A `workflow_dispatch` workflow, never on a schedule and never on PRs: it builds the store-shots web export
(`EXPO_PUBLIC_STORE_SHOTS=1`), runs `node app/tools/store-shots.mjs --theme light` for all targets, and opens a PR
with the changed pictures, with a contact sheet as an artifact or PR comment. Inputs: optional `scenes` and `only`
(the tool's `--scenes` / `--only`), so one frame can be redone.

## Why
A clean checkout each run (the 34 leftovers of 2026-11-25 cannot happen), off the owner's Mac (no contention with
heavy jobs), one Chrome and one font set every run, and the pictures are reviewed in the PR. By hand only: the flow
and salmon frames show live data, so every run changes about 20 pictures and a person has to look at them (each
round this week found something no check caught); needed about once per release.

## Work
1. First, before anything else: prove the map frames render on a Linux runner (headless Chrome without a GPU,
   software GL) and that the tool's checks pass there (20 km scale, out-of-coverage band, watershed name). Not
   verified. Expect every picture to change slightly on the first run (Linux text rendering against macOS); the
   owner looks at the first set.
2. The workflow, on standard runners only (Actions stays free on a public repo), with the least permissions that
   can open a PR; no store keys: uploads stay the owner's.
3. The runbook's reshoot step names the workflow; the local command stays as the fallback.

Acceptance: a run started by hand opens a PR whose pictures pass the tool's checks and the size check; a run with
`only` set changes one frame; a failed check fails the run with the frame's name. Severity: none (tooling). Model:
Sonnet. Size: S–M. Owner: yes (looks at the first Linux-rendered set).
