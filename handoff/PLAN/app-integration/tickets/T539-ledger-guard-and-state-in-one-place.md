# T539 — Ledger guard checks that IN REVIEW can close; state lives in one place

Status: OPEN.

Source: workflow review 2026-11-09 (verified against the files). Rank 7.

## Today
- `app/tools/check-tickets.mjs:98-102` compares only the leading status keyword of file and row; a hash is required only
  for DONE (`:83,100`). README `:28` defines IN REVIEW as "name the proof", unchecked. Sampled: T529 says "left (Claude):
  site smoke + release --staging rerun" but STATUS records both done (smoke 18/18, r-a178299f6005) — it is DONE, unclosed.
  T530 (shipped in build 23) and T525, T390, T461, T310, T292 carry status lines with no step anyone can tick.
- The restart page goes stale in hours: `NEXT-SESSION-after-chat9.md:9-11,22-23` says build 22 and "next APK carries
  T530 / T531 waits"; STATUS the same day says builds 23 and 24 shipped both. OWNER-TODO's phone-check item was renamed to
  "build 24", while 19 ticket files still cite it as `"Phone check with build 22"`.
- AGENTS.md reuses rule numbers: 5 (`:30,40`), 6 (`:32,47`), 45 (`:322,380`), 46 (`:336,385`). `:349-351` says the guard
  checks "Status line and ledger row agree" (it checks the keyword only). `:366` gives one `CONFIRM_PROD` form; the
  restart page uses another.
- STATUS.md has five stray lines above its title; OWNER-TODO cites a file that does not exist.

## Work
1. `check-tickets.mjs`: an IN REVIEW or PARTIAL status must name its step as `owner: OWNER-TODO "<text>"` (text must
   exist in OWNER-TODO.md) or `left (Claude): …`; a ticket whose only remaining step is Claude's is IN PROGRESS. Mutation
   tests as for the existing rules.
2. Close T529; fix the status lines the new rule flags.
3. Restart page holds no state: paste prompt, ordered ticket ids, rules learned. Release id and Play build live only in
   the STATUS top entry. The owner phone-check item gets a stable name ("Phone check (current build)").
4. Docs guard in fast-fail (seconds): STATUS starts with its title; every repo path cited in START-HERE, the restart
   page and OWNER-TODO exists; AGENTS rule ids are unique (renumber once, section-prefixed).
5. Correct AGENTS `:349-351` and the `CONFIRM_PROD` line.

## Acceptance
- Guard fails on: IN REVIEW with no step, a cited OWNER-TODO text that is gone, a repeated rule id, a dead path.
- Ledger passes with every IN REVIEW ticket naming a step that exists.

Depends: —. Size: M. Owner: no.
