# T539 — Ledger guard checks that IN REVIEW can close; state lives in one place

Status: PARTIAL — 7323ce649; left (Claude): drain NO_STEP_YET (62 older IN REVIEW / PARTIAL rows) and move the restart page's owner-waiting sections to OWNER-TODO (see "Built" below).

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

## Built (2026-11-10, 7323ce649)
- `app/tools/check-tickets.mjs` (runs in `pnpm check` and fast-fail's "ticket ledger"): an IN REVIEW or PARTIAL Status line must name its step as `owner: OWNER-TODO "<text>"` (the text must exist in OWNER-TODO.md) or `left (Claude): …`; IN REVIEW whose only step is Claude's is flagged "that is IN PROGRESS". The 62 rows that predate the rule are on `NO_STEP_YET` (a list that only shrinks: a listed id that now complies, or is no longer in review, fails until it is taken off; a new ticket cannot be added). T529 was already DONE (smoke 18/18, r-a178299f6005).
- Docs guard, same script (`checkDocs`): STATUS.md starts with its title; every repo path cited in backticks in START-HERE, the restart page and OWNER-TODO exists (git-ignored outputs may be absent); the restart page holds no release id or build number; AGENTS rule ids are unique.
- Doc fixes the guard forced: the 5 stray lines above STATUS.md's title moved whole under "Orphan lines (T539)" at the end; the restart page's State bullet now points to the STATUS top entry (release id and Play build removed); OWNER-TODO: two dead paths reworded, phone-check item renamed "Phone check (current build)"; AGENTS: Absolute rules are A1-A6, Working-style 45/46 are W45/W46 (other files cite 5/6/45/46 for the data-model and Reading rules, so those keep their numbers); the ticket-ledger bullet and the CONFIRM_PROD line corrected.
- Proof: `app/tools/check-tickets.test.ts` (28 tests: IN REVIEW/PARTIAL with no step, a gone OWNER-TODO text, Claude-only IN REVIEW, a stale or new NO_STEP_YET id, STATUS stray lines, a dead path in each of the three docs, restart-page state, a repeated rule id) and `node app/tools/check-tickets.mjs` against the live ledger and docs: passes.
- Left (Claude): (1) drain `NO_STEP_YET` (62 ids: re-word each Status line with its step, or close it); (2) the restart page still lists "Waiting on the owner" and "Review app leftovers" sections, which are state (owner items belong in OWNER-TODO); the guard checks only release ids and build numbers there; (3) the 19 ticket files citing "Phone check with build 22" are not edited (the guard does not read them); (4) `scripts/fast-fail.sh` already runs the guard; no change needed there.
