# T1150 — Noons Creek and Seymour River: two added city streams are filed under the wrong river; use FWA's line for now

Status: OPEN — 0.10.0 (owner 2026-11-25); a read-only finding first, then a curated change + an atlas rebuild

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1150 | Noons Creek and Seymour River: two added city streams are filed under the wrong river; use FWA's line for now | OPEN — 0.10.0 (owner 2026-11-25); a read-only finding first, then a curated change + an atlas rebuild | — | 0.10.0 chat: the finding is running; nothing written until it is in; the two parts are a hard stop in T1140's naming until this lands |

Source: the 0.10.0 chat's T1140 naming lane, 2026-11-25. Noons Creek p4 and Seymour River p3 are added municipal
stream lines filed under those rivers but lying 693 m and 1,068 m from them, touching other creeks; they have no
flow link, so no join distance can be derived and the naming step stops on them (no silent label). Related: T1140,
T1146, the memory notes `added-streams-topology-resolver`, `added-streams-dem-orientation`.

Owner, 2026-11-25 evening (to the 0.10.0 chat, relayed), verbatim: "We want them to be refiled properly so let's
take the fwa version for now but add a gotcha to check on which one is the real stream line."

## Work
1. Read-only finding: what the two rows are, why they sit in those rivers' items, what FWA holds there, where the
   earlier "leave out" acceptance is recorded and why it did not take effect, and whether other added streams are
   misfiled the same way (a count, with the distance from their river).
2. For these two: stop using the added municipal line and use FWA's own line. This is a curated change: it is
   shown to the owner as a proposal with the before/after map picture; no agent writes a mark.
3. A gotcha where gotchas live (`pipeline/docs`), in his words: check which one is the real stream line.
4. Atlas rebuild; the expected parity stated before the run; diff every rule binding; tiles and bundle of one
   vintage.

## Acceptance
Neither water has a part that cannot be named; the binding diff is attributed line by line and touches only these
two waters (anything else stops and comes to the planning chat); a check refuses an added stream farther than the
snap limit from the river it is filed under. Severity: medium (a stream line shown under the wrong river).
Model: Opus. Size: S–M + one atlas rebuild. Owner: the before/after look; the gotcha's check later.

## Owner

- [x] 2026-11-25 — Noons p4 and Seymour p3 cannot be named (misfiled added streams): what to do → "We want them to be refiled properly so let's take the fwa version for now but add a gotcha to check on which one is the real stream line." (2026-11-25)
- [ ] 2026-11-25 — Later, not blocking: check which line is the real stream for Noons Creek and Seymour River (the added city line or FWA's); the before/after map pictures come with the 0.10.0 morning summary.
