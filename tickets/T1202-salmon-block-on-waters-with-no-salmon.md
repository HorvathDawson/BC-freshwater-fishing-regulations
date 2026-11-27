# T1202 — A water with no salmon shows "Salmon · DFO · Open today"

Status: DROPPED — the owner: leave it as it is (2026-11-26)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1202 | A water with no salmon shows "Salmon · DFO · Open today" | DROPPED — the owner: leave it as it is (2026-11-26) | — | — |

Source: seen by the tester feedback chat, 2026-11-26, in a closed tester's picture of a water page (app 0.9.0; the
tester did not mention it). Feedback log D22.

Dease Lake (Region 6) drains to the Arctic by the Dease and Liard rivers and has no salmon. Its page opens with a
"SALMON · DFO · ✓ Open today" bar and a Salmon row under Species and limits, ahead of the game fish an angler there is
after. The words are legally right (the Region 6 salmon rules apply to every water not named), but the block leads the
page with a fish that is not there.

Found: there is no salmon-presence gate. `useSalmonCard` (`app/packages/ui/src/regs.ts`) returns a card for every page
when the release ships salmon data; `WaterScreen.tsx` skips it only on tidal pages; `salmonCard`
(`app/packages/core/src/salmon.ts`) falls back to the region default.

## Work (after the owner's answer)

- A: no change (the rule applies, so it is shown).
- B: keep the block, but on a water with no known salmon put it below the game fish and say so in one line
  ("No salmon are known here; the regional salmon rules still apply").
- C: hide the block on waters with no known salmon, with the rule still reachable from the Sources.

B and C need a presence source decided in the pipeline, not in the app (candidates: drainage — the Arctic watersheds;
the DFO NuSEDS / run-timing waters already in the release; the Province's fish observations). Wrong "no salmon" is worse
than a spare block, so the gate must be conservative and reviewed.

## Acceptance

The owner's pick is recorded; for B or C: the presence list is in the bundle with its source, Dease Lake reads as
picked, and a salmon water (the Skeena) is unchanged, each pinned by a test.

Depends: the owner's answer. Size: M (B) / M–L (C). Model: Opus (data). Owner: yes.

## Owner

- [x] 2026-11-26 — a water with no salmon (Dease Lake, Arctic drainage) leads with "Salmon · DFO · Open today".
  A: leave it. B: move the block below game fish with a "no salmon known here" line. C: hide it there.
  → "Leave it as it is (Recommended)" (2026-11-26)
