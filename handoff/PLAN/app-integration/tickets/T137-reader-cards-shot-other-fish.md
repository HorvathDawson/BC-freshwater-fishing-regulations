# T137 — The reader shooter's TALL cards shot drops the Other fish list

Status: OPEN — source: reader gate 2026-10-23 (handoff/PLAN/app-integration/reader/2026-10-23-app.md), the one miss (Q12).

## Today
`app/tools/compare-regs.mjs` `tall()` clips a `cards` shot at the bottom of the last `[data-testid=keep-group]` /
`[data-testid=keep-plain]` block. The Keep tab's Other fish list (`keep-otherfish`, KeepBlocks.tsx `OtherFish`) comes
after it, so every TALL `*-cards.png` ends at "See every regulation line ›" with no Kokanee / Whitefish / ... rows. The
prototype's `cards` shots (tall.mjs; r20i gate-after/ur1-kootenay-lwa-oct10-cards.png) end with the Other fish list,
and prompt-gate39 Q12 (kokanee on Sat Oct 10 / Mon Oct 12 at the Kootenay LWA) is answered from it. On the app set the
reader answered "not shown" from the named cards, conf 5 -> 3: the gate fails (38/39) on the harness, not the app.

## Work
- `cards` mode ends at the foot of the Other fish list when the Keep tab has one (include `keep-otherfish` in the end
  measure), matching the prototype's cards shots; `keep` mode unchanged.
- Re-shoot the reader set (`node app/tools/compare-regs.mjs --reader OUT_DIR`) and run one fresh Opus reader on it with
  prompt-gate39.md (owner Q13: Claude runs it); file the result as a new dated file in `reader/`.

## Acceptance
- ur1-kootenay-lwa-oct10-cards.png shows "Kokanee · Weekends: keep 5 · Weekdays: release" with "5 a day";
  ur2-kootenay-lwa-oct12-cards.png shows the Monday kokanee row (release); every other `cards` shot ends with its Other
  fish list where the water has one (Bella Coola May 1: Kokanee release, Whitefish 15 a day, ...).
- A test on the shooter's end rule (cards end = max bottom of keep-group / keep-plain / keep-otherfish).
- Reader gate re-run: 39/39, Q12 answered from the cards (Sat 5, Mon release) at conf >= 4, no other drop >= 2.

Depends: none (T41's tool). Size: S + a reader run. Owner: Claude (Q13).
