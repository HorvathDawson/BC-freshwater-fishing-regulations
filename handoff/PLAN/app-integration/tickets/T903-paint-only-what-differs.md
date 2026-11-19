# T903 — Map modes paint only what differs from a default (Salmon red, Conditions/Runs grey)

Status: OPEN — stressed bench done 2026-11-19 03:51: not a clear win (switches into Salmon faster, pans slower); next chat (branch app/t903-paint-default, prototype behind map-lab switches, not merged).

Owner 2026-11-19 ~01:33 (app chat), verbatim: "i feel like salmon painting is still slow. maybe it should default stream
color to red then only paint things different then that? might reduce the load when on salmon view? then goes back to
default for normal rules after to keep it fast too. similar default grey for conditions and runs?"

Idea: today a mode switch writes a per-feature colour (feature-state) for every drawn water. Instead, each mode's layer
paint gets a DEFAULT (Salmon: the most common answer, likely closed red; Conditions / Runs: grey / no data), and
feature-state is written only for the waters whose answer differs; switching back to Trout clears only those. Fewer
writes per switch and per tile load. Prove first: count each mode's states over the live bundle (how many differ from
the default), then bench mode switch + pinch/pan on Salmon, Conditions and Runs (testbed zoom-pinch, a mode-switch
scenario) before and after on the emulator, patched MapLibre. Lands only if it clearly wins; otherwise the numbers go in
this ticket for the next chat. Related: T848, T855, T861, T420.

## 2026-11-19 ~02:40: measured — no clear win (branch app/t903-paint-default, b66b1e58d, not merged)

**Today's write path.** The writes do not depend on the mode. Map tab: the status (`p`), salmon (`sp`, the section's
CHAIN, not its colour), method (`gm`) and issue (`gi`) labels go on every section in ONE feature-state call per section
(`colouring.ts` `Colouring.label`), whatever Trout/Salmon says. The day's colours live in the style (`salmonDayProfiles`
lists). On the phone, every mode or tab switch is a STYLE SWAP that drops all feature-state, so the engine relabels every
held tile (`native/useColouring.ts` `swapped`). Conditions/Runs already work the owner's way: only sections with a value
get a key (`standing`, `rt`), and the rest draw the grey `missing`.

**Counts** (r-fc137c4040fa, 2026-11-19, 1,958,586 sections):
- Salmon: 1,505,511 sections closed (77 %), 453,067 open, 8 check. By chain label: 1,095 chains = 920 closed, 170 open,
  5 check. The largest chain covers 448,636 sections.
- Trout (`p`): base 1,328,246, own 568,269, closed 57,167, no access 3,597, none 1,307. 116 profiles.
- Conditions: 141,189 sections have a standing class (7 %); the other 93 % are never written.
- Runs: ~435k sections carry a run profile (22 %, older atlas's feed); the rest are never written.
- Writes saved by never writing a channel's most common label (`p`=b, `sp`=top chain, `gm`=m): 1,958,586 → 1,763,544
  per full relabel (−10 %). The keys ride together, so a default helps only where ALL of them are default.

**Where the Salmon cost really is: the label LISTS, not the writes.** The style tests labels with
`["in", label, ["literal", [...]]]`, which is a linear scan in both MapLibres. Salmon's visible paint is 498 KB with
90 such lists (54,699 labels scanned at worst per feature), against Trout's 91 KB and 4,318. The `closed` list is 920
labels long and sits in the stream, stream-heavy, closed stripe, issue stripes, casings and gear layers.

**Prototype.** The map-lab switches are `paintDefault` and `labelMatch` (`paint-default.ts`, applied in `labStyle`, native
only). `paintDefault` writes each big set as the negation of the small rest plus `""`, and keeps the most common colour
as the match's fallback. `labelMatch` turns each test into a hashed `match`. Exactly the same look: 0 of 6.48M
evaluations differed in MapLibre's own evaluator, run on the release's real labels (`scratchpad/t903/bench.ts`). Salmon
paint: 430 → 125 KB. GL JS evaluation time: 4,465 → 2,447 (default) → 1,168 ms (match), which is Trout's level.

**Bench** (n1_pixel emulator, patched MapLibre, b66b1e58d, 3 runs each, Fraser Valley; scenarios `mode-switch` (new) and
`zoom-pinch` with `PINCH_MODE=salmon`; run `scratchpad/t903/run1`):

| | base | paintDefault | + labelMatch |
|---|---|---|---|
| tap → painted, Trout → Salmon (median ms) | 319 | 306 (−4 %) | 298 (−7 %) |
| … Salmon → Conditions | 414 | 345 (−17 %) | 337 (−19 %) |
| … Conditions → Runs | 249 | 269 | 283 |
| … Runs → Map (Salmon) | 333 | 317 (−5 %) | 319 (−4 %) |
| … Salmon → Trout | 276 | 300 | 284 |
| whole cycle | 1,591 | 1,537 (−3 %) | 1,521 (−4 %) |
| Salmon pinch ×6: fps / frame gaps >50 ms (3 runs) | 53.0 / 44 | 55.6 / 30 | 53.3 / 39 |
| Salmon pinch: render thread / map workers ms | 4,855 / 2,692 | 4,502 / 2,390 | 5,104 / 1,780 (−34 %) |
| Salmon zoom-out z12→7: fps / workers ms | 49.4 / 1,450 | 54.3 / 1,202 | 53.7 / 1,020 (−30 %) |
| Salmon pans (12 flings): frames >50 ms | 30 / 1,849 | 20 / 1,843 | 12 / 1,848 |

**Verdict: no clear win on the emulator.** The cut in worker CPU (tile layout) is real (−17 to −34 %). What a reader
sees moves little: the switch into Salmon is −4 to −7 %. Pinch gaps fall 32 % with `paintDefault` but only 11 % with
`labelMatch`, which does less work, so that drop is within the noise. On this emulator Salmon paints only ~40 ms slower
than Trout (319 vs 276). The stressed run (`TESTBED_EMU_ECORES=1 TESTBED_EMU_ARGS="-cores 2"`) did not start: the app
was re-running its first-time setup and took longer than the 90 s cold-start limit. Next chat: run that stressed bench
(the lag only showed under stress in T420) or look at a phone trace. If it wins there, land `labelMatch` in the shared
adapter (`adapters/contract.ts` `paint`, so web and phone share it) with an evaluation test like `tools/stream-heavy.test.ts`.

## 2026-11-19 ~03:55: the STRESSED bench — not a clear win either (numbers only; prototype not merged)

**The setup failure, fixed.** `session.sh` uninstalls the app at the start of every batch, so the first scenario's cold
start re-runs the first-time setup (~480 MB). Under stress that takes 143–171 s, past the scenarios' 90 s cold-start
wait. Fix: `install` first in the same batch (`run.mjs --no-r2 … install mode-switch zoom-pinch`). No code change.
One more one-off: batch 1's first traced pinch (base) failed writing its end trace marker (perfetto was not up after the
2.5 s wait), so base pinch numbers come from batch 2 only.

**Run.** Same APK (b66b1e58d, patched MapLibre, build id 7bf3dc29…), `TESTBED_EMU_ECORES=1 TESTBED_EMU_ARGS="-cores 2"`,
Fraser Valley, Salmon for the pinch. Batch 1: arms base → paintDefault → +labelMatch. Batch 2: the reverse order, cut at
03:51 to give the emulator back to the 0.8.2 benches (pinch complete; mode-switch with +labelMatch 1–2 rounds and
paintDefault 3, base none). Medians over both batches, n per cell in brackets.

| stressed | base | paintDefault | + labelMatch |
|---|---|---|---|
| tap → painted, Trout → Salmon (ms) | 1,500 [3] | 1,338 (−11 %) [6] | 1,255 (−16 %) [5] |
| … Salmon → Conditions | 1,039 | 1,035 (0 %) | 1,120 (+8 %) |
| … Conditions → Runs | 940 | 937 | 938 |
| … Runs → Map (Salmon) | 1,620 | 1,269 (−22 %) | 1,275 (−21 %) |
| … Salmon → Trout | 1,559 | 1,312 (−16 %) | 1,483 (−5 %) |
| whole cycle | 6,943 [3] | 5,880 (−15 %) [6] | 6,120 (−12 %) [4] |
| pans on Salmon (4 flings/round): frames >50 ms, p95 | 33 %, 116 ms | 38 %, 124 ms | 46 %, 154 ms |
| pans, other modes: p95 range | 101–116 ms | 89–120 ms | 105–146 ms |
| Salmon pinch ×6: fps / worst frame (median) | 3.1 / 926 ms [3] | 5.2 / 654 ms [6] | 8.0 / 457 ms [6] |
| Salmon pinch: render thread / map workers ms | 4,196 / 7,470 | 4,574 / 6,088 | 5,330 / 4,281 (−43 %) |
| Salmon zoom-out z12→7: fps / worst frame | 3.6 / 982 ms [6] | 6.7 / 559 ms [6] | 12.9 / 332 ms [6] |
| Salmon zoom-out: map workers ms | 2,778 | 2,130 | 1,643 (−41 %) |

(Pinch frame-gap COUNTS are left out: a stressed arm that draws fewer frames counts fewer gaps; fps and the worst frame
are the fair reading.)

**Verdict: not clear by the landing rule.** For it: the switches into Salmon are 11–22 % faster, the whole cycle −15 %
with `paintDefault`, the zoom gestures draw 2–3.5× the frames, and the map workers do ~40 % less. Against: the pans got
WORSE on every mode with `labelMatch` (Salmon p95 116 → 154 ms, frames >50 ms 33 → 46 %, in both batches) and a little
worse with `paintDefault` (p95 +7 %); Salmon → Conditions +8 % with `labelMatch`; the base arm has only 3 mode-switch
rounds, all run first in batch 1; and this emulator draws 3–13 fps when pinching, far below a phone, so it may not say
what a phone does. Next chat: `paintDefault` ALONE is the promising half — a balanced rerun of base vs `paintDefault`
only (base not first, ≥ 6 rounds, the pans watched), e.g. `MS_ARMS='lab:paintDefault=true;base'`, and the pan
regression explained before anything lands. Runs in the lane scratchpad (`t903/stress`, `t903/stress2`, `t903/agg.py`).

