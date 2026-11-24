# T855 — The Salmon map draws noticeably slower than the Trout map (0.8.0)

Status: DONE — 4c005321e, 309c05303. same cause as T848; the stream-heavy layer split takes the Salmon zoom-out from ~2.5 s to ~0.5 s and pan from ~1.1 s to ~285 ms.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T855 | The Salmon map draws noticeably slower than the Trout map on 0.8.0 (owner) | DONE | 4c005321e, 309c05303 | — |

Closed by the 2026-11-20 audit: measured and fixed by T848's 4c005321e (merged, 0.8.1; production 0.8.3); ticket records before/after on the same trace.

Owner 2026-11-18 ~14:20 (to the main chat, relayed), verbatim: "so we fixed the salmon painting? right now on 0.8.0 it
seems way slower then trout painting?"

Not fixed yet. T848 (web zoom-out lag: MapLibre re-evaluating the stream line width on every feature-state change) is the
Trout map's case and is being built for 0.8.1; nobody has measured the Salmon map. Work: time the Salmon map against the
Trout map on the same view (staging web in Chrome, and the Android emulator on 0.8.0): first colour after the switch, pan
and zoom frames. Find what the Salmon paint does that the Trout paint does not (the salmon slices decoded on demand, the
`sp` feature-state, the issue stripe layers, the gear/closed layers per mode) and fix it, or carry it on T848's layer
split if it is the same cause. Proof: the same trace before and after, Salmon within reach of Trout.

2026-11-18 (lane A, branch worktree-agent-a1e41c47a10b4ccef 4c005321e, with T848/T832): same cause as T848, worse. On
Salmon the stream width expression is 21 KB (long closed-to-salmon label lists), re-evaluated on every feature-state
change (1.67 s in `In.evaluate` on one zoom-out). The T848 layer split fixes it: Intata z12->z7 at 4x CPU, Salmon zoom-out
16-19 long tasks / 2.4-2.6 s -> 6 / ~525 ms; Salmon pan 600 px 1.0-1.2 s -> ~285 ms; 0 map pixels changed. Waiting on the
independent review, then the 0.8.1 merge.
