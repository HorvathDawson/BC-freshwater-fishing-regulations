# T855 — The Salmon map draws noticeably slower than the Trout map (0.8.0)

Status: OPEN (app chat; 0.8.1).

Owner 2026-11-18 ~14:20 (to the main chat, relayed), verbatim: "so we fixed the salmon painting? right now on 0.8.0 it
seems way slower then trout painting?"

Not fixed yet. T848 (web zoom-out lag: MapLibre re-evaluating the stream line width on every feature-state change) is the
Trout map's case and is being built for 0.8.1; nobody has measured the Salmon map. Work: time the Salmon map against the
Trout map on the same view (staging web in Chrome, and the Android emulator on 0.8.0): first colour after the switch, pan
and zoom frames. Find what the Salmon paint does that the Trout paint does not (the salmon slices decoded on demand, the
`sp` feature-state, the issue stripe layers, the gear/closed layers per mode) and fix it, or carry it on T848's layer
split if it is the same cause. Proof: the same trace before and after, Salmon within reach of Trout.
