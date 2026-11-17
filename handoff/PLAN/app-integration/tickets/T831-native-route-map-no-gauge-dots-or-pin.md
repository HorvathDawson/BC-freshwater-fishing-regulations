# T831 — The route map inside a water's Conditions view draws no gauge dots and no "you are here" pin on the phones

Status: OPEN (app chat; feature-sized).

Source: found by the T826 agent 2026-11-17 while checking the owner's iPhone report (T826 part 1), on both Android
and iOS; the web draws them.

The route map's key names gauge dots and the "you are here" pin, but the native map ignores `trace`
(`app/packages/map/src/Map.native.tsx:115`, `trace: _noTrace`). Build: carry the trace layers into the native map as
the web adapter does, with an adapter-contract test, and prove it on both phones (shots beside the web).
