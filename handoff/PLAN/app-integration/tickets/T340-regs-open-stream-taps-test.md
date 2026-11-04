# T340 — Test bed regs-open: stream taps fail (known since 0.3.0)

Status: DONE (2c96522; out/r050b regs-open 13/13). Closing search puts the camera back where it was (T48), so the scenario tapped where the creek had been framed; it now opens the water from its result and goes Back (the map comes back framed on it).

The emulator `regs-open` scenario's STREAM taps fail on 0.3.0 and 0.4.0 alike (lake taps pass: 247/78/53 ms). A
known test issue nobody ticketed: find why (tap point off the line? zoom?), fix the scenario so stream opens are
timed too. Size: S.

## 2026-11-04 emulator run (app/tools/testbed/out/b5-before)
Both stream cases fail now (small AND fraser; lake 3/3 passes). Shot 006: after "Norrish Creek" the camera is on
Agassiz/Chilliwack, not on the creek near Dewdney, and the logged `[target]` (441,1136 px) sits beside a line, not on
it — the target is taken for a frame the camera is not showing (or the stream's target point is off its line). Next:
log the `[target]`'s sid + camera beside the shot, and check `findTarget` for streams (lakes answer by polygon).
