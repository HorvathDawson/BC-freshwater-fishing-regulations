# T340 — Test bed regs-open: stream taps fail (known since 0.3.0)

Status: OPEN (loose end turned into a ticket, 2026-10-31).

The emulator `regs-open` scenario's STREAM taps fail on 0.3.0 and 0.4.0 alike (lake taps pass: 247/78/53 ms). A
known test issue nobody ticketed: find why (tap point off the line? zoom?), fix the scenario so stream opens are
timed too. Size: S.
