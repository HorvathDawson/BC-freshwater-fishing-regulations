# T498 — The rules / conditions pane opens in one piece

Status: OPEN — owner 2026-11-08 (phone, 0.6.9). Part of T495 (smooth map).

Tapping a stream opens the pane in a janky, flickery way: the page shows, then its content loads in. Ideal: build the
content first, then show the finished pane.

## Do
- Reproduce on the emulator; record what renders frame by frame (sheet open vs content arrival vs layout jumps).
- Open the sheet only once its first content is ready (or with a stable skeleton of the final size), one load path for
  every way in (rule: one load path per screen). No silent fallbacks.
