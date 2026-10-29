# T257 shots — before / after pixel diff

before = static web export of 2dfe54f3f^ (no T257), after = 8f196181b (T257); both built in their own worktrees and served
side by side, shot by `app/tools/polish-shots.mjs` (headless Chrome, reduced motion, placeholder pinned), desktop
1280×800 @1x and phone 390×844 @2x, light + dark. Pixel = any channel sum difference > 24.

The tile/data server's release changed under both old builds during the session (the map draws blank in both; the
Vedder start link shows the regulations-unavailable state in both), so the map canvas is equal in every pair. That
state put the migrated ErrorState + "Try again" link on screen: regs-* are 0.00 % — the extraction is pixel-identical.
Busy / spinner states are timing-dependent and are held by render tests, not shots.

Expected differences (the two redesigned surfaces): firstrun-* (the new notice) and tour-* (cards → coach marks).
Every migrated screen otherwise: 0.00 % (more-phone-light 0.01 %: a few anti-aliased pixels). Nothing over threshold
that is not one of the two redesigns. `after/tour2..5-*` are the later tour steps (no before).
`live-tour-*` are from the live dev tree with the run-timing lane's Runs tab in flight (6 steps) and the spotlight
clamp (75b2a70b5) that keeps a foot tab's ring on screen.

| shot | pixels differing |
|---|---|
| conditions-desktop-dark.png | 0.00% |
| conditions-desktop-light.png | 0.00% |
| conditions-phone-dark.png | 0.00% |
| conditions-phone-light.png | 0.00% |
| firstrun-desktop-dark.png | 14.74% |
| firstrun-desktop-light.png | 23.51% |
| firstrun-phone-dark.png | 35.95% |
| firstrun-phone-light.png | 53.10% |
| help-desktop-dark.png | 0.00% |
| help-desktop-light.png | 0.00% |
| help-phone-dark.png | 0.00% |
| help-phone-light.png | 0.00% |
| layers-desktop-dark.png | 0.00% |
| layers-desktop-light.png | 0.00% |
| layers-phone-dark.png | 0.00% |
| layers-phone-light.png | 0.00% |
| map-desktop-dark.png | 0.00% |
| map-desktop-light.png | 0.00% |
| map-phone-dark.png | 0.00% |
| map-phone-light.png | 0.00% |
| more-desktop-dark.png | 0.00% |
| more-desktop-light.png | 0.00% |
| more-phone-dark.png | 0.00% |
| more-phone-light.png | 0.01% |
| regs-desktop-dark.png | 0.00% |
| regs-desktop-light.png | 0.00% |
| regs-phone-dark.png | 0.00% |
| regs-phone-light.png | 0.00% |
| spots-desktop-dark.png | 0.00% |
| spots-desktop-light.png | 0.00% |
| spots-phone-dark.png | 0.00% |
| spots-phone-light.png | 0.00% |
| tour-desktop-dark.png | 6.26% |
| tour-desktop-light.png | 29.97% |
| tour-phone-dark.png | 22.85% |
| tour-phone-light.png | 75.03% |
