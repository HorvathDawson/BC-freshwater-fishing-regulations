# T172 — Android smoke: show the app builds, installs and runs on the emulator (before cleanup)

Status: OPEN — next heavy job after the current batch merges; before the cleanup band.

Source: owner 2026-10-23: "maybe do a simple android emulation before cleanup but do full emulator work and comparison
after cleanup. that way we show it at least works before". The full parity walk and system-bar work stay T50 (after
cleanup).

## Setup (measured 2026-10-23)
Android SDK at ~/Library/Android/sdk (build-tools, cmake, cmdline-tools, emulator, ndk, platform-tools, platforms,
system-images); AVD `n1_pixel` (from the N1 spike, handoff/PLAN/app-data/N1-SPIKE.md); 48 GB RAM. adb/emulator are not
on PATH (use $HOME/Library/Android/sdk/{platform-tools,emulator}); no JAVA_HOME set — use Android Studio's bundled JBR
(/Applications/Android Studio.app/Contents/jbr/Contents/Home) or say what is missing.

## Work (smoke only)
1. Boot `n1_pixel` headless (`-no-window` if it runs; else windowed) via scripts/heavy.sh, ALONE (no reach run, app check
   or other emulator at the same time).
2. A development build of app/apps/mobile (`expo prebuild` + `expo run:android`, or the documented script) installed on
   the emulator; data from the local servers (the emulator reaches the Mac at 10.0.2.2:39217; never stop :39217/:39218).
3. Smoke path, a screenshot of each (adb screencap): the app opens; Map tab draws tiles; tap a water → its regulations
   page (Fraser part); Conditions map with gauge colours; Spots tab; More tab. Note crashes, red screens, blank maps and
   any system bar covering UI — do NOT fix parity here (that is T50); only fix a blocker that stops the app opening, or
   list it.
4. Write the result into this ticket (what works, what does not, build time, the exact commands), shots under
   handoff/PLAN/app-integration/T172-shots/; anything found becomes a line in T50.
Size: S (build time dominates). Shut the emulator down at the end (its own PID only).
