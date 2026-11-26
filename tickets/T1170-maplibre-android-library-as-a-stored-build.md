# T1170 — The pinned MapLibre Android library becomes a stored build: built once per pin on GitHub, kept with its checksum, downloaded by every build

Status: OPEN — the speed work (owner 2026-11-25); prerequisite for Android builds off the Mac (T1133)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1170 | The pinned MapLibre Android library becomes a stored build: built once per pin on GitHub, kept with its checksum, downloaded by every build | OPEN — the speed work (owner 2026-11-25); prerequisite for Android builds off the Mac (T1133) | — | a build lane: a workflow keyed by pin commit + build settings that builds only when the stored file is missing; a GitHub Release asset + sha256 in the repo; the Mac build script downloads and verifies; S–M |

Source: owner 2026-11-25 night (planning chat), verbatim: "i thought we use maplibre main pegged library? couldnt
we put this into an artifact or something on github so it can use that build then we can move it? or onto r2? or
gha could rebuild it if it doesnt exist in the artifacts already". From the phone-build deep look (notes: `tickets/archive/night-notes-2026-11-25.tar.gz`
`phone-builds-deep-look.md`) and the ship-workflow design: the Android app uses MapLibre Native built from source
at a pinned upstream main commit (`app/tools/maplibre-android/`; upstream publishes no library for an arbitrary
commit); it is built once per pin and the ONLY copy is on the owner's Mac. Related: T1133, T1063, T1120, the
memory note `maplibre-patched-always`.

## Work
1. A key: the pinned commit + the build settings (and the toolchain versions that change the output).
2. A hand-started and pin-change-started workflow: if a stored library for the key exists, do nothing; else build
   it from the pinned commit, store it, record its sha256. Time the first run (not measured yet).
3. Store: a GitHub Release asset (permanent, free, public like the repo, fetchable from a fresh clone); R2 is the
   alternative if the owner prefers it. NOT an Actions artifact (expires in at most 90 days) and not the Actions
   cache (evicted after about a week unused).
4. The Android build script (Mac today, a runner later) downloads the file and verifies the checksum pinned in the
   repo; a mismatch or a missing file is a shown stop, never a silent local rebuild. The Mac's existing copy is
   compared with the CI-built one once (same pin: are the bytes the same; if not, say why).

## Acceptance
A fresh clone builds the Android app without compiling MapLibre; moving the pin triggers exactly one rebuild; a
tampered file is refused (test). Severity: enabling + removes a single-copy risk. Model: Opus. Size: S–M. Owner:
the store pick below.

## Owner

- [x] 2026-11-25 — Where the built library is kept: a GitHub Release file (default) or R2. → "Yes to all four" (2026-11-26: a public GitHub Release file)
- [x] 2026-11-26 — The four speed items (the web-only release path and the data floor check, T1165; the pre-cut gate, T1159; the first steps of T1133; the public GitHub Release file for the MapLibre library, T1170) → "Yes to all four" (2026-11-26; also "Yes to all four; relay it", 2026-11-26 01:54)
