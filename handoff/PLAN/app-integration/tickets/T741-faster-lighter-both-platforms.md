# T741 — Faster and lighter on iPhone and Android: a standing optimisation pass

Status: IN PROGRESS (iOS chat; survey first, fixes on branches).

Source: owner 2026-11-14 (evening), verbatim: "can you make sure to keep working on optimizations? make app faster and
lighter for both ios and android".

## What this covers
- **Faster:** cold start, the first install (download, checking, first map), a tap to a water's page, tab and page
  changes, search, map pan and zoom, the rule book.
- **Lighter:** memory in use (per screen and at the peak), the app's size in the store (APK / AAB, IPA), the data
  download (479 MB today), storage on the phone, battery and data used in the background.

## Rules for this ticket
- Measure first, then change. Every item names its number before and after, on the same device and build type, and
  says when a comparison is not like for like.
- One change per commit, on a branch (`perf/...`), never straight on `redesign/stream-sections`. A change to shared code
  runs the Android benches (`regs-open`, `tap-stalls`) and the iOS twins before it is offered for merge.
- No change to what a reader sees or to data content. A saving that drops, simplifies or re-encodes with loss
  anything shown (tile detail, zoom levels, photo quality, text) is a **proposal for the owner**, never done unasked.
- Existing work is built on, not redone: T360 (Android memory), T686 (data savings, ranked; owner pick pending), T716
  and T719 (tap to page), T717 and T740 (map kept under a page: memory), T720 (stalls), T722 (install), T725 (iOS
  memory), T737 (prefetch), T738 (first-install bar), T272 (hash while downloading), T420 and T492 (MapLibre).
- Timed runs only on a quiet machine (no other heavy job), with the load average written down.

## Known starting numbers (2026-11-14)
| measure | iPhone simulator | Android emulator | source |
|---|---|---|---|
| cold start to map | about 0.6 s | not re-read today | T725 |
| tap → page shown | 374 to 553 ms | 331 to 586 ms | T719, T716 |
| worst JS stall | 227 ms (cold start) | under 300 ms | T720 |
| memory on a water's page | 279 MB (was 213 to 230 before T717) | 332 to 411 MB | T725, T740 |
| peak memory | 389 MB (rule book over a page) | 379 MB (11-09, old APK) | T725, T360 |
| data download | 479 MB (basemap 225, atlas 197, rule book 12, rest 45) | same files | T718 |
| after the last byte, checking | 44 s | not read | T718 |
| app size in the store | not measured | not measured | — |

## Work
1. Survey (no device time): what is in the APK / AAB and the IPA by size; the JS bundle by module; what the cold
   start loads before the first frame; what holds memory per screen (from the existing tables and the code); the
   first-install timeline. Output: a ranked list here, each row with its measured or estimated saving, its risk, and
   whether it changes anything a reader sees.
2. Fixes, best ratio first, each on a branch with its before and after.
3. Proposals for the owner for anything lossy or visible.

## Results
(filled as items land)
