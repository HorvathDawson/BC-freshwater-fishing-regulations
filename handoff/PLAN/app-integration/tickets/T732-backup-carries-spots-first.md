# T732 — The phone's backup carries saved spots first, photos second, never the downloaded release

Status: IN PROGRESS (iOS chat; proven on the emulator 2026-11-14 with spots, settings and smaller photo copies, branch `android/t732-backup` head 56b6ea539, results in the ticket on that branch; not merged; OWNER ANSWERED 2026-11-16 11:57: 1280 px copies, and he asks for stronger compression, see the last section).

Source: audit finding G14 (`handoff/PLAN/ios/parity-audit.md`, pass 2, 2026-11-14): on Android, saved spots and photos
are never in the Google backup, because the installed release (~480 MB) puts the app over Auto Backup's 25 MB limit and
Android then skips the whole backup. Owner 2026-11-14, as relayed by the Android chat (the iOS chat did not hear it
first hand; to be confirmed with the owner on return), verbatim: "we should have it save spots with prioirty" and
"images can be compressed if still visible". The Android chat's reading: spots (place, name, notes, parking point, visit
log) always first; photos second and may be stored / backed up compressed while they still look fine; the release,
tiles and other re-downloadable data excluded from backup, as on iOS (T722, F6).

## Work
1. Prove the gap on the Android emulator: what the backup includes today and its size (`bmgr` / a backup dry run).
2. Android backup rules through the Expo config (full-backup-content and data-extraction-rules): exclude the release,
   tiles and caches; include the spots data.
3. Photos: Auto Backup skips EVERYTHING when the set passes 25 MB, so photos cannot simply be "included". Design so
   spots are never the part dropped and nothing is lost silently: say in the app or here what is and is not backed up.
4. Photo compression: propose size and quality, one before/after pair; existing photos must not be degraded visibly.
5. iOS: the same priority holds there by construction once T722 excludes the release (iCloud backup has no 25 MB cap);
   say so with evidence.
6. Check the privacy page and `handoff/PLAN/play/data-safety.md` still hold; the Android chat owns both files today:
   report sentences that need changing, do not edit them.

## Acceptance
A backup dry run on the emulator that carries the spots data and stays under 25 MB with the release installed; a restore
onto a wiped install brings the spots back; what happens to photos past the limit is stated and shown; the photo pair
looked at by the owner. Merge only after the owner's look, the Android bench, and a message to the Android chat.

Depends: T722 (iOS backup exclusion, sibling). Size: M. Owner: yes (look at the photo pair and the "what is backed up"
wording; confirm the relayed words). Default: build as a proposal on the branch; timing before closed testing only if it
lands clean and proven, otherwise first thing after (the Android chat tells the owner).

## Results
(filled in as the work runs)

## Owner's answer, 2026-11-16 11:57 (question dialog in the main chat)
Asked the three picks together (screen lock: built NO; copy size 1600 px, about 54 fit, or 1280 px, about 81 fit; new
photos stored at 2048 px, proposed YES), he typed: "1280 is probably enough also could we compress as jpeg or
something which makes the size a lot smaller? ". Readings (the main chat's and mine): copies at 1280 px; no
screen-lock rule and 2048 px for new photos stand as proposed (he did not object).
On his question: the copies are already JPEG (quality 70; 0.37 MB at 1600 px, measured; about 0.25 MB at 1280 px
from the "81 fit" figure). To do before changing the quality: measure JPEG 60 and WebP at 1280 px on the sample photo
and show him one before / after pair; my estimate is 0.17 to 0.20 MB (about 100 to 115 photos), not measured.

## Measured for his question, 2026-11-16 12:57 (the app chat; the comparison picture went to him in chat at 12:59)
The same stand-in photo, copies at 1280×960 (`sips` for JPEG, `cwebp` for WebP; a phone's encoder will differ a little):
JPEG 70 = 245,946 B (about 81 fit in 20 MB); JPEG 60 = 179,946 B (about 110); JPEG 50 = 134,821 B (about 148);
WebP 75 = 81,444 B (about 245); WebP 60 = 65,438 B; WebP 50 = 56,386 B. At full size the app chat sees no difference
between JPEG 70, JPEG 60 and WebP 75 on this photo. Files: scratchpad `t732-pair/` of session 29ad1860 (regenerate
with the two tools). ASKED, NOT ANSWERED: B (JPEG 70), C (JPEG 60) or D (WebP 75); recommended D if a restore on
the emulator shows WebP copies (the copies are Android-only; `expo-image-manipulator` has `SaveFormat.WEBP`; the
copy's file name ends `.jpg` today, so the name or the restore mapping in `photoStore.native.ts` needs a look),
otherwise C. To-Do row `t732-backup-words-confirm` carries the question. Nothing is changed on the branch yet: the
built code is on `t732-proof` @ 56b6ea539 (`app/packages/data/src/spots/backupSet.native.ts`, 1600 px, JPEG 0.7);
`android/t732-backup` is now at fa562f0d6 (the rules commit only), so merge from `t732-proof`.
