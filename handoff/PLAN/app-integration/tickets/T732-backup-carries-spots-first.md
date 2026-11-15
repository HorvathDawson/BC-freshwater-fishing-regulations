# T732 — The phone's backup carries saved spots first, photos second, never the downloaded release

Status: IN PROGRESS (iOS chat; proven on the emulator 2026-11-14, branch `android/t732-backup` head c95ea31c5, results in the ticket on that branch; not merged: a proposal that waits for the owner's look and three picks).

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
