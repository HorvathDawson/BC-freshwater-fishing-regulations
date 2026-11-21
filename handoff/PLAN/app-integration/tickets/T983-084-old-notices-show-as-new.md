# T983 — After updating to 0.8.4, 7 expired in-season notices show as "New" (dated Nov 19)

Status: IN PROGRESS — fixed on lane/085-t983, owner picks built (the 7 keep showing; no auto-repair; the inbox row's date
is the notice's own effective date, else "first seen Nov 19": core `rowDate` / `INBOX_WORDS.firstSeen`, whole-string
tests); left (Claude): merge + combined review.

## Measured (2026-11-21, against the live feeds data.canifishthis.ca + data-staging, and git)
1. **Ids did not change.** The 32 rows carry the same ids in production and staging (content hashes, worker `rowId`,
   pinned in `workers/feeds/test/events.test.ts`); every id has its entry in the release-independent `matches.json`.
   The seen-store was not reset either: the other 25 notices stayed read.
2. **Cause: the in-force rule moved under the inbox.** 0.8.3's `inSeasonSpan` inferred an end from a printed window that
   opens on the row's `from` day; 0.8.4 has T548's 09320de47 (owner 11-20: "While the Province lists it"), which stopped.
   Replaying both rules over today's production feed: exactly 7 rows flip from ended (to 2026-09-15) to in force —
   Salmon (R3), Okanagan, Okanagan Oxbows, Similkameen, Granby, Granby's Tributaries, Bessette (R8), all effective
   "July 30, 2026". The inbox only ever entered rows in force, so these had never entered (or had been dropped 30 days
   after Sept 15); when 0.8.4 put them in force, `inboxMerge` met them as never-seen ids on a non-first read: NEW.
3. **"Nov 19" is not the notice's date**: it is the production feeds worker's `firstSeen` (its first run, 2026-11-19
   05:14 UTC; staging's rows say 2026-11-08). The notices were posted for July 30, 2026.
4. They show at all because of T548's owner ruling (shown while the Province lists them; no end invented). Not changed.

## Fix (lane/085-t983)
- `joinInSeason` also returns `ended` (rows on the page whose `to` has passed).
- The inbox history keeps `known`: every row this device met begun (in force or ended) while it is on the page. A row
  entering the history that is `known` (and not an amendment the feed links) enters as a first read does: quiet when
  matched. A row met before it began is still news the day it begins.
- A history written by 0.8.4 or earlier (no `known`) reads the feed's status once in its place: a row the feed calls
  "unchanged" was on the page before. So 0.8.3 → 0.8.5 shows none of the 7 as New.
- Pinned: `core/src/inseasonInbox.test.ts` "(T983) …" (5 cases, the real Okanagan row); without the fix 3 fail.

## Owner picks (not done here)
- Phones already on 0.8.4 hold the 7 as unread: the fix does not mark them read (a precise repair would need the
  removed window rule back). They clear with "Mark all as read". Repair them in 0.8.5 or leave?
- "An ended closure never shows as current" conflicts with the 11-20 T548 pick ("While the Province lists it"). Keep it,
  or end these 7 with a curated `to: 2026-09-15` in `data/curated/inseason/notice_waters.json`?
- The inbox date for a notice is the worker's `firstSeen`; showing the notice's own effective date instead is a choice.

Source: owner 2026-11-21, Android, right after installing 0.8.4 from Play Internal: "I just installed 0.8.4 why did all
these messages show up?". Screenshot: `T983-shots/android-084-notices-all-new.png`. The bell shows 7. The notices
are Salmon River (R3), Okanagan River, Granby River's Tributaries, Similkameen River, Okanagan River Oxbows and Granby
River (R8), all "New · Nov 19" and all "No fishing July 30 to September 15" (summer closures, ended).

## Questions to answer first (measure; don't guess)
1. Did the notices' ids change between r-6258ffded676 and r-fec3fab4c5aa (L7 moved in-season resolution to
   `data/generated/inseason/resolved.json`, and the matches were re-resolved)? If so, the inbox's read state no longer
   matches them, and they come back as new on every upgrading phone.
2. Why are notices whose closure ended in September in the inbox at all (T548: in force and fresh)? Is "Nov 19" the
   notice's own date or our fetch or resolve date?
3. Did the update keep the inbox state, or was it a fresh install?

## Work
- Notice identity must be stable across releases: the source notice's own id, not something the resolution derives.
  Pin it with a test that re-resolves and keeps the ids.
- An ended notice is not listed as New (or not listed at all; check T548's rule).
- If ids did change in production data, decide with the owner whether a data fix ships before 0.8.4 reaches more
  phones.

## Acceptance
Updating 0.8.3 → 0.8.4 (or any release → the next) shows no old notice as New. An ended closure never shows as current.
Both are pinned by tests.

Depends: —. Size: M. Owner: yes (whether to hot-fix the data).
