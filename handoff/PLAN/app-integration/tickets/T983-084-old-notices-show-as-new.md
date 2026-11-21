# T983 — After updating to 0.8.4, 7 expired in-season notices show as "New" (dated Nov 19)

Status: OPEN — urgent: every phone that updates may see it.

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
