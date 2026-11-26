# T1134 — App Store product page: the Header and Search Results pictures

Status: IN PROGRESS — the owner uploaded the four files 2026-11-25; PR #256 waits on its checks (the ffprobe fix)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1134 | App Store product page: the Header and Search Results pictures | IN PROGRESS — the owner uploaded the four files 2026-11-25; PR #256 waits on its checks (the ffprobe fix) | | PR #256 to main (repo-move chat); the API slot name APP_IPHONE_61 is unconfirmed |

Source: owner 2026-11-25, in the iOS chat's App Store walk-through at the version page (relayed): "i need search
and header asset too". Related: T1072 (the picture set, direction A), T1071 (the submission), T1131 (the pictures
by workflow: these two join its list).

What the iOS chat read on the App Store Connect screen (not checked against Apple's help page by the planning chat;
whoever builds this reads the current spec first and writes its link here): two optional product page pictures, a
"Header" (3840 x 1646 PNG) and a "Search Results" picture (3:2, up to 3840 x 2560), or one universal 5244 x 2950
PNG that serves both. Apple calls them optional.

## Work
1. Read Apple's current spec (sizes, safe areas, what text is allowed, where each is shown) and write it here.
2. Two or three looks in direction A as pictures; the owner picks. Words on them come from the one listing source
   (`app/store-listing/`), no new claims.
3. The store-shots tool makes them (a scene per asset), with a pixel-size check, and the push list (T1127) and the
   App Store push script know the two slots, or the upload steps say "by hand" if the API has no slot yet.
4. Hand the files to the iOS chat; it adds the slots to the submission page.

Acceptance: both pictures at Apple's exact sizes on main, made by the tool, uploaded by the owner. Severity: low
(optional slots; a better product page). Release: with the 0.10.0 App Store submission if ready before the build
lands (about 18:00), else added to the live page after. Model: Sonnet. Size: S. Owner: yes (picks the look; uploads).

## 2026-11-25 afternoon: uploaded; the PR is left to finish
- The store content chat was closed by the owner ("i am killing thee image generation chat now", relayed).
- Owner, 2026-11-25 (planning chat): "yes i uploaded them to the ios" (the Header and Search Results stills and the two
  8 s videos; 3840 x 1646 and 3840 x 2560, no alpha; videos 30 fps H.264, checked by the iOS chat).
- PR #256 failed CI for a real reason: `app/tools/store-shots/creative.test.ts` called ffprobe at
  `/opt/homebrew/bin/ffprobe`, which a runner does not have. The repo-move chat fixed it on the owner's yes
  ("yes it can fix ti"), commit 3192f957b: the two video checks skip by name on a runner without ffprobe, are
  listed in the no-silent-skips guard, and fail under the strict full-test switch.
- Unconfirmed, kept from the closed chat: the push script's slot name for the 6.3" class (`APP_IPHONE_61`) and the
  new `listing.json` line "BC fishing rules and conditions, by the water".
- PR #238 (T1072, the native 6.3" set) is open with auto-merge off; frame 3's re-crop was not confirmed finished.
  The owner looks at frame 3, then merge or redo; T1131's workflow regenerates these sets anyway.
