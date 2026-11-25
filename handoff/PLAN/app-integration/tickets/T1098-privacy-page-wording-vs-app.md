# T1098 — The privacy page says things the app does not do (Apple reads the policy at App Review)

Status: OPEN.

Source: iOS release chat 2026-11-24 ~23:40, its read of build 0.9.0 (45) at 65f84020c for T1071 (App Privacy and review risk). The page is built by `app/tools/seo/html.ts` (lines 219–305). None of it blocks review on
its own, but the policy URL is part of the submission, so it should be true before the owner submits.

## What differs (line numbers in `app/tools/seo/html.ts`)
a. :298 crash reports hold "only the random install id, which I cannot connect to a person". A problem report
   stores an email and the same install id in one record (`app/workers/feeds/src/reports.ts`, kept 180 days), so
   the two can be connected.
b. :262 "Directions opens Google Maps": the iPhone opens Apple Maps (`app/packages/core/src/spotWords.ts`:154).
c. :248 downloads "carry the app's version": the User-Agent also carries the OS name and version
   (`app/packages/core/src/appVersion.ts`:88–91).
d. :288 names a "New app version" notice the App Store build does not have
   (`app/apps/mobile/src/notify.native.ts`:52–53).
e. The background wake can start a release download, not only a check (`app/packages/data/src/notify/wake.ts`:2–8).
f. The App Store version lookup (itunes.apple.com, expo-in-app-updates) is not mentioned.
g. Not said at all: the push token is registered even when the reader taps Don't Allow on the notification prompt
   (state written before the ask, `app/packages/ui/src/notify.ts`:147–153; silent pushes need no permission), and a
   stored token has no age limit (`app/workers/feeds/src/push.ts`).

## Work
1. Rewrite the page so each sentence is true on Android, iPhone and web (say the platform where they differ). Every
   changed sentence goes to the owner as a before/after list: it is his legal statement.
2. (a) depends on an owner decision the iOS chat is asking: keep the install id on a report that carries an email
   (then say so plainly), or leave it out (then the sentence is true as written). Record his pick here.
3. (g) is behaviour, not only words: propose to the owner either saying it, or not registering a token after
   Don't Allow, and a token age limit. A behaviour change is its own ticket.
4. A test that pins the page's claims to the code where it cheaply can (the map app named per platform, the
   headers sent), so the page cannot drift again unnoticed.
5. The site deploy to production is the owner's (CONFIRM_PROD).

## Acceptance
The owner has read the before/after list; the page on production matches the app; the App Store listing's privacy
URL shows it before the submission.

Severity: medium (a public legal statement that is partly untrue; Apple reads it). Release: before the App Store
submission (T1071): a site-only change, with 0.9.1's site deploy or its own. Model: Sonnet (Opus not needed: the
code facts are listed). Size: S. Owner: yes (reads the new words; item 2 and 3 picks; the production deploy).
