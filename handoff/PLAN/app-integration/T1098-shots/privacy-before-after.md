# T1098: the privacy page, before and after (owner reads this first)

Page source: `app/tools/seo/html.ts` (`privacyBody`), URL `/privacy`. Words only; the app does not change.
Pick on item a (owner 2026-11-24, "Strict: linked"): the install id stays on a report that carries an email.
Each row was checked against the code first; the ticket was right about a to f. Item g: owner's answer 2026-11-25 (bottom).
Two rows (8, 9) go beyond the ticket's list; they are marked. Em dashes: none added.

| # | Item | Section | Before | After | Code fact |
|---|------|---------|--------|-------|-----------|
| 1 | c | Downloads from my servers | Each request carries the app's version (so I can tell old versions from new) | Each request carries the app's version and, from the phone app, your phone's system name and version (so I can tell old versions from new) | `versionHeaders` builds `canifishthis/<tag> (<platform>)`; the phone's platform is `ios 18.1` / `android 34` (`platformLine.native.ts`); the web sends only `X-App-Version` |
| 2 | f | Downloads from my servers (new last sentence) | (nothing) | Looking for a newer version of the app itself goes through the store it came from: on an iPhone from the App Store, the app asks Apple's App Store which version is current, and Apple sees that request and your IP address as any website would; on Android from Google Play, the check is Google Play's own. | `expo-in-app-updates` `ios/AppStoreLookup.swift` calls `itunes.apple.com/lookup`; `appUpdater.native.ts` channels play / app-store. The Google Play half is mine (not in the ticket): the Play check was also unsaid. |
| 3 | b | Sharing and directions | Directions opens Google Maps with a spot's parking point. | Directions opens Apple Maps on an iPhone, and Google Maps on Android and the web, with a spot's parking point. | `directionsUrl` in `spotWords.ts` |
| 4 | a | Reporting a problem, email bullet | your email address, only if you type one. It is used only to reply, and only if you tick "You may contact me". | ... "You may contact me". It is stored in the same record as the install id, so a report with an email can be connected to you (and, through the same install id, to the error reports that install sent). | `workers/feeds/src/reports.ts`: email + install id in one record, 180 days |
| 5 | a | App errors, install id bullet | ... the same id goes with a problem report you send. Uninstalling ... | ... the same id goes with a problem report you send, and if you type an email address in that report it is stored with the id in one record (see Reporting a problem). Uninstalling ... | same |
| 6 | a | Deleting your data | Crash reports hold no name, email or place, only the random install id, which I cannot connect to a person; they are deleted automatically 90 days ... | Crash reports hold no name, email or place, only the random install id, which on its own I cannot connect to a person. The one way it can be connected is a problem report you send with your email address: that report holds the email and the same install id in one record. Crash reports are deleted automatically 90 days ... | same |
| 7 | d | Push token, first sentence | ... (a gauge alert, "New regulations data" or "New app version"), the app sends ... | ... (a gauge alert or "New regulations data"), the app sends ... | `notify.native.ts`: `VERSION_FILE` is null for the App Store build and for an iPhone with no channel, so `appVersion: VERSION_FILE !== null` is false and the switch is not offered |
| 8 | e | Push token | ... a silent signal telling the app to check for new data; the signal carries no words ... | ... a silent signal telling the app to check for new data and, when a new data release is out and your download setting allows it, to start downloading it; the signal carries no words ... | `wake.ts`: the wake runs the update service check with `release: true` and can start the download |
| 9 | f | No selling, no sharing | (Cloudflare, Open-Meteo, Esri, and Apple for push only) | (Cloudflare, Open-Meteo, Esri, Apple for push and for the App Store version check, and Google Play for its update check) | follows rows 2 |
| 10 | g | Push token | (nothing) | The app sends the token only while the phone's own notification permission is on, and removes it if you turn that permission off. A token the app has not renewed for 30 days is deleted. (depends on the behaviour-change ticket; 30 is a placeholder) | owner 2026-11-25: "that behaviour change can move to 0.9.1" |

## Item g: DECIDED (owner 2026-11-25, typed, verbatim): "that behaviour change can move to 0.9.1"

Option 2 is chosen: the behaviour change ships in 0.9.1 under its own ticket (another lane builds it; this PR does not): the token is registered only while the phone's permission is granted, unregistered when it is revoked, and the worker drops a token not re-registered for about 30 days.

Fact it rests on (verified): `useNotifications` writes the switch before it asks the phone (`packages/ui/src/notify.ts` setOn / putRule), and the registrar syncs on `notifyActive(state)` only, never on the phone's permission; `workers/feeds/src/push.ts` has no token age limit.

Row 10 (Push token paragraph, new sentences):

> The app sends the token only while the phone's own notification permission is on, and removes it if you turn that permission off. A token the app has not renewed for 30 days is deleted.

**Depends on the behaviour-change ticket.** The 30 is a placeholder until that lane fixes the day count (update `html.ts` and the pin in `site.test.ts` to match). This page must NOT go live before the behaviour change does: until then these two sentences are untrue.

## Not changed (checked, true as written)

The "three things" bullet (iPhone with notifications on), the Directions-on-web claim, the 90 and 180 day figures.
