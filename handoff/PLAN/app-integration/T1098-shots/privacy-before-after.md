# T1098: the privacy page, before and after (owner reads this first)

Page source: `app/tools/seo/html.ts` (`privacyBody`), URL `/privacy`. Words only; the app does not change.
Pick on item a (owner 2026-11-24, "Strict: linked"): the install id stays on a report that carries an email.
Each row was checked against the code first; the ticket was right about a to f. Item g is the owner's pick (bottom).
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
| 10 | g | Push token | (nothing) | PROPOSAL, see below | |

## Item g: your pick (I did not decide)

Fact (verified): `useNotifications` writes the switch before it asks the phone (`packages/ui/src/notify.ts` setOn / putRule: `write(...)` then `allow()`), and the registrar syncs on `notifyActive(state)` only, never on the phone's permission. So after Don't Allow the token is still registered (silent pushes need no permission). `workers/feeds/src/push.ts` has no token age limit; a token goes only on unregister or an APNs refusal.

Option 1, SAY IT (built into this PR as the default, clearly a proposal): row 10 adds, in the Push token paragraph:

> The app sends the token when notifications are switched on inside the app, even if you tapped Don't Allow on the phone's own prompt (then nothing is shown to you, but the silent signal still reaches the app). A token has no age limit: I keep it until you switch notifications off or Apple tells me it no longer works.

Option 2, CHANGE BEHAVIOUR (its own ticket, nothing built): the registrar also requires the phone's permission to be granted (not registering after Don't Allow, unregistering if it is later revoked), and the worker drops a token not re-registered for N days (the app re-registers every 7 days, so about 30 would be safe). Then the sentence is replaced by "The token is sent only while the phone's permission is on" and an age line.

If you pick option 2, delete the row-10 sentence from `html.ts` and its pin in `site.test.ts` before the page goes live.

## Not changed (checked, true as written)

The "three things" bullet (iPhone with notifications on), the Directions-on-web claim, the 90 and 180 day figures.
