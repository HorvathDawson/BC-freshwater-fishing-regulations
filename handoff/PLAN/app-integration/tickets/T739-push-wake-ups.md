# T739 — Push "wake-ups" so alerts and notices arrive on time (iPhone first, Android as a second step)

Status: IN PROGRESS (both steps approved by the owner 2026-11-14: iPhone, then "for android too."; iOS chat, branch `ios/t739-push`; Android needs the owner's Firebase project file before it can be built).

Source: owner 2026-11-14, verbatim: "dont we want to use it for the notifications thought?" (on the Push Notifications
box of the App ID), "there are also time sensitive notifications?" and "can we do this same push notification style to
android?". Audit findings F3 / G3 (`handoff/PLAN/ios/parity-audit.md`): the app's own words say checks run "about every
15 minutes at best. Android may check less often while the phone is idle" (`packages/core/src/alerts.ts`), and on iPhone
the background check runs only when iOS allows, often hours late.

## Owner answers (2026-11-14, late)
Verbatim: "okay i ticked push notifications for app yes build for iphone." / "dont need this (Closure notices marked
time-sensitive on iPhone: off for now / on.)" / "i guess if new data is there we could send a notification that the user
needs to download it."
So: step 1 (iPhone, APNs) is approved and Push is ticked on the App ID; the `aps-environment` entitlement stays (audit
G12's removal is dropped). Time-sensitive notices are NOT wanted. New scope from the third sentence (Claude's reading):
the wake-up also fires when a new release is published; the phone then starts the download itself when its settings
allow, and otherwise makes a local notification saying new data is ready to download. Android (FCM): answered a minute later, verbatim: "for android too."
So the Android step is approved as well, said after being told it means Google's Firebase inside the app: Claude reads
that as the owner's exception to "self-host everything" for push delivery only (no Firebase Analytics, nothing else of
Firebase). The owner makes the Firebase project and hands over `google-services.json`.

## Today (read in the code)
No notification on either platform uses push. The phone wakes in the background (`apps/mobile/src/notify.native.ts`,
`packages/data/src/notify/check.ts`), reads our feeds, and makes a local notification itself. The alert rules (which
waters, which thresholds) never leave the phone. There is no Firebase and no push token anywhere in the app.

## Proposal
A content-free "check now" push. When a feed changes (a new in-season notice, a DFO salmon change, a gauge update
round), the feeds worker sends a silent push to every registered phone; the phone runs the SAME check it runs today and
makes the same local notification if a rule of that reader's fires. The periodic background check stays as the fallback
and is reported when it is the only thing that ran (no silent fallback).
- **What our server learns:** a device push token and the platform, nothing else. Not which waters are watched, not the
  alert rules, not a location. A token is dropped when the push service says it is dead.
- **iPhone (step 1):** Apple's push service (APNs) straight from the worker with an APNs key. No third-party SDK. Needs
  Push Notifications ticked on the App ID (owner), an APNs key saved as a worker secret (owner makes it), the
  `aps-environment` entitlement kept (audit G12's removal is on hold for this), Background Modes "remote-notification".
  Limits to state in the app: iOS throttles silent pushes (a few an hour), does not deliver them to an app the reader
  swiped away, and holds them in Low Power Mode.
- **Android (step 2, only if phones show late alerts):** the only push channel is Google's Firebase Cloud Messaging: a
  Firebase project, `google-services.json`, the FCM library in the app (analytics off). That is a Google service in the
  app, so it needs the owner's exception to "self-host everything" (FCM is to Android what APNs is to iPhone). Android's
  15-minute check mostly works today; Doze on an idle phone is the case push would fix.
- **Time Sensitive (separate pick):** a closure notice for a watched water could be marked time-sensitive on iPhone so
  it breaks through Focus and the scheduled summary (capability ticked on the App ID, one field on the notification).
  Off by default in this proposal.
- **Privacy:** one new sentence on the privacy page and in both stores' answers (a push token, for delivering
  notifications, not linked to an account). The Android chat owns the privacy page and `play/data-safety.md`.

## Acceptance (if picked)
On a real iPhone, with the app in the background, a new in-season notice for a watched water notifies within 2 minutes
of the feed changing (today: hours); the worker's store holds tokens only; turning notifications off in the app removes
the token; a failed push is reported and the periodic check still catches the notice.

Depends: T88 (a signed build on a real iPhone), the feeds worker. Size: M (iPhone), M (Android). Owner: yes (the pick,
the APNs key, the App ID box; for Android the Firebase project and the self-host exception).

## Results
(filled in as the work runs)

## Owner answer, 2026-11-15 evening (OWNER-TODO.md "owner answers, round 6", verbatim: "Yes to both (Recommended by it)")
The proposed privacy-page line ("Push token. If you turn notifications on, the app sends our server a push token for
this phone …") and the App Store privacy answer (Identifiers → Device ID: collected, App Functionality, not linked to
the user, not used for tracking) are approved as written (To-Do row `t739-privacy-wording`). Left (Claude): the line
into the privacy page's source on the push branch; the Android / Firebase line and the Play Data safety answer still
wait for the Firebase decision. Putting the page live is the owner's, with the production release. This removes the
wording block on merging the push code; the merge order (push last) stands.
