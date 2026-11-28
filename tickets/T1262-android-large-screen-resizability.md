# T1262 — Play Console warning: orientation and resizability limits on large screens (Android 16)

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1262 | Play Console warning: orientation and resizability limits on large screens (Android 16) | OPEN | — | walk the app on a tablet and a foldable (portrait lock is ignored there); look changes go to the owner as whole options |

Source: the owner, 2026-11-27, read in Google Play Console (Android, closed testing), verbatim: "Remove resizability and orientation restrictions in your app to support large screen devices
From Android 16, Android will ignore resizability and orientation restrictions for large screen devices, such as foldables and tablets. This may lead to layout and usability issues for your users."

## What a read-only look found (2026-11-27; guesses are labelled)
- `app.json` has `"orientation": "portrait"` (top level). It becomes `screenOrientation="portrait"` on the activity.
  `plugins/with-optional-portrait.js` (T913) only marks the portrait hardware feature optional so Play does not hide the app.
- No `resizeableActivity` is set anywhere in `app.json`, `app.config.js` or the plugins (so the platform default applies; GUESS on what the
  generated manifest holds, not checked: nothing was prebuilt). `ios.supportsTablet` is false (iOS, not part of this warning).
- The layout code uses `useWindowDimensions` (`app/apps/mobile/src/App.tsx`); whether any screen assumes a phone-width portrait window is not known.

## Work
1. Decide with the owner: keep the portrait lock for phones (Android 16 ignores it only on screens of 600 dp and wider) and make the
   layout hold at tablet and foldable sizes, or drop the lock. Default: keep the lock for phones, make large screens work.
2. Walk the app in an emulator at tablet and foldable sizes, in both orientations and in split-screen: map, water page, sheets,
   guided tour, report form. List what breaks.
3. Any look change (a wider layout, a side panel, a max width) goes to the owner as whole options, one sheet each, one question. Nothing ships unasked.
4. `with-optional-portrait` stays correct if the lock stays; if the lock goes, remove the plugin in the same PR (its own header says why it exists).

## Acceptance
A written walk list with screenshots at phone, tablet and foldable sizes; no clipped or unusable screen in either orientation; the
owner's pick recorded for any look change; the Play warning gone or explained.

Depends: none. Size: M. Priority: normal. Owner: yes (look options). Default: phones stay portrait, large screens get a layout that works with no new look.
