# T263 — the bell says when a data update is waiting for Wi-Fi, ready, or needs a newer app

Status: DONE — ae4181e30

Source: owner 2026-10-29 (T255 brief: a bigger update → "update available/needed at the top + in the bell/
notifications"). Today the release's state reaches only the sync button's label/dot and More → Data; the bell
(`Shell.tsx` `notices`, `bellLabel`, `InSeasonNotice`) lists in-season changes and FAILED updates only, so a release
waiting for Wi-Fi, one downloaded and waiting for a pause, or one that needs a newer app (`app-too-old`) is not in it.

## Work
1. Core: a release notice in the bell's words (`UPDATE_WORDS.bell`): waiting for Wi-Fi ("A data update (N MB) is
   waiting for Wi-Fi" + "Download on mobile data" = `allowMetered` for this one), ready ("An update is ready" + "Use it
   now" = the App's `apply`), app too old ("Update the app to get new regulations" + the store link when there is one).
   `bellLabel` counts it.
2. Shell/InSeasonNotice: a release section above the sync failures; the bell shows while any is present.
3. Render tests for each phase; the T255 `big-update` scenario's bell step then passes.
## Acceptance
`run.mjs big-update`: the bell step passes; render tests pin the three phases and their actions.
Depends: T231, T251. Size: M. Owner: yes — the words and whether "Download on mobile data" belongs in the bell.
Default: build it as above.

## Done (2026-10-29)
Words (core `UPDATE_WORDS.bell.release`, VM `releaseNoticeVM`, `bellLabel` counts it; plain downloading does not count): waiting
"New regulations ready to download — waiting for Wi-Fi (N MB)." + Download now (asks first: it turns on "Large updates on
mobile data"); downloading "Downloading new regulations… N%"; ready "New regulations downloaded — they'll apply next time
you return to the map." + Apply now (the App's `apply`); needs a newer app + Update the app (production: Play/App Store
listing by platform, placeholder URLs until the listings exist, `EXPO_PUBLIC_PLAY_STORE_URL` / `EXPO_PUBLIC_APP_STORE_URL`;
dev/staging: `EXPO_PUBLIC_DEV_APK_URL`, no button until set). Section first in the bell (`InSeasonNotice`). Tests in
`InSeason.test.tsx`; phone-width shots in `T263-shots/`. Not run: the emulator `big-update` bell step.
