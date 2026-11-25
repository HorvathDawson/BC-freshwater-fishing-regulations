# T1101 — iPhone: About and the version headers show the Android build number (build 45 probably says "build 44")

Status: DONE — 59f7aa831 (version-stamp.js stamps both numbers, config.ts picks with Platform.select; tests in app/tools/app-version.test.ts). Cause confirmed in code: build-ios-testflight.sh sets ios.buildNumber in the build worktree only, the stamp read android.versionCode. Not checked on a device; ships with the next iPhone build.

Source: iOS release chat 2026-11-24 ~23:40, its read of build 0.9.0 (45) at 65f84020c for T1071 (App Privacy and review risk). `app/apps/mobile/version-stamp.js` line 24 reads the Android versionCode for both platforms. Not yet checked
on a device.

## Work
1. Check on the iPhone build (About, the User-Agent / version header, the report's version field) what number shows.
2. The stamp reads each platform's own build number (iPhone: CFBundleVersion); reports and crash kinds then name
   the build that really ran.
3. Test on the stamp for both platforms.

## Acceptance
iPhone build N shows N in About and in a report sent from it.

Severity: low (cosmetic, but reports name the wrong build). Release: the next iPhone build (0.9.1 if its app build
is not cut yet, else 0.9.2). Model: Sonnet. Size: S. Owner: no.
