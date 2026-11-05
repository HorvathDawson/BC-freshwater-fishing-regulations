/**
 * The shared declaration for `./appUpdater` (T390): the platform's way to a newer app. `appUpdater.native.ts` is the
 * phone's (Play's in-app update, the App Store page, the staging APK page); `appUpdater.web.ts` has none (the site is
 * always the latest). Bundlers pick by platform extension, TypeScript does not.
 */
import type { AppVersion } from "@app/core";
import type { AppUpdatePort } from "@app/ui";

export declare function makeAppUpdater(own: AppVersion): AppUpdatePort | null;
