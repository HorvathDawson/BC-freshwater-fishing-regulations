/**
 * (T390) The web has no newer app to offer: the site serves the latest build, so there is no channel and no store
 * package in the web bundle (no first-paint weight). A release refused for an old app is still said (the strong prompt,
 * with no button).
 */
import type { AppVersion } from "@app/core";
import type { AppUpdatePort } from "@app/ui";

export function makeAppUpdater(_own: AppVersion): AppUpdatePort | null {
  return null;
}
