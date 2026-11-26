/** (T723 G6) which lines are a refused permission (and so carry "Open Settings"), pinned as whole strings. */
import { describe, expect, it } from "vitest";
import { CAMERA_REFUSED, needsSettings, OPEN_SETTINGS } from "./permissions";
import { MY_LOCATION, SPOTS_MENU } from "./mapRail";

describe("needsSettings", () => {
  it("the refusal lines say so; others do not", () => {
    expect([
      needsSettings(MY_LOCATION.denied),
      needsSettings(SPOTS_MENU.mineOff),
      needsSettings("The photo was not added (the camera was not allowed). Nothing else changed."),
      needsSettings(SPOTS_MENU.noLocation),
      needsSettings(MY_LOCATION.noFix),
      needsSettings(MY_LOCATION.deviceOff),     // (T1093) the app's own settings page would show Location allowed: no action
      needsSettings("The photo was not added (disk full). Nothing else changed."),
      needsSettings(null), needsSettings(undefined),
    ]).toEqual([true, true, true, false, false, false, false, false, false]);
    expect(CAMERA_REFUSED).toBe("the camera was not allowed");
  });
  it("(T1093) the phone's Location off says so whole, and is not the app's refusal", () => {
    expect(MY_LOCATION.deviceOff).toBe("Your phone could not get a location. Check that Location is turned on in its settings, then tap again.");
  });
  it("the action's words", () => {
    expect(OPEN_SETTINGS.label).toBe("Open Settings");
  });
});
