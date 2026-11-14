/**
 * (T723 G6) A REFUSED PERMISSION IS A CONDITION OF THE DEVICE, NOT A DEFECT, AND THE WAY OUT IS THE SETTINGS APP. After one
 * "Don't Allow" (iOS never prompts again; Android after two) only Settings can turn the camera or location back on, so
 * every line that says "it was refused" carries an "Open Settings" action. The refusal itself is `environmentError`
 * (shown, never reported: `failure.ts`).
 */
import { MY_LOCATION, SPOTS_MENU } from "./mapRail";

/** The camera was refused: what `pickPhoto` throws, and what the photo line says in brackets (`photoFailed`). */
export const CAMERA_REFUSED = "the camera was not allowed";

/** The action's label, its screen-reader name, and the line said when Settings itself would not open. */
export const OPEN_SETTINGS = {
  label: "Open Settings",
  a11y: "Open this app's settings to allow it",
  failed: "Could not open Settings.",
} as const;

/** The lines that say a permission was refused: each is followed by the "Open Settings" action. */
const REFUSAL_LINES: readonly string[] = [MY_LOCATION.denied, SPOTS_MENU.mineOff, CAMERA_REFUSED];

/** Does this line say a permission was refused (so Settings is the way out)? */
export const needsSettings = (line: string | null | undefined): boolean =>
  !!line && REFUSAL_LINES.some((r) => line.includes(r));
