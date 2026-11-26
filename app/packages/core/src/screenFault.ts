/**
 * (T1164) A SCREEN THAT COULD NOT BE DRAWN, in words. A fault while drawing one screen is shown on that screen (and
 * reported, `failure.ts`), never the end of the app; each screen says which it is. Whole sentences, one per screen,
 * so a test pins the line the reader sees.
 */
export type ScreenName = "map" | "conditions" | "runs" | "spots" | "more" | "water" | "spot" | "newSpot" | "app";

const LINES: Readonly<Record<ScreenName, string>> = {
  map: "The map hit a problem and could not be drawn.",
  conditions: "Conditions hit a problem and could not be drawn.",
  runs: "Runs hit a problem and could not be drawn.",
  spots: "Spots hit a problem and could not be drawn.",
  more: "More hit a problem and could not be drawn.",
  water: "This water hit a problem and could not be drawn.",
  spot: "This spot hit a problem and could not be drawn.",
  newSpot: "Adding a spot hit a problem and could not be drawn.",
  // the last resort: the shell's own chrome (bars, rail, sheets), the boot screen, or the app's top level
  app: "The app hit a problem and could not be drawn.",
};

/** What a screen says when it could not be drawn. */
export const screenFaultLine = (screen: ScreenName): string => LINES[screen];

/** The two ways on from it: draw the screen again, or go back to where the reader came from. */
export const SCREEN_FAULT_ACTIONS = { retry: "Try again", back: "Go back" } as const;
