import { createContext } from "react";

/**
 * (T269) THE BAR'S WIDTH BEFORE IT IS MEASURED. In the app: 0 — the bar waits for its layout, as it always has. The SEO
 * build's server render never lays out, so it hands the width its layout class draws the bar at (`tools/seo/pane.tsx`),
 * and the bar is drawn at that width scaled to its box (a viewBox; the day's marker placed by percent): the same bar at
 * that width, and within a few pixels of it on any other, until the app's own pane replaces it.
 */
export const StripWidthHint = createContext(0);
