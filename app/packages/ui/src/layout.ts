/**
 * T45 — THE LAYOUT CLASS: how much room a screen has, as one of three words, so a component arranges itself for a phone,
 * a tablet or a desktop window without ever reading the window (owner 2026-10-21: "we are mobile focused but we want to
 * make a decent desktop experience too").
 *
 *   phone    below `LAYOUT_BREAKPOINTS.tablet` — THE DESIGN: every phone screen is drawn as it always was
 *   tablet   from `tablet` (900): a reading column instead of one phone column stretched across the frame
 *   desktop  from `desktop` (1280): room for a side column beside the page as well
 *
 * WHERE THE WIDTH COMES FROM: the app shell (apps/mobile App.tsx) provides the window's width through
 * `LayoutWidthContext` (react-native's `useWindowDimensions`, the platform glue — this package may not import
 * react-native). Components call `useLayoutClass()` or take the class as a prop; none reads the window itself. No
 * provider (a screen mounted alone in a test) is a phone: the design, never a guess at a wide screen.
 */
import { createContext, useContext } from "react";

export type LayoutClass = "phone" | "tablet" | "desktop";

/** The breakpoint tokens: the window's width (px) at which each wider class starts. */
export const LAYOUT_BREAKPOINTS = { tablet: 900, desktop: 1280 } as const;

/** The class for a window this wide; no width (null, 0, NaN: not measured) is a phone. */
export function layoutClassFor(width: number | null | undefined): LayoutClass {
  if (!width || !(width > 0)) return "phone";
  return width >= LAYOUT_BREAKPOINTS.desktop ? "desktop" : width >= LAYOUT_BREAKPOINTS.tablet ? "tablet" : "phone";
}

/** The window's width, provided once by the app shell (null: not provided — a phone). */
export const LayoutWidthContext = createContext<number | null>(null);

/** The layout class of the window the app is drawn in (re-renders on a resize the shell reports). */
export function useLayoutClass(): LayoutClass {
  return layoutClassFor(useContext(LayoutWidthContext));
}
