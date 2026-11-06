/**
 * (T409, owner 2026-11-05) THE WEB MAP IS BUILT WHEN IT IS FIRST SEEN.
 *
 * A water's own page (`/water/<slug>`) mounts the browse map behind the water once idle (the Shell's warm map, kept
 * unseen at `opacity: 0`). Building maplibre there cost ~1.5 s of main thread (style parse, paint arrays) on a page whose
 * reader is reading the pane, not the map. So on the web a map whose box sits under an ancestor at opacity 0 is not built
 * until that ancestor shows it (the phone's first "back to map" then waits for the map once — accepted). On a desktop-
 * wide window it is built at once, as before (owner's call).
 *
 * THE SAME MAP HOWEVER IT IS REACHED: the gate only delays the mount. Once seen, `Map.web.tsx` mounts the one map with
 * the props it has THEN — exactly what a map mounted visible at that moment gets (a tap, back, step, search, refresh).
 * Once seen it stays built; nothing here ever unbuilds it.
 *
 * Web-only (this folder is `Map.web.tsx`'s): the native map is never gated.
 */
import { useLayoutEffect, useState, type RefObject } from "react";

/**
 * The window width from which the map is built at once: `@app/ui` `LAYOUT_BREAKPOINTS.desktop` (this package may not
 * import `@app/ui`; `firstSeen.render.test.tsx` pins the two equal).
 */
export const BUILD_AT_ONCE_FROM = 1280;

/** Whether this element or an ancestor is drawn at opacity 0 (the Shell's `BEHIND`): nothing in it is seen. */
function unseen(el: Element): boolean {
  const view = el.ownerDocument.defaultView;
  if (!view) return false;
  for (let e: Element | null = el; e; e = e.parentElement)
    if (view.getComputedStyle(e).opacity === "0") return true;
  return false;
}

/** Whether the window is desktop-wide (the map is built at once there). */
function wide(el: Element): boolean {
  return el.ownerDocument.documentElement.clientWidth >= BUILD_AT_ONCE_FROM;
}

/**
 * True from the first moment the probe's box is seen (or the window is desktop-wide), and for good. Checked before the
 * first paint, then again whenever an ancestor's class or style changes (react-native-web restyles by class) or the
 * window is resized.
 */
export function useFirstSeen(probe: RefObject<HTMLElement | null>): boolean {
  const [seen, setSeen] = useState(false);
  useLayoutEffect(() => {
    const el = probe.current;
    if (seen || !el) return;
    const now = () => wide(el) || !unseen(el);
    if (now()) { setSeen(true); return; }
    const view = el.ownerDocument.defaultView!;
    const check = () => { if (now()) setSeen(true); };
    const watch = new view.MutationObserver(check);
    for (let e: Element | null = el.parentElement; e; e = e.parentElement)
      watch.observe(e, { attributes: true, attributeFilter: ["class", "style"] });
    view.addEventListener("resize", check);
    return () => { watch.disconnect(); view.removeEventListener("resize", check); };
  }, [seen, probe]);
  return seen;
}
