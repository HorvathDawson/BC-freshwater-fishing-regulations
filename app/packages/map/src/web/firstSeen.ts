/**
 * (T409, owner 2026-11-05) THE WEB MAP IS BUILT WHEN IT IS FIRST SEEN.
 *
 * A water's own page (`/water/<slug>`) mounts the browse map behind the water once idle (the Shell's warm map, kept
 * unseen at `opacity: 0`). Building maplibre there cost ~1.5 s of main thread (style parse, paint arrays) on a page whose
 * reader is reading the pane, not the map. So on the web a map whose box sits under an ancestor at opacity 0 is not built
 * until that ancestor shows it (the phone's first "back to map" then waits for the map once — accepted). At every width
 * (owner 2026-11-05: the water page covers the whole window on desktop too, so the map is unseen there as well).
 *
 * THE SAME MAP HOWEVER IT IS REACHED: the gate only delays the mount. Once seen, `Map.web.tsx` mounts the one map with
 * the props it has THEN — exactly what a map mounted visible at that moment gets (a tap, back, step, search, refresh).
 * Once seen it stays built; nothing here ever unbuilds it.
 *
 * Web-only (this folder is `Map.web.tsx`'s): the native map is never gated.
 */
import { useLayoutEffect, useState, type RefObject } from "react";

/** Whether this element or an ancestor is drawn at opacity 0 (the Shell's `BEHIND`): nothing in it is seen. */
function unseen(el: Element): boolean {
  const view = el.ownerDocument.defaultView;
  if (!view) return false;
  for (let e: Element | null = el; e; e = e.parentElement)
    if (view.getComputedStyle(e).opacity === "0") return true;
  return false;
}

/**
 * True from the first moment the probe's box is seen, and for good. Checked before the first paint, then again whenever
 * an ancestor's class or style changes (react-native-web restyles by class).
 */
export function useFirstSeen(probe: RefObject<HTMLElement | null>): boolean {
  const [seen, setSeen] = useState(false);
  useLayoutEffect(() => {
    const el = probe.current;
    if (seen || !el) return;
    if (!unseen(el)) { setSeen(true); return; }
    const view = el.ownerDocument.defaultView!;
    const check = () => { if (!unseen(el)) setSeen(true); };
    const watch = new view.MutationObserver(check);
    for (let e: Element | null = el.parentElement; e; e = e.parentElement)
      watch.observe(e, { attributes: true, attributeFilter: ["class", "style"] });
    return () => watch.disconnect();
  }, [seen, probe]);
  return seen;
}
