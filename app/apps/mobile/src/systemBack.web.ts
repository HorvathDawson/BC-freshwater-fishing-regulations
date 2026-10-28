/**
 * The browser's back button, and a phone browser's own swipe-back, call the one back (G1): every back step the route
 * holds is a history entry, kept in step by `@app/ui` `historySync` — so the browser's back pops exactly one frame,
 * the ‹ button leaves no stranded entry, and back at the browse root is the browser's own (it leaves the app).
 */
import { historySync, routeSearch, type HistoryPort, type Route, type SystemBack } from "@app/ui";

/** The key this app's entries carry their back depth under, beside anything else a page keeps in `history.state`. */
const KEY = "canifishthis.back";

const port: HistoryPort = {
  index: () => {
    const s: unknown = window.history.state;
    const v = s && typeof s === "object" ? (s as Record<string, unknown>)[KEY] : undefined;
    return typeof v === "number" ? v : null;
  },
  push: (index) => { window.history.pushState({ [KEY]: index }, ""); },
  go: (delta) => { window.history.go(delta); },
  listen: (onPop) => {
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  },
};

export const systemBack: SystemBack = (back) => historySync(port, back);

/**
 * (T221, owner 2026-10-28) THE ADDRESS BAR FOLLOWS THE ROUTE: every move rewrites the CURRENT history entry's URL to the
 * water in front (`routeSearch`: `?regs=<item>&part=&view=`), so a refresh reopens that water, part and view through
 * the start link. It only REPLACES: the entries themselves are `systemBack`'s above (one per back step), so the back
 * button is unchanged; it lives here because this file is the one place the History API is touched (back-layers.test). The rewrite waits a microtask — until the same commit's history push has made the new
 * entry current — and runs again after the browser's own back / forward lands, so each entry carries its own place.
 */

let last: Route | null = null;
let listening = false;

function write(): void {
  if (last === null) return;
  const { pathname, search, hash } = window.location;
  const next = routeSearch(last, search);
  if (next !== search) window.history.replaceState(window.history.state, "", `${pathname}${next}${hash}`);
}

export function routeUrl(r: Route): void {
  last = r;
  if (!listening) {
    listening = true;
    window.addEventListener("popstate", () => { setTimeout(write, 0); });
  }
  queueMicrotask(write);
}
