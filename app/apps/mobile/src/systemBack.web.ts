/**
 * The browser's back button, and a phone browser's own swipe-back, call the one back (G1): every back step the route
 * holds is a history entry, kept in step by `@app/ui` `historySync` — so the browser's back pops exactly one frame,
 * the ‹ button leaves no stranded entry, and back at the browse root is the browser's own (it leaves the app).
 */
import { historySync, type HistoryPort, type SystemBack } from "@app/ui";

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
