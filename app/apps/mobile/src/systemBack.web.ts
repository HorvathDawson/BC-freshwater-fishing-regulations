/**
 * The browser's back button, and a phone browser's own swipe-back, call the one back (G1): every back step the route
 * holds is a history entry, kept in step by `@app/ui` `historySync` — so the browser's back pops exactly one frame,
 * the ‹ button leaves no stranded entry, and back at the browse root is the browser's own (it leaves the app).
 */
import type { ReleaseLoader } from "@app/data/release";
import type { RegsStore } from "@app/data/regs";
import { historySync, reportFailure, routeSearch, waterFrame, type HistoryPort, type Route, type SystemBack } from "@app/ui";
import { linkSearch, placeReads, type PlaceReads } from "@app/ui/place";
import { slugKnown, slugOf } from "./waterSlugs";

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
 * water in front (`linkSearch`: `?regs=<item>&blk=&m=&view=`, T369 the part as a PLACE made from the release), so a
 * refresh reopens that water, part and view through the start link. It only REPLACES: the entries themselves are `systemBack`'s above (one per back step), so the back
 * button is unchanged; it lives here because this file is the one place the History API is touched (back-layers.test). The rewrite waits a microtask — until the same commit's history push has made the new
 * entry current — and runs again after the browser's own back / forward lands, so each entry carries its own place.
 */

let last: Route | null = null;
let listening = false;
/** (T369) The release's reads a place is made from (`routeLinks`); null until the release is open. */
let places: PlaceReads | null = null;
/** (T369) A part the release cannot put in the address is reported once a session, not on every move. */
let missingSaid = false;
const onMissing = (e: Error) => { if (!missingSaid) { missingSaid = true; reportFailure(e); } };

/**
 * (T369, owner 2026-11-03) NOTHING WAITS FOR THE PLACE: the address is written at once without it (`?regs=<water>&view=…`),
 * and the place is added once the page is drawn — `lines` is fetched then, behind the page's own files (an extra, net
 * `priorityOf`) — by rewriting the same entry again. Only OPENING a link that carries a place waits for `lines`.
 */
async function write(): Promise<void> {
  if (last === null) return;
  const asked = last;
  put(asked, routeSearch(asked, window.location.search));
  if (places === null) return;
  await drawn();
  if (last !== asked) return;
  const q = await linkSearch(places, asked, window.location.search, onMissing);
  if (last === asked) put(asked, q);
}

/** After the next frame is painted: the page in front is drawn before its place is asked of the release. */
const drawn = (): Promise<void> => new Promise((done) => requestAnimationFrame(() => setTimeout(done, 0)));

/**
 * (T269) THE ADDRESS OF A NAMED WATER IS ITS PAGE: `/water/<slug>` (what a search engine lists), with the route's other
 * keys (part, view) as a query; anything else — the map, an unnamed reach, a water whose slug is not known (yet) — is
 * `/` with the start link, as before. A slug still being fetched rewrites the address again when it lands.
 */
function put(asked: Route, q: string): void {
  const { pathname, search, hash } = window.location;
  const f = waterFrame(asked);
  const item = f?.item ?? null;
  const slug = item ? slugKnown(item) : null;
  let path = "/", next = q;
  if (slug) {
    const p = new URLSearchParams(q);
    p.delete("regs");
    const rest = p.toString().replace(/%3A/gi, ":");
    path = `/water/${slug}`;
    next = rest ? `?${rest}` : "";
  } else if (item) {
    void slugOf(item).then((s) => { if (s && last === asked) void write(); });
  }
  if (next !== search || path !== pathname) window.history.replaceState(window.history.state, "", `${path}${next}${hash}`);
}

export function routeUrl(r: Route): void {
  last = r;
  if (!listening) {
    listening = true;
    window.addEventListener("popstate", () => { setTimeout(() => void write(), 0); });
  }
  queueMicrotask(() => void write());
}

/** (T369) The release the address makes places from (its regs store + loader: `lines`); set once it is open. */
export function routeLinks(store: RegsStore | null, loader: ReleaseLoader | null): void {
  places = store && loader ? placeReads(store, loader) : null;
  if (last !== null) queueMicrotask(() => void write());
}
