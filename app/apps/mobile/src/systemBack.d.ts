/**
 * The shared declaration for `./systemBack` (G1): the platform's own back input, wired to the one back (`@app/ui`
 * `useBack`). `systemBack.native.ts` is Android's `BackHandler` (the button and the edge gesture; false at the browse
 * root lets Android leave the app); `systemBack.web.ts` is the browser's back button and a phone browser's own
 * swipe-back, through the History API. Bundlers pick by platform extension, TypeScript does not.
 */
import type { ReleaseLoader } from "@app/data/release";
import type { RegsStore } from "@app/data/regs";
import type { Route, SystemBack } from "@app/ui";

export declare const systemBack: SystemBack;

/**
 * (T221) The web's address bar follows the route — the water in front as a start link (`@app/ui` `routeSearch`), so a
 * refresh opens it again; the current entry is rewritten, never a new one pushed. Native: nothing (no address bar).
 */
export declare function routeUrl(r: Route): void;

/**
 * (T369) The open release the address makes a frame's PLACE from (`@app/ui/place` `linkSearch`, web-only): its regs
 * store and loader (the `lines` file, fetched in the background once the page is drawn); native has no address bar.
 */
export declare function routeLinks(store: RegsStore | null, loader: ReleaseLoader | null): void;
