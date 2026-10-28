/**
 * The shared declaration for `./systemBack` (G1): the platform's own back input, wired to the one back (`@app/ui`
 * `useBack`). `systemBack.native.ts` is Android's `BackHandler` (the button and the edge gesture; false at the browse
 * root lets Android leave the app); `systemBack.web.ts` is the browser's back button and a phone browser's own
 * swipe-back, through the History API. Bundlers pick by platform extension, TypeScript does not.
 */
import type { Route, SystemBack } from "@app/ui";

export declare const systemBack: SystemBack;

/**
 * (T221) The web's address bar follows the route — the water in front as a start link (`@app/ui` `routeSearch`), so a
 * refresh opens it again; the current entry is rewritten, never a new one pushed. Native: nothing (no address bar).
 */
export declare function routeUrl(r: Route): void;
