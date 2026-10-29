/**
 * The shared declaration for `./store`.
 *
 * `store.native.ts` writes a `.spots` file; `store.web.ts` uses browser storage. Bundlers
 * pick by platform extension, TypeScript does not — so both are checked against one
 * signature. A store that exports something the other does not is how a feature ships on
 * one platform only.
 */
import type { SpotsStore } from "./core";

export declare function openSpots(): SpotsStore;

/** The Layers sheet's choice, as whole text under one name (`@app/ui` `LayerStore`). */
export declare function openLayerStore(): { read(): string | null; write(text: string): void };

/** T51: the last fetched "Like your visit?" forecasts, as whole text under one name (`./likeVisit` `LikeStore`). */
export declare function openLikeStore(): { read(): string | null; write(text: string): void };

/** (T211) The last captured DFO salmon feed, as whole text under one name (`../feed/salmon` `SalmonFeedStore`). */
export declare function openSalmonFeedStore(): { read(): string | null; write(text: string): void };

/** (T230) The last captured in-season feed + links (`../feed/inseason`), as whole text under one name. */
export declare function openInSeasonStore(): { read(): string | null; write(text: string): void };
/** (T230) The in-season row ids this device has been shown in the notice (a JSON list). */
export declare function openInSeasonSeenStore(): { read(): string | null; write(text: string): void };
/** (T231) The update service's small state (`@app/data` `UpdateStore`), as whole text under one name. */
export declare function openUpdateStore(): { read(): string | null; write(text: string): void };
/** (T251) The Updates log's ring buffer (development and staging builds only), as whole text. */
export declare function openUpdateLogStore(): { read(): string | null; write(text: string): void };
/** (T236) The first-run notice's accepted version (`@app/core` `FIRST_RUN_VERSION`), as whole text. */
export declare function openFirstRunStore(): { read(): string | null; write(text: string): void };
/** (T248) The first-run walkthrough's seen version (`@app/core` `WALKTHROUGH_VERSION`), as whole text. */
export declare function openWalkthroughStore(): { read(): string | null; write(text: string): void };
/** (T247) The recent searches, a JSON list as whole text; device-only. */
export declare function openRecentSearchStore(): { read(): string | null; write(text: string): void };
/** (T237) Problem reports waiting to be sent (offline), as whole text (a JSON list). */
export declare function openReportOutboxStore(): { read(): string | null; write(text: string): void };
