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
