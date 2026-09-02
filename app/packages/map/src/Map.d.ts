/**
 * The shared declaration for `./Map`.
 *
 * The implementations are `Map.native.tsx` and `Map.web.tsx`, and BUNDLERS pick between
 * them by platform extension — TypeScript does not. Without this file `tsc` cannot resolve
 * `./Map` at all; with it, both implementations are checked against one signature, which
 * is the property that matters: a prop only one renderer honours is how two platforms
 * start drawing different maps.
 */
import type { MapProps } from "./map-props";

export declare function Map(props: MapProps): JSX.Element;
