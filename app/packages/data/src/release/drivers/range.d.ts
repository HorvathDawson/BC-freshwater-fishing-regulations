/**
 * The shared declaration for `./range` (T831): reading an INSTALLED release file by byte range, where its bytes are —
 * the map reads the atlas's header, directories and a route's tiles so (`@app/map` `TileEndpoints.readRange`: the route
 * map's lines). `fetch` of a `file://` uri ignores the range and reads the whole archive.
 *
 *   range.native.ts   expo-file-system's file handle, as the install's `readAt` (`install.native.ts`).
 *   range.web.ts      null: the web installs nothing; its archive is read over HTTP by range (`pmtiles`' own fetch).
 */

/** `length` bytes of the file at `uri` (its `file://` uri) from `offset`: fewer only at its end, none past it. */
export type ReadAt = (uri: string, offset: number, length: number) => Promise<Uint8Array>;

export declare const atlasRange: ReadAt | null;
