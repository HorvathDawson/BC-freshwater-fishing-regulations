/**
 * The shared declaration for `./open` (P11): how a release SQLite file is opened on this platform.
 *
 *   open.web.ts      sql.js over the verified bytes (the loader's `bytes()`): the web holds no file.
 *   open.native.ts   expo-sqlite on the INSTALLED file, in place: nothing is read into memory. A file not installed
 *                    (never the case after `install`) falls back to the verified bytes, deserialized.
 *
 * Bundlers pick by platform extension and TypeScript does not, so both are checked against this one signature
 * (check-platform holds the pair's export surfaces equal).
 */
import type { DbName, Opener } from "../loader";

export interface OpenAt {
  /** Where sql.js finds its wasm (web). The phone has SQLite built in and ignores it. */
  wasmBase: string;
  /** The installed copy of a file — its folder (a file:// uri) and name — or null where none is installed. */
  installed: (name: DbName) => { dir: string; file: string } | null;
}

export declare function releaseOpener(at: OpenAt): Opener;
