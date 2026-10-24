/**
 * The smallest thing a SQLite driver has to be.
 *
 * Three drivers will implement it and they have nothing else in common: `node:sqlite` for
 * tests and the packager, a wasm build reading byte ranges off R2 for the web, and
 * expo-sqlite against a resident file on the phone. Everything above this line — every
 * query, every row mapping — is written once and runs on all three.
 *
 * ASYNCHRONOUS, and that was a correction. I wrote this synchronous on the reasoning that
 * every driver can be sync once the page is in memory — which is true of node:sqlite and
 * of the native builds, and false of the one that matters most. Synchronous wasm SQLite on
 * the web needs `SharedArrayBuffer`, which needs the page to be cross-origin isolated
 * (COOP + COEP), which would in turn break every cross-origin resource that does not send
 * CORP — including the basemap's font and sprite CDN. The whole app would have paid for a
 * property only the database wanted.
 *
 * `RegsSource` is async at every method anyway, so nothing above this line changed shape.
 */
/** `noUncheckedIndexedAccess` is on, so a column read is `| undefined` — which is right:
 *  asking for a column the query did not select should not typecheck as present. */
export type Cell = string | number | null | Uint8Array | undefined;

export interface Row {
  [column: string]: Cell;
}

export interface Db {
  all(sql: string, ...params: (string | number | null)[]): Promise<Row[]>;
  get(sql: string, ...params: (string | number | null)[]): Promise<Row | undefined>;
  close?(): void;
}

/** Strings out of SQLite are `string | number | null`; most of ours are known to be text. */
export const str = (v: Cell): string => (v == null ? "" : String(v));
export const num = (v: Cell): number | null =>
  v == null || v === "" ? null : Number(v);
