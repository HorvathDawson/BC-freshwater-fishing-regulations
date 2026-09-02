/**
 * The shared declaration for `./engine`.
 *
 * The implementations are `engine.native.ts` (expo-sqlite) and `engine.web.ts` (sql.js),
 * and BUNDLERS pick between them by platform extension — TypeScript does not. Both are
 * checked against this one signature, which is the property that matters: a driver that
 * exports something the other does not is how a feature ships on one platform only.
 */
import type { Db } from "../db";

export interface LoadedBundle extends Db {
  /** How many bytes the bundle cost to open. 0 once a range-read driver exists. */
  bytes: number;
}

/** Web only: where sql.js finds its wasm. A no-op on native, which has SQLite built in. */
export declare function setSqlWasmUrl(url: string): void;

export declare function loadBundle(
  url: string, signal?: AbortSignal, onStage?: (stage: string) => void,
): Promise<LoadedBundle>;
