/**
 * The `node:sqlite` driver. Tests, the packager, and any script that reads a bundle.
 *
 * NOT the app's driver on any platform: the web reads byte ranges off R2 through a wasm
 * build, and the phone reads a resident file through expo-sqlite. This one exists so the
 * queries can be tested against a real SQLite without a browser, which is what makes the
 * other two drivers cheap — they only have to satisfy `Db`, and `Db` is four methods.
 */
import { DatabaseSync } from "node:sqlite";
import type { Db, Row } from "../db";

import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";

/** The development bundle `pnpm fixture` writes. */
export const DEV_BUNDLE = fileURLToPath(new URL("../../../dev/bundle.sqlite", import.meta.url));
export const hasDevBundle = (): boolean => existsSync(DEV_BUNDLE);

export function openBundle(path: string = DEV_BUNDLE): Db {
  const db = new DatabaseSync(path, { readOnly: true });
  return {
    // node:sqlite is synchronous; `Db` is not, because the web driver cannot be. Resolving
    // immediately is the whole adaptation.
    all: async (sql, ...params) => db.prepare(sql).all(...params) as unknown as Row[],
    get: async (sql, ...params) => db.prepare(sql).get(...params) as unknown as Row | undefined,
    close: () => db.close(),
  };
}
