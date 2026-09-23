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

import { existsSync, readFileSync } from "node:fs";
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

/** Whether a bundle file is there — for tests that opt in to a real bundle by path. */
export const bundleExists = (path: string | undefined): path is string =>
  !!path && existsSync(path);

/** THE ONE DEFINITION OF THE FORMAT, which the production bundler also executes. */
export const BUNDLE_SCHEMA = fileURLToPath(
  new URL("../../../../../../pipeline/deliver/bundle/schema.sql", import.meta.url));

/**
 * An empty bundle in memory, built from `schema.sql`, that a test fills with rows shaped exactly
 * as the bundler writes them. For tables the dev fixture cannot hold (it is cut from a design
 * file older than them), so a column renamed on either side still fails a test.
 */
export function schemaBundle(fill: (db: DatabaseSync) => void): Db {
  const db = new DatabaseSync(":memory:");
  db.exec(readFileSync(BUNDLE_SCHEMA, "utf8"));
  fill(db);
  return {
    all: async (sql, ...params) => db.prepare(sql).all(...params) as unknown as Row[],
    get: async (sql, ...params) => db.prepare(sql).get(...params) as unknown as Row | undefined,
    close: () => db.close(),
  };
}
