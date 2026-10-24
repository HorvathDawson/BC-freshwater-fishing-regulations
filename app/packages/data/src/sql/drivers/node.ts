/**
 * The `node:sqlite` driver: tests and tools (`@app/data/sql/node`).
 *
 * NOT the app's driver on any platform: the web opens a release file's verified bytes in sql.js and the phone in
 * expo-sqlite (`release/drivers`). This one exists so the queries can be tested against a real SQLite without a
 * browser, which is what makes the other two drivers cheap — they only have to satisfy `Db`, and `Db` is four methods.
 */
import { DatabaseSync } from "node:sqlite";
import type { Db, Row } from "../db";

function wrap(db: DatabaseSync): Db {
  return {
    // node:sqlite is synchronous; `Db` is not, because the web driver cannot be. Resolving
    // immediately is the whole adaptation.
    all: async (sql, ...params) => db.prepare(sql).all(...params) as unknown as Row[],
    get: async (sql, ...params) => db.prepare(sql).get(...params) as unknown as Row | undefined,
    close: () => db.close(),
  };
}

/** A SQLite file on disk, read-only. */
export function openSqlite(path: string): Db {
  return wrap(new DatabaseSync(path, { readOnly: true }));
}

/**
 * A database built in memory from DDL and a few rows — for a test that needs a shape no fixture has (a lake cut into
 * parts). `pipeline/deliver/bundle/schema.sql` is the one definition of the pipeline bundle's tables, which the release
 * files copy.
 */
export function openInMemory(schemaSql: string, seedSql: string): Db {
  const db = new DatabaseSync(":memory:");
  db.exec(schemaSql);
  db.exec(seedSql);
  return wrap(db);
}
