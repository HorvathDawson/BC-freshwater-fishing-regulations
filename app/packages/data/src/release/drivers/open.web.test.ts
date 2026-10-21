/**
 * P11: the web opener does not keep a FAILED engine. sql.js's wasm is a network fetch on the web; one that fails (a dropped
 * connection at start) must not leave every later file open failing for the life of the page. sql.js is wrapped so the
 * opener's first load fails and later ones are the real engine.
 */
import { describe, expect, it, vi } from "vitest";

type Init = (o: unknown) => Promise<{ Database: new () => { run(s: string): void; export(): Uint8Array; close(): void } }>;
const loads = vi.hoisted(() => ({ n: 0 }));
vi.mock("sql.js", async (real) => {
  const actual = ((await real()) as { default: Init }).default;
  return { default: (o: unknown) => (++loads.n === 1 ? Promise.reject(new Error("wasm: network")) : actual(o)) };
});

import { releaseOpener } from "./open.web";

const WASM = new URL("../../../../../node_modules/sql.js/dist/", import.meta.url).pathname;

describe("open.web: a failed engine load is not kept", () => {
  it("the next open, from the same place, loads the engine again and opens", async () => {
    const SQL = await (await vi.importActual<{ default: Init }>("sql.js")).default({
      locateFile: (f: string) => `${WASM}${f}` });
    const made = new SQL.Database();
    made.run("CREATE TABLE meta (k TEXT, v TEXT); INSERT INTO meta VALUES ('format', 'core/1')");
    const bytes = made.export();
    made.close();

    const open = releaseOpener({ wasmBase: WASM, installed: () => null });
    await expect(open("core", async () => bytes)).rejects.toThrow("wasm: network");
    const db = await open("core", async () => bytes);
    expect(await db.all("SELECT k, v FROM meta")).toEqual([{ k: "format", v: "core/1" }]);
    expect(loads.n).toBe(2);
    db.close?.();
  });
});
