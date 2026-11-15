/**
 * (T737, 2026-11-15) WHAT A NAMED WATER'S OPEN WAITS ON, ASKED AS THE WATER IS KNOWN: `warmWaterPage` (geo; a failure
 * left to the page's own read) and `warmedFirst` (the opener's part / solo question tells it
 * first, its answer unchanged).
 */
import { describe, expect, it } from "vitest";
import type { ItemId, SectionId } from "@app/data";
import { warmedFirst, warmUnnamedPage, warmWaterPage } from "./regs";
import { waterOpener } from "./openWater";

const VEDDER = "gnis:3062" as ItemId;

describe("(T737) warmWaterPage", () => {
  it("geo for a named water, nothing for an unnamed reach; a failure never thrown", async () => {
    const asked: string[] = [];
    const loader = {
      manifest: { files: { geo: {}, access: {} } },
      db: async (n: string) => { asked.push(n); throw new Error("offline"); },
    } as unknown as Parameters<typeof warmWaterPage>[0];
    const warm = warmWaterPage(loader);
    warm(null);
    warm(VEDDER);
    await new Promise((r) => setTimeout(r, 0));
    expect(asked).toEqual(["geo"]);
  });
});

describe("(T737, 2026-11-15) warmUnnamedPage + the opener's unnamed hint", () => {
  it("an unnamed tap asks every file its page waits on in the same moment (regs-sid, down, the region's water slice, access)", async () => {
    const asked: string[] = [];
    const loader = {
      manifest: { files: { "regs-sid": {}, down: {}, access: {} } },
      db: async (n: string) => { asked.push(n); throw new Error("offline"); },
      warmSlice: async (n: string) => { asked.push(`${n}-<focus>`); },
      access: async () => { asked.push("access"); return null; },
    } as unknown as Parameters<typeof warmUnnamedPage>[0];
    warmUnnamedPage(loader)();
    // all asked before any of them answered: together, not one after another (a failure never thrown)
    expect(asked).toEqual(["regs-sid", "down", "water-<focus>", "access"]);
    await new Promise((r) => setTimeout(r, 0));
  });

  it("the opener tells it for an unnamed reach's tap, before the route is dispatched; never for a named water", async () => {
    const log: string[] = [];
    const source = { itemForSection: async (sid: number) => (sid === 1 ? null : VEDDER) };
    const opener = waterOpener(source as never, (a) => log.push(`go:${(a as { type: string }).type}`),
                               { unnamed: (sid) => log.push(`warm:${sid}`) });
    await opener.tap(1 as SectionId, null, "regs");
    await opener.tap(2 as SectionId, null, "regs");
    expect(log).toEqual(["warm:1", "go:openWater", "go:openWater"]);
  });
});

describe("(T737) warmedFirst", () => {
  it("tells the water BEFORE the question is asked (its reads run beside), and hands the question's answer on", async () => {
    const log: string[] = [];
    let done!: (v: { part: number }) => void;
    const partOf = warmedFirst((item) => log.push(`warm:${item}`),
      (item: ItemId, sid: number) => { log.push(`ask:${item}:${sid}`); return new Promise<{ part: number }>((r) => { done = r; }); });
    const p = partOf(VEDDER, 7);
    expect(log).toEqual([`warm:${VEDDER}`, `ask:${VEDDER}:7`]);
    done({ part: 2 });
    expect(await p).toEqual({ part: 2 });
    expect(await warmedFirst(undefined, async (i: ItemId) => i)(VEDDER)).toBe(VEDDER);   // no loader: the question alone
  });
});
