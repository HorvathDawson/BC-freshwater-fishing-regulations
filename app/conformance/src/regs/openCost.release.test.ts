/**
 * (T286) WHAT ONE TAP COSTS IN SQLITE CALLS, on the live release (skipped without one). On a phone every call is a
 * native round trip through T264's one-at-a-time queue, so the count is the time: before T286 a tap's regulations took
 * 116 calls for a small stream, 222 for the Fraser and 83 for a lake; batched (`reads.ts` `batched`), a few dozen. Counted
 * for the three cases the ticket names, every file the tap reads already open, as the app's are by the time a reader
 * taps. The budgets pin the batching: a read that goes back to one row per call fails here. Time on the emulator is the
 * test bed's (`run.mjs regs-open`).
 */
import { describe, expect, it } from "vitest";
import { tapDay } from "@app/core";
import type { Db } from "@app/data";
import { makeReleaseSource } from "@app/data/release";
import { itemsOfSections, loadRawWaterGroup, loadRegsInputs, openRegsStore, targetFor } from "@app/data/regs";
import { liveRelease, openReleaseDir } from "../fixtureRelease";

const LIVE = liveRelease();
/** Calls for target + inputs (the page's own reads) and for the whole tap (+ the water sheet and its group). */
const CASES = [
  { id: "small stream (Norrish Creek)", item: "gnis:23402", page: 30, all: 50 },
  { id: "the Fraser (17 parts)", item: "gnis:39325", page: 40, all: 60 },
  { id: "a lake (Cultus Lake)", item: "wbk:329083342", page: 30, all: 50 },
] as const;

describe.skipIf(!LIVE)(`T286 a tap's SQLite calls on the live release (${LIVE ?? "absent"})`, () => {
  let calls = 0;
  const counted = (_: string, d: Db): Db => ({
    all: (sql, ...p) => { calls += 1; return d.all(sql, ...p); },
    get: (sql, ...p) => { calls += 1; return d.get(sql, ...p); },
    close: () => d.close(),
  });

  it.each(CASES)("$id: within its budget", async (c) => {
    const loader = openReleaseDir(LIVE!, { db: counted });
    const store = await openRegsStore(loader), source = makeReleaseSource(loader);
    await Promise.all([store.statics(), store.rules(), store.lics()]);
    // the files open, and the tapped section (the water's first) known, as at the tap — from a SECOND store, so the
    // first's caches answer nothing for the one counted
    const ord = (await store.itemOrd(c.item))!;
    const sid = (await store.navRows(ord)).sections[0]!.sid;
    const st = await openRegsStore(loader);
    await Promise.all([st.statics(), st.rules(), st.lics(), st.sid(), st.core(), st.water()]);
    for (const p of await store.parts(ord)) if (p.akey !== null) await st.region(p.akey);
    calls = 0;
    const d = { month: 6, day: 15 }, y = 2026;
    await loadRegsInputs(st, await targetFor(st, c.item, sid), d, tapDay(d, y).weekday, y);
    const page = calls;
    await Promise.all([source.water(c.item as never), loadRawWaterGroup(st, c.item)]);
    console.log(`[T286] ${c.id}: page ${page} calls, whole tap ${calls}`);
    expect([page <= c.page, calls <= c.all]).toEqual([true, true]);
  });

  it("the prefetch's waters on screen: every section of the three waters names its water, in one read", async () => {
    const loader = openReleaseDir(LIVE!, { db: counted });
    const st = await openRegsStore(loader);
    const sids: number[] = [], want = new Map<number, string>();
    for (const c of CASES) {
      for (const s of (await st.navRows((await st.itemOrd(c.item))!)).sections) { sids.push(s.sid); want.set(s.sid, c.item); }
    }
    await st.core();
    calls = 0;
    const got = await itemsOfSections(st, sids);
    expect([calls, got.size, [...want].every(([sid, item]) => got.get(sid) === item)]).toEqual([1, want.size, true]);
  });
});
