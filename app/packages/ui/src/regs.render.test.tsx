/**
 * T11, MOUNTED (render project): the regs hooks over THE COMMITTED REGS FIXTURE RELEASE (T10). The store opens once and
 * is paired; a refusal is a `failed` state with the store's reason; the day is the Shell's `mapDay`, read at call time;
 * a new day is a new question and a slower answer to the old one never overwrites it; who is fishing is shared.
 *
 * `useRegulations` / `useRegsTarget` are exercised through their T09 seam with a recording stand-in (the stand-in returns
 * a marker, never a regulation answer), and once by default over T09's own `targetFor` / `loadRegsInputs` (T21).
 */
import { afterEach, beforeAll, describe, expect, it } from "vitest";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import type { Day, RegsInputs, RegsTarget, Weekday, Who } from "@app/core";
import type { ItemId } from "@app/data";
import { RegsStoreError, type RegsStore } from "@app/data/regs";
import { openRegsFixture } from "@app/data/release/node";
import {
  DEFAULT_WHO, loadRegsStore, setAnglerProfile, useAnglerProfile, useRegsPage, useRegsStore, useRegsTarget, useRegulations,
  type LoadRegsInputs,
} from "./regs";

afterEach(() => { cleanup(); act(() => setAnglerProfile(DEFAULT_WHO)); });

const VEDDER: RegsTarget = { kind: "part", item: "gnis:3062", partIx: 0 };

/** One fixture loader for the seam tests (opening it decodes and verifies the water file: ~1 s), its store through the cache. */
const LOADER = openRegsFixture();
let STORE: RegsStore;
beforeAll(async () => { STORE = await loadRegsStore(LOADER); }, 30_000);
const fixtureStore = async (): Promise<RegsStore> => STORE;

/** A stand-in for T09's loader: records each ask, answers when the test says. */
function recorder() {
  const asks: { target: RegsTarget; day: Day; weekday: Weekday; year: number; resolve: (v: RegsInputs) => void }[] = [];
  const load: LoadRegsInputs = (_st, target, day, weekday, year) =>
    new Promise<RegsInputs>((resolve) => { asks.push({ target, day, weekday, year, resolve }); });
  const marker = (tag: string) => ({ marker: tag }) as unknown as RegsInputs;
  return { asks, load, marker };
}

describe("useRegsStore", () => {
  it("6. refused against another atlas: failed, with the store's reason", async () => {
    const tiles = "0000000000000000.000000000000";
    const { result } = renderHook(() => useRegsStore(openRegsFixture(), { tiles }));
    await waitFor(() => expect(result.current.state).toBe("failed"), { timeout: 10_000 });
    const e = result.current.error!;
    expect([e instanceof RegsStoreError, (e as RegsStoreError).why, e.message])
      .toEqual([true, "digest", `regs store: built for atlas d768d6e135e62b99, the map is ${tiles} — refused`]);
  });

  it("7. over the fixture: ready, the release's store, paired; re-rendering opens nothing again", async () => {
    const loader = openRegsFixture();
    const { result, rerender } = renderHook(() => useRegsStore(loader));
    await waitFor(() => expect(result.current.state).toBe("ready"), { timeout: 10_000 });
    const st = result.current.value!;
    expect([st.meta.release, st.refused(), loader.opened()]).toEqual([loader.release, null, ["water"]]);
    rerender();
    expect(result.current.value).toBe(st);
    // a second mount of the same loader shares the store
    const two = renderHook(() => useRegsStore(loader));
    await waitFor(() => expect(two.result.current.state).toBe("ready"));
    expect(two.result.current.value).toBe(st);
    expect(loader.opened()).toEqual(["water"]);
  });

  it("10. no loader (no release open): ready(null), nothing opened", async () => {
    const { result } = renderHook(() => useRegsStore(null));
    await waitFor(() => expect(result.current.state).toBe("ready"));
    expect(result.current.value).toBeNull();
  });
});

describe("useRegsTarget / useRegulations (through the T09 seam)", () => {
  it("by default the hooks ask T09 (T21): the Vedder's part, then its inputs on Jun 15 — the store's answer, not a guess", async () => {
    const st = await fixtureStore();
    const t = renderHook(() => useRegsTarget(st, "gnis:3062" as ItemId, null));
    await waitFor(() => expect(t.result.current.state).toBe("ready"));
    expect(t.result.current.value).toEqual(VEDDER);
    const r = renderHook(() => useRegulations(st, VEDDER, new Date(2026, 5, 15)));
    await waitFor(() => expect(r.result.current.state).toBe("ready"), { timeout: 10_000 });
    expect([r.result.current.value!.target, r.result.current.value!.day, r.result.current.value!.item?.name])
      .toEqual([VEDDER, { month: 6, day: 15 }, "Vedder River"]);
  });

  it("useRegsTarget hands the store, item and tapped section to targetFor; disabled without a store or item", async () => {
    const st = await fixtureStore();
    const seen: unknown[] = [];
    const targetFor = async (s: RegsStore, item: string, section: number | null): Promise<RegsTarget> => {
      seen.push([s === st, item, section]);
      return { kind: "part", item, partIx: 0 };
    };
    const { result } = renderHook(() => useRegsTarget(st, "gnis:3062" as ItemId, null, targetFor));
    await waitFor(() => expect(result.current.state).toBe("ready"));
    expect([result.current.value, seen]).toEqual([VEDDER, [[true, "gnis:3062", null]]]);
    const off = renderHook(() => useRegsTarget(null, "gnis:3062" as ItemId, null, targetFor));
    await act(async () => { await Promise.resolve(); });
    expect([off.result.current.state, seen.length]).toEqual(["loading", 1]);
  });

  it("(T53) an unnamed reach (no item): the target is its tapped section, never a part; nothing without a section", async () => {
    const st = await fixtureStore();
    const seen: unknown[] = [];
    const targetFor = async (_s: RegsStore, item: string, section: number | null): Promise<RegsTarget> => {
      seen.push([item, section]);
      return { kind: "part", item, partIx: 0 };
    };
    const { result } = renderHook(() => useRegsTarget(st, null, 634 as never, targetFor));
    await waitFor(() => expect(result.current.state).toBe("ready"));
    expect([result.current.value, seen]).toEqual([{ kind: "section", handle: 634 }, []]);   // the handle given above
    const none = renderHook(() => useRegsTarget(st, null, null, targetFor));
    await act(async () => { await Promise.resolve(); });
    expect([none.result.current.state, seen]).toEqual(["loading", []]);
  });

  it("7–8. asks for the Shell's day (local date, real weekday, its year); a new day is a new question; a stale answer never lands", async () => {
    const st = await fixtureStore();
    const { asks, load, marker } = recorder();
    let mapDay: Date | null = new Date(2026, 5, 15);
    const { result, rerender } = renderHook(() => useRegulations(st, VEDDER, mapDay, load));
    await waitFor(() => expect(asks.length).toBe(1));
    expect(asks.map(({ target, day, weekday, year }) => ({ target, day, weekday, year })))
      .toEqual([{ target: VEDDER, day: { month: 6, day: 15 }, weekday: "Monday", year: 2026 }]);

    mapDay = new Date(2026, 4, 15);
    rerender();
    await waitFor(() => expect(asks.length).toBe(2));
    expect(result.current.state).toBe("loading");
    expect({ day: asks[1]!.day, weekday: asks[1]!.weekday }).toEqual({ day: { month: 5, day: 15 }, weekday: "Friday" });

    await act(async () => { asks[1]!.resolve(marker("may15")); });
    await waitFor(() => expect(result.current.state).toBe("ready"));
    await act(async () => { asks[0]!.resolve(marker("jun15")); });       // the slower, older answer arrives last
    expect(result.current.value).toEqual(marker("may15"));

    rerender();                                                          // same day, same target: no new ask
    expect(asks.length).toBe(2);
  });

  it("mapDay null is today, read at call time", async () => {
    const st = await fixtureStore();
    const { asks, load } = recorder();
    renderHook(() => useRegulations(st, { kind: "section", handle: 634 }, null, load));
    await waitFor(() => expect(asks.length).toBe(1));
    const now = new Date();
    expect([asks[0]!.target, asks[0]!.day, asks[0]!.year])
      .toEqual([{ kind: "section", handle: 634 }, { month: now.getMonth() + 1, day: now.getDate() }, now.getFullYear()]);   // as given
  });

  it("no store or no target: nothing asked", async () => {
    const { asks, load } = recorder();
    const a = renderHook(() => useRegulations(null, VEDDER, null, load));
    const st = await fixtureStore();
    const b = renderHook(() => useRegulations(st, null, null, load));
    await act(async () => { await Promise.resolve(); });
    expect([a.result.current.state, b.result.current.state, asks.length]).toEqual(["loading", "loading", 0]);
  });
});

describe("useRegsPage", () => {
  it("no inputs: no page", () => {
    const { result } = renderHook(() => useRegsPage(null, DEFAULT_WHO, { month: 6, day: 15 }));
    expect(result.current).toBeNull();
  });
});

describe("useAnglerProfile", () => {
  it("9. starts as DEFAULT_WHO; a change in one mount is seen by another", () => {
    const a = renderHook(() => useAnglerProfile());
    const b = renderHook(() => useAnglerProfile());
    expect([a.result.current[0], b.result.current[0]]).toEqual([DEFAULT_WHO, DEFAULT_WHO]);
    const u16: Who = { age: "u16", residency: "non_resident" };
    act(() => a.result.current[1](u16));
    expect([a.result.current[0], b.result.current[0]]).toEqual([u16, u16]);
  });
});
