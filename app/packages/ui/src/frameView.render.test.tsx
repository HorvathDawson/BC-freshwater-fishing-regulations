/**
 * (T1034, 0.9.0 device pass) A FRAME MOVED TO ANOTHER WATER NEVER PAIRS ITS PART WITH THE WATER BEFORE'S NAV. The Zymoetz
 * (6 parts) on its Map, "Flows into Skeena River" pressed: the frame turned to the Skeena's part 10 while `useAsync` still
 * held the Zymoetz's `navRows` for the render after the key changed, and `middleOf` threw "frame: part 10 has no
 * part_geo row" (also a Sumas / Skeena link opened over the Vedder). The waters here are built by the test: A has fewer
 * parts than B's part, and B's nav arrives after the frame has moved.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import type { ItemId } from "@app/data";
import type { NavRows, RegsStore } from "@app/data/regs";
import { useFrameView, type FrameKey } from "./frameView";

afterEach(cleanup);

const A = "gnis:1" as ItemId, B = "gnis:2" as ItemId;
/** A water of `n` parts: part p holds sections base+2p and base+2p+1, its middle the second, at (base+p, 50). */
function nav(n: number, base: number): NavRows {
  const parts = Array.from({ length: n }, (_, p) => p);
  return {
    sections: parts.flatMap((p) => [{ sid: base + 2 * p, partIx: p }, { sid: base + 2 * p + 1, partIx: p }]),
    geo: parts.map((p) => ({ partIx: p, midSid: base + 2 * p + 1, mid: [base + p, 50] as const,
                             box: [base + p - 0.1, 49.9, base + p + 0.1, 50.1] as const })),
    marks: [], exit: null, continuesAs: null, continuesFrom: null,
  };
}
const frame = (item: ItemId, part: number): FrameKey => ({ item, part, tapSid: null, at: null });

function storeWithLateB() {
  let arrive!: (rows: NavRows) => void;
  const late = new Promise<NavRows>((res) => { arrive = res; });
  const store = {
    itemOrd: async (i: string) => (i === A ? 1 : i === B ? 2 : null),
    navRows: (ord: number) => (ord === 1 ? Promise.resolve(nav(2, 100)) : late),
  } as unknown as RegsStore;
  return { store, arrive };
}

describe("(T1034) a frame moved to another water waits for that water's nav", () => {
  it("A (2 parts) → B's part 4: loading until B's nav is read, then B's middle — never A's rows for B's part", async () => {
    const { store, arrive } = storeWithLateB();
    // every render's answer, not only the last: the render right after the switch is the one that paired the waters
    const seen: ReturnType<typeof useFrameView>[] = [];
    const r = renderHook(({ f }) => { const v = useFrameView(store, f); seen.push(v); return v; },
                         { initialProps: { f: frame(A, 1) } });
    await waitFor(() => expect(r.result.current.state).toBe("ready"));
    expect(r.result.current.value).toEqual({ target: { kind: "part", item: A, partIx: 1 }, part: 1, section: 103,
                                             here: { lon: 101, lat: 50 } });
    seen.length = 0;
    r.rerender({ f: frame(B, 4) });
    const beforeB = seen.splice(0);
    await act(async () => { arrive(nav(6, 200)); });
    // until B's nav is read, every render says "not known yet"; then B's own middle, from B's own rows
    expect(beforeB.length).toBeGreaterThan(0);
    expect(beforeB).toEqual(beforeB.map(() => ({ state: "loading", value: null, error: null })));
    expect(seen.at(-1)).toEqual({ state: "ready", error: null,
      value: { target: { kind: "part", item: B, partIx: 4 }, part: 4, section: 209, here: { lon: 204, lat: 50 } } });
  });

  it("a part genuinely missing on its own water still throws, once that water's nav is the one read", async () => {
    const { store, arrive } = storeWithLateB();
    arrive(nav(6, 200));
    const quiet = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      const r = renderHook(({ f }) => useFrameView(store, f), { initialProps: { f: frame(B, 5) } });
      await waitFor(() => expect(r.result.current.state).toBe("ready"));
      expect(() => r.rerender({ f: frame(B, 9) })).toThrow("frame: part 9 has no part_geo row");
    } finally { quiet.mockRestore(); }
  });
});
