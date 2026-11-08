/**
 * (T508) A FEED TAKEN KEEPS WHAT THE READER OPENED ON THE WATER MAP. A DFO feed taken (`useFeedVersion("dfo")`) hands
 * `useWaterMap` a NEW salmon reading; keyed on its identity, the walk started again — an open boundary mark, the chooser
 * and the parts list closed under the reader's finger. It starts again only when the reading GROUPS the pieces anew.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import { openRegsStore, type RegsStore } from "@app/data/regs";
import { openRegsFixtureWithLines } from "@app/data/release/node";
import type { ItemId } from "@app/data";
import { useWaterMap } from "./waterMap";

afterEach(cleanup);

const C = "gnis:8634" as ItemId;                     // the Chilliwack: four parts, boundary marks
const st: RegsStore = await openRegsStore(openRegsFixtureWithLines());

describe("(T508) the water map keeps an open mark and the parts list through a feed taken", () => {
  it("a new salmon reading that groups the pieces alike keeps them open; one that groups them anew starts again", async () => {
    const same1 = () => "run", same2 = () => "run";
    const r = renderHook(({ k }) => useWaterMap(st, C, 1, null, undefined, false, k), { initialProps: { k: same1 as (sid: number) => string } });
    await waitFor(() => expect(r.result.current.value?.vm).toBeTruthy(), { timeout: 20_000 });
    act(() => { r.result.current.value!.act({ type: "list" }); });
    act(() => { r.result.current.value!.act({ type: "mark", ix: r.result.current.value!.vm.marks[0]!.ix }); });
    const open = () => ({ pop: r.result.current.value?.vm.pop !== null, list: !!r.result.current.value?.vm.list?.open });
    expect(open()).toEqual({ pop: true, list: true });
    // the DFO feed taken: a new reading, the same grouping
    r.rerender({ k: same2 });
    await act(async () => {});
    expect(open()).toEqual({ pop: true, list: true });
    // a reading that splits every section from the next: a new grouping, the walk starts again on the frame's part
    r.rerender({ k: (sid: number) => `s${sid}` });
    await act(async () => {});
    expect(open()).toEqual({ pop: false, list: false });
  }, 30_000);
});
