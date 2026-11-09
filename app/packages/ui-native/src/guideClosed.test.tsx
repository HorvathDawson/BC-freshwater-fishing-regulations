/**
 * (T466) THE WALKTHROUGH'S CLOSED EXAMPLE ON THE COMMITTED REGS FIXTURE RELEASE: found by its water's name and its run's
 * stream key + start (never a raw id), closed all year, and reached the reader's way — the Vedder's water map, ▲ part by
 * part (the walk's "Show me" presses the same ▲) — with the words of its steps inside the controls they ring. The map
 * renderer is stubbed (no WebGL in jsdom); the bar and its buttons are the real component.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { useState } from "react";
import { act, cleanup, render, renderHook, screen, waitFor } from "@testing-library/react";
import { GUIDE_CLOSED, WATER_MAP_WORDS } from "@app/core";
import type { ItemId, NameHit, SectionId, WaterSource } from "@app/data";
import { openRegsFixture } from "@app/data/release/node";
import { openRegsStore, type RegsReads } from "@app/data/regs";
import { LIGHT } from "./theme";
import { CoachContext, useCoachRegistry, type CoachRegistry } from "./CoachMarks";
import { GuideControlContext, useGuideControlHost } from "./guideControls";
import { findGuideClosed } from "./guideClosed";
import { GUIDE_STEPS } from "./Walkthrough";

vi.mock("@app/map", async (orig) => ({ ...(await orig<Record<string, unknown>>()), Map: () => null }));
const { WaterMapView } = await import("./water/WaterMapView");

afterEach(cleanup);
const loader = openRegsFixture();
/**
 * Search's names as the release's `core` holds them (the regs fixture ships no `near` file for the app's own search): every
 * water of that name, with its kind — so the finder's choice by name and kind is what is tested.
 */
const source: Pick<WaterSource, "searchNames"> = {
  searchNames: async (q) => (await (await (await openRegsStore(loader)).core())
    .all("SELECT item_id, name, kind FROM item WHERE name = ? ORDER BY ord", q))
    .map((r) => ({ item: String(r.item_id), name: String(r.name), kind: String(r.kind) }) as unknown as NameHit),
};
const MAP = { at: { atlas: "a", basemap: "b" } as never, theme: "light", camera: { lon: -122, lat: 49, zoom: 8 }, view: "plain" };

describe("(T466) the closed example", { timeout: 60_000 }, () => {
  it("is the Chilliwack's part from the run 380887781 at 23325 m, closed all year in the release", async () => {
    const c = await findGuideClosed(source, await openRegsStore(loader));
    expect(c).toMatchObject({ name: GUIDE_CLOSED.name,
      label: "100 m below the Slesse Creek confluence → Chilliwack River Ecological Reserve boundary" });
    const store = await openRegsStore(loader);
    const ord = (await store.itemOrd(c!.item as ItemId))!;
    expect((await store.parts(ord))[c!.part]!.facts!.closed_all_year).toBe(true);
    expect((await store.partLines(ord)).some((r) => r.partIx === c!.part && r.blk === GUIDE_CLOSED.blk && r.m0 === GUIDE_CLOSED.from))
      .toBe(true);
  });

  it("is none when the run is not a part closed all year, or not there, or the water is not: never another part", async () => {
    const real = await openRegsStore(loader);
    const open: Pick<RegsReads, "itemOrd" | "parts" | "partLines" | "statics"> = {
      itemOrd: (i) => real.itemOrd(i), partLines: (o) => real.partLines(o), statics: () => real.statics(),
      parts: async (o) => (await real.parts(o)).map((p) => (p.facts ? { ...p, facts: { ...p.facts, closed_all_year: false } } : p)),
    };
    expect(await findGuideClosed(source, open)).toBeNull();
    expect(await findGuideClosed(source, { ...open, parts: (o) => real.parts(o), partLines: async () => [] })).toBeNull();
    expect(await findGuideClosed({ searchNames: async () => [] }, real)).toBeNull();
  });

  it("is reached from the Vedder's map with ▲ (the walk's Show me presses it), its words inside the rings", async () => {
    const closed = (await findGuideClosed(source, await openRegsStore(loader)))!;
    const host = renderHook(() => useGuideControlHost());
    const reg: CoachRegistry = renderHook(() => useCoachRegistry()).result.current;
    const at: { item: string; part: number }[] = [];
    function Route() {
      // the Shell's route as far as the map is concerned: a step is the frame's part, a cross opens the other water
      const [f, setF] = useState({ item: "gnis:3062", part: 0 });
      at.push(f);
      return <WaterMapView loader={loader} item={f.item as ItemId} part={f.part} tapSid={null as SectionId | null} palette={LIGHT}
                           map={MAP} onOpenWater={() => {}} onShowRegs={() => {}} onOtherSection={() => {}}
                           onCross={(item, part) => setF({ item, part })} onPart={(item, part) => setF({ item, part })} />;
    }
    render(<GuideControlContext.Provider value={host.result.current.sink}>
      <CoachContext.Provider value={reg}><Route /></CoachContext.Provider>
    </GuideControlContext.Provider>);
    const here = () => at.at(-1)!;
    let presses = 0;
    while (!(here().item === closed.item && here().part === closed.part) && presses < 6) {
      await waitFor(() => { host.rerender(); expect(host.result.current.values["map:up"]).toBe(true); }, { timeout: 20_000 });
      const was = here();
      act(() => { host.result.current.set("map:up", true); });
      presses += 1;
      await waitFor(() => expect(here()).not.toEqual(was), { timeout: 20_000 });
      expect(screen.queryByTestId("water-map-chooser"), "no fork on the way up").toBeNull();
    }
    expect([here(), presses]).toEqual([{ item: closed.item, part: closed.part }, 5]);   // (T517) the Vedder's two parts, then the Chilliwack's: Vedder ▲, → Chilliwack, ▲ ×3 (mouth first, river order)
    await waitFor(() => expect(screen.getByTestId("water-map-name").textContent).toBe(closed.label), { timeout: 20_000 });
    // the steps' named controls are inside their rings: ▲ "Upstream", and "Show regulations for this part"
    const said = (k: string) => {
      const el = reg.get(k) as unknown as HTMLElement | null;
      return el ? `${el.textContent ?? ""} | ${el.getAttribute("aria-label") ?? ""}`.toLowerCase() : "";
    };
    for (const s of GUIDE_STEPS.filter((x) => x.targets[0] === "map:up" || x.targets[0] === "map:regs"))
      for (const n of s.names ?? []) expect(said(s.targets[0]!), `${s.key} names "${n}"`).toContain(n.toLowerCase());
    expect(said("map:regs")).toContain(WATER_MAP_WORDS.regs.toLowerCase());
  });
});
