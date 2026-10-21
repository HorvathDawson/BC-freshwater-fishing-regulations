/**
 * SEARCH'S PREVIEW MAP (owner, 2026-10-20): the map under the results is OPEN and shows what the reader is looking at;
 * a tap on a row shows that water there without leaving; the eye goes to it on the main map; and where the reader was
 * survives the trip (the Shell holds it — `memory`).
 *
 * The renderer is stubbed (MapLibre needs WebGL): what is asserted is what the screen SENDS the map — the camera's
 * subject and box (`fit`) and the sections lit (`highlight`) — and the caption it draws over it.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId, RegsSource } from "@app/data";
import { NO_SEARCH, type SearchMemory } from "@app/ui";
import { LIGHT } from "./theme";

const sent: { fit: unknown; highlight: unknown }[] = [];
vi.mock("@app/map", async (orig) => ({
  ...(await orig<Record<string, unknown>>()),
  Map: (p: { fit: unknown; highlight: unknown }) => { sent.push({ fit: p.fit, highlight: p.highlight }); return null; },
}));

const { SearchScreen } = await import("./SearchScreen");

afterEach(() => { cleanup(); sent.length = 0; });

const TILES = { atlas: "a", basemap: "b" } as never;
const CHILLIWACK = "gnis:8634" as ItemId, JEPERSON = "gnis:11481" as ItemId;
const last = () => sent[sent.length - 1]!;
const settle = () => act(async () => { await new Promise((r) => setTimeout(r, 260)); });

async function typed(source: RegsSource, q: string, extra: Partial<Parameters<typeof SearchScreen>[0]> = {}) {
  const onPick = vi.fn();
  const view = render(<SearchScreen source={source} palette={LIGHT} onPick={onPick} tiles={TILES} {...extra} />);
  fireEvent.change(screen.getByLabelText("Search for a river, lake or town"), { target: { value: q } });
  await settle();
  await settle();
  return { onPick, view };
}

describe("<SearchScreen> preview map", () => {
  it("is OPEN under the results, and opens on the best match by its outline, lit whole", async () => {
    const source = makeFixtureSource();
    await typed(source, "Chilliwack River");
    const w = (await source.water(CHILLIWACK))!;
    expect(last()).toEqual({
      fit: { key: `item:${CHILLIWACK}#0`, bbox: [-122.0, 49.05, -121.4, 49.12], refine: false },
      highlight: w.sections });
    // the row's name and the caption over the map: the water by name, never "cannot place it"
    expect(screen.getAllByText("Chilliwack River").length).toBe(2);
    expect(screen.queryByText(/cannot place/)).toBeNull();
  });

  it("a TAP on a row shows that water in the preview, selects it, and never leaves", async () => {
    const source = makeFixtureSource();
    const { onPick } = await typed(source, "ch");
    fireEvent.click(screen.getByRole("button", { name: /^Show Jeperson Side Channel/ }));
    await settle();
    const w = (await source.water(JEPERSON))!;
    expect(last()).toEqual({
      fit: { key: `item:${JEPERSON}#0`, bbox: [-121.95, 49.17, -121.93, 49.18], refine: false },
      highlight: w.sections });
    expect(onPick).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: /^Show Jeperson Side Channel/ }).getAttribute("aria-pressed"))
      .toBe("true");
    // the caption names the water shown — never "cannot place it"
    expect(screen.queryByText(/cannot place/)).toBeNull();
    expect(screen.getAllByText("Jeperson Side Channel").length).toBe(2);
  });

  it("the EYE, which only the selected row carries, sends that water to the main map", async () => {
    const source = makeFixtureSource();
    const { onPick } = await typed(source, "ch");
    expect(screen.queryByRole("button", { name: /^View Jeperson Side Channel/ })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: /^Show Jeperson Side Channel/ }));
    fireEvent.click(screen.getByRole("button", { name: /^View Jeperson Side Channel/ }));
    expect(onPick.mock.calls).toEqual([[JEPERSON]]);
  });

  it("the FIT control re-sends the camera to the whole subject, and only when there is one", async () => {
    const source = makeFixtureSource();
    render(<SearchScreen source={source} palette={LIGHT} onPick={() => {}} tiles={TILES} />);
    expect(screen.queryByRole("button", { name: "Show all of it" })).toBeNull();
    fireEvent.change(screen.getByLabelText("Search for a river, lake or town"), { target: { value: "Chilliwack River" } });
    await settle();
    await settle();
    fireEvent.click(screen.getByRole("button", { name: "Show all of it" }));
    expect((last().fit as { key: string }).key).toBe(`item:${CHILLIWACK}#1`);
  });

  it("before anything is typed the preview says how it works", () => {
    render(<SearchScreen source={makeFixtureSource()} palette={LIGHT} onPick={() => {}} tiles={TILES} />);
    expect(screen.getByText("Tap a result to see it here · the eye shows it on the map")).toBeTruthy();
    expect(last()).toEqual({ fit: null, highlight: [] });
  });

  it("WHERE THE READER WAS survives leaving: a remount on the held memory has the query, the row and the preview", async () => {
    const source = makeFixtureSource();
    let held: SearchMemory = NO_SEARCH;
    const hold = (m: SearchMemory) => { held = m; };
    const first = render(<SearchScreen source={source} palette={LIGHT} onPick={() => {}} tiles={TILES}
                                       memory={held} onMemory={(m) => { hold(m); first.rerender(
                                         <SearchScreen source={source} palette={LIGHT} onPick={() => {}} tiles={TILES}
                                                       memory={m} onMemory={hold} />); }} />);
    fireEvent.change(screen.getByLabelText("Search for a river, lake or town"), { target: { value: "ch" } });
    await settle();
    await settle();
    fireEvent.click(screen.getByRole("button", { name: /^Show Jeperson Side Channel/ }));
    expect(held).toEqual({ q: "ch",
      pick: { selected: { kind: "water", item: JEPERSON, name: "Jeperson Side Channel" }, town: null } });
    first.unmount();
    sent.length = 0;
    render(<SearchScreen source={source} palette={LIGHT} onPick={() => {}} tiles={TILES} memory={held} onMemory={hold} />);
    await settle();
    await settle();
    expect((screen.getByLabelText("Search for a river, lake or town") as HTMLInputElement).value).toBe("ch");
    expect(screen.getByRole("button", { name: /^Show Jeperson Side Channel/ }).getAttribute("aria-pressed"))
      .toBe("true");
    expect((last().fit as { key: string }).key).toBe(`item:${JEPERSON}#0`);
  });
});
