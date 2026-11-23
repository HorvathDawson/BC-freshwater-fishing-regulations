/**
 * (T867 review) THE INBOX READS WHAT IT SHOWS, only when the sheet is in front, in the real Shell on the fixture: on a fresh
 * install with a notice not on the map, accepting the first-run notice must not read it before the walkthrough has come
 * and gone — the sheet opens after the tour, and the notice is still New there.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import { httpInSeason } from "@app/data";
import { INSEASON_LINKS_FORMAT } from "@app/core";
import { HOME, LayoutWidthContext } from "@app/ui";
import { LIGHT } from "./theme";

vi.mock("@app/map", async (orig) => ({ ...(await orig<Record<string, unknown>>()), Map: () => null }));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
vi.mock("./BarControls", async () => await import("./BarControls.native"));
const { Shell } = await import("./Shell");
afterEach(cleanup);

const memory = (v: string | null = null) => ({ v, read() { return this.v; }, write(t: string) { this.v = t; } });
const SRC = "https://www2.gov.bc.ca/in-season";
const row = { id: "is-b", region: "8", water: "Okanagan River", effective: "Nov 1, 2026", from: null, to: null,
              change: "Closed to all fishing", status: "new", firstSeen: "2026-11-01T00:00:00Z", changedAt: "2026-11-01T00:00:00Z" };
const feed = { format: "inseason-feed/1", fetchedAt: "2026-11-20T00:00:00Z", source: SRC, removed: [], rows: [row] };
const serve = (async (u: string) => (String(u).endsWith("index.json") ? new Response(JSON.stringify(feed))
  : new Response(JSON.stringify({ format: INSEASON_LINKS_FORMAT, notices: {} })))) as unknown as typeof fetch;

function mount(extra: Record<string, unknown>) {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
  return render(<LayoutWidthContext.Provider value={390}><GestureHandlerRootView>
    <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
           tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={HOME} spots={[]} onSaveSpot={() => {}}
           locate={null} {...extra} />
  </GestureHandlerRootView></LayoutWidthContext.Provider>);
}
const slidOut = () => act(() => {
  for (const el of document.querySelectorAll("div"))
    for (const type of ["animationend", "webkitAnimationEnd"]) el.dispatchEvent(new Event(type, { bubbles: true }));
});
const settle = async () => { await act(async () => { await new Promise((r) => setTimeout(r, 300)); }); slidOut(); };
const press = (id: string) => act(() => { fireEvent.click(screen.getByTestId(id)); });

describe("(T867 review) opening reads only when the sheet is in front", () => {
  it("a notice not on the map is not read under the first-run notice or the walkthrough; it is New when the sheet opens after them", async () => {
    const inbox = memory();
    mount({ inSeason: httpInSeason("http://x/feeds/inseason", memory(), serve), inSeasonInbox: inbox, inSeasonSeen: null,
            firstRun: memory(null), walkthrough: memory(null) });
    await settle();
    expect(screen.getByTestId("first-run-notice")).toBeTruthy();
    press("first-run-accept");
    await settle();
    // the walkthrough is up (or coming): nothing has been read, the sheet is not in front
    expect(await screen.findByTestId("guide-skip", {}, { timeout: 4000 })).toBeTruthy();
    expect(JSON.parse(inbox.v!).items.map((i: { id: string; read: boolean }) => [i.id, i.read])).toEqual([["is-b", false]]);
    expect(screen.queryByTestId("inbox")).toBeNull();
    press("guide-skip");
    await settle();
    // the tour is over: the sheet opens by itself, the notice listed as New, and only now read
    expect(screen.getAllByTestId("inbox-group").map((g) => g.textContent)).toEqual(["New"]);
    expect(screen.getAllByTestId("inbox-row").map((r) => r.getAttribute("aria-label"))).toEqual([
      "Unread. New: Okanagan River, Region 8, first seen Nov 1. Not on the map yet. Closed to all fishing"]);
    expect(JSON.parse(inbox.v!).items.map((i: { read: boolean }) => i.read)).toEqual([true]);
  });
});
