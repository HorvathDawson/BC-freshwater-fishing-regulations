/**
 * (T747, owner 2026-11-14 on the staging site: "i clicked search skeena then clicked view skeena river then it went to
 * this it is the section view but it is fully zoomed out instead of on the first section") THE WATER'S MAP FRAMES THE
 * PART ITS BAR NAMES, whatever way it was reached — through the real Shell on the fixture: a search result's Open, the
 * Regulations view then the Map tab, and the part's own frame (a link or a tap opens at its part). The bar's part line, the
 * corner button's words and the camera the water's map is handed are compared; ⤢ "Whole river" frames the whole water and
 * says "This part". On a phone and on a wide screen alike (the desktop strip is the one the owner saw).
 * The map renderer is a stub that writes down every framing it is handed, as in Search.shell.test.tsx.
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { useRef } from "react";
import { Text, View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { SEARCH_BAR } from "@app/core";
import type { ItemId } from "@app/data";
import { makeFixtureSource } from "@app/data/fixture";
import { HOME, openWater, reduce, type Route } from "@app/ui";
import { LIGHT } from "./theme";

type Fit = { key: string; bbox: unknown } | null | undefined;
const seen = vi.hoisted(() => ({ fits: [] as { key: string; bbox: unknown }[] }));
vi.mock("./MapScreen", () => ({
  MapScreen: (p: { fit?: Fit }) => {
    const f = p.fit;
    const last = useRef<string | null>(null);
    const said = f ? `${f.key} ${JSON.stringify(f.bbox)}` : "none";
    if (f && last.current !== said) { last.current = said; seen.fits.push({ key: f.key, bbox: f.bbox }); }
    return <View><Text>{`fit ${f?.key ?? "none"}`}</Text></View>;
  },
}));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");
const { openRegsFixtureWithLines } = await import("@app/data/release/node");

const CHILLIWACK = "gnis:8634" as ItemId;
const size = (w: number, h: number) => {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: w });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: h });
  window.dispatchEvent(new Event("resize"));
};
afterEach(() => { cleanup(); seen.fits.length = 0; size(390, 844); });
beforeAll(() => size(390, 844));

function mount(start: Route = HOME) {
  render(
    <GestureHandlerRootView>
      <Shell source={makeFixtureSource()} loader={openRegsFixtureWithLines()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={start} systemBack={() => ({ sync: () => {}, dispose: () => {} })}
             feed={{ index: async () => ({ fetchedAt: new Date().toISOString(), stations: {} }) } as never}
             spots={[]} onSaveSpot={async () => {}} />
    </GestureHandlerRootView>);
}
const settle = () => act(async () => { await new Promise((r) => setTimeout(r, 260)); });
const field = () => screen.getByLabelText(SEARCH_BAR.label) as HTMLInputElement;

/** What the water's Map shows: its bar's part line, the corner button's words, and the camera it was last handed. */
async function drawn() {
  await waitFor(() => expect(screen.queryByTestId("water-map-whole")).not.toBeNull(), { timeout: 20_000 });
  await settle();
  const fit = seen.fits.filter((f) => f.key.startsWith(`${CHILLIWACK}:`)).at(-1) ?? null;
  return { where: screen.getByTestId("water-map-where").textContent, button: screen.getByTestId("water-map-whole").textContent,
           fit };
}

const ways = {
  async "search, the result's Open"() {
    mount();
    act(() => { fireEvent.focus(field()); });
    fireEvent.change(field(), { target: { value: "chilliwack river" } });
    await settle(); await settle();
    fireEvent.click(screen.getByRole("button", { name: /^Show Chilliwack River/ }));
    await settle();
    fireEvent.click(screen.getByRole("button", { name: /^Open Chilliwack River/ }));
    return drawn();
  },
  async "search, Open, Regulations, then the Map tab"() {
    await ways["search, the result's Open"]();
    fireEvent.click(screen.getAllByRole("tab").find((t) => /Regulations/.test(t.textContent ?? ""))!);
    await settle();
    fireEvent.click(screen.getAllByRole("tab").find((t) => /^Map$/.test(t.textContent ?? ""))!);
    return drawn();
  },
};

describe("(T747) the water's Map frames the part its bar names, on every way in", () => {
  for (const [label, w, h] of [["a phone", 390, 844], ["a wide desktop screen", 1512, 738]] as const) {
    it(`on ${label}: a search Open draws what the part's own frame draws (part framed, "⤢ Whole river"); ⤢ frames the whole`, async () => {
      size(w, h);
      const got: Record<string, unknown> = {};
      for (const [way, run] of Object.entries(ways)) { got[way] = await run(); cleanup(); seen.fits.length = 0; }
      // the part the bar names, opened by its own frame (a link's place or a tap opens at its part)
      const named = /^Part (\d+) of/.exec((got["search, the result's Open"] as { where: string }).where);
      expect(named).not.toBeNull();
      mount(reduce(HOME, openWater({ item: CHILLIWACK, part: Number(named![1]) - 1, view: "map" })));
      const own = await drawn();
      expect(own.button).toBe("⤢ Whole river");
      expect(own.fit?.key.endsWith(":whole")).toBe(false);
      expect(own.fit?.bbox).not.toBeNull();
      for (const [way, d] of Object.entries(got)) expect({ way, d }).toEqual({ way, d: own });
      // ⤢: the whole water, and the button offers the part back
      fireEvent.click(screen.getByTestId("water-map-whole"));
      const whole = await drawn();
      expect([whole.where, whole.button, whole.fit?.key]).toEqual([own.where, "This part", `${CHILLIWACK}:whole`]);
    }, 60_000);
  }
});
