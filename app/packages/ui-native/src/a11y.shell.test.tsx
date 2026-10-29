/**
 * T243 — THE APP WITH A SCREEN READER, outside the Regulations view (which `regs/a11y.test.tsx` holds): every press on
 * the map tabs and their controls, the bottom bar, search and its results, the Layers sheet, the Spots tab, a spot, the
 * spot capture, a water's three views, the More tab and every menu page — on a phone and on a desktop — has a name and
 * a box that reaches 44 × 44 (`shellPressAudit`: its `hitProps` box), no labelled container hides words its label
 * does not say, and no wordless picture carries a label without a role (the web never says it: axe aria-prohibited-attr) (`labelAudit`; a row whose label SAYS its words — a source's "Synopsis: No date recorded. Unknown." —
 * reads in full and is allowed). The map renderer is stubbed (no WebGL in jsdom); everything else is the app's, on the
 * fixture. Each rule is pinned by a planted violation at the foot.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Pressable, Text, View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import type { Spot } from "@app/data/spots";
import { HOME, type Route } from "@app/ui";
import { LIGHT } from "./theme";
import { labelAudit, shellPressAudit } from "./regs/testing/a11yAudit";
import { hitProps } from "./regs/hit";

vi.mock("@app/map", async (orig) => ({ ...(await orig<Record<string, unknown>>()), Map: () => null }));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");
afterEach(cleanup);

const VISIT = new Date(2026, 7, 30, 23, 30).getTime();
const TAMIHI: Spot = {
  id: "s1", createdAt: VISIT, visitedAt: VISIT, updatedAt: 0, lat: 49.0974, lon: -121.9675, item: null,
  waterName: null, title: "Tamihi run", notes: "", photos: [], reading: null, weather: null, trace: null, panel: null,
};

function mount(start: Route, width: number) {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: width });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: width > 700 ? 900 : 844 });
  render(<GestureHandlerRootView>
    <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
           tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={start} spots={[TAMIHI]} onSaveSpot={() => {}}
           locate={null} />
  </GestureHandlerRootView>);
}
/** a closed sheet slides out on react-native-web and is gone at the end of its animation, which jsdom never plays */
const slidOut = () => act(() => {
  for (const el of document.querySelectorAll("div"))
    for (const type of ["animationend", "webkitAnimationEnd"]) el.dispatchEvent(new Event(type, { bubbles: true }));
});
const settle = async () => { await act(async () => { await new Promise((r) => setTimeout(r, 250)); }); slidOut(); };
const press = (name: string | RegExp) => act(() => { fireEvent.click(screen.getAllByLabelText(name)[0]!); });

/** what a screen reader would trip on, here: offending presses, and labelled containers that hide words */
function findings(): string[] {
  const p = shellPressAudit(document.body);
  const hid = labelAudit(document.body).filter((x) => !x.label.includes(x.text));
  // a label on an element with no role and no words is not said at all on the web (ARIA prohibits it there; axe's
  // aria-prohibited-attr): a picture — a legend's tick, a bar — needs role "image" for its label to be heard
  const mute = [...document.querySelectorAll("[aria-label]")].filter((e) => !e.getAttribute("role")
    && !(e.textContent ?? "").trim() && !["INPUT", "TEXTAREA", "IMG", "svg"].includes(e.tagName)
    && !e.closest('[aria-hidden="true"]'));
  return [...p.bad.map((b) => `${b.role} "${b.name}": ${b.why}`), ...hid.map((h) => `"${h.label}" hides "${h.text}"`),
          ...mute.map((e) => `"${e.getAttribute("aria-label")}" is on a wordless ${e.tagName.toLowerCase()} with no role: never said`),
          ...(p.checked === 0 ? ["no press at all: the state did not render"] : [])];
}

const WATER = (view: string): Route => ({ mode: "water", from: HOME,
  stack: [{ item: "gnis:8634" as never, part: null, tapSid: null, at: null, view: view as never }] });
const MENU_PAGES = ["Offline maps", "Help", "Glossary", "Sources and licences", "About", "Report a problem"];

/** each state: how to reach it from a start route */
const STATES: [string, Route, (() => Promise<void>) | null][] = [
  ["Map tab", HOME, null],
  ["Conditions tab", { ...HOME, tab: "conditions" }, null],
  ["Spots tab", { ...HOME, tab: "spots" }, null],
  ["a spot", { ...HOME, tab: "spots", spot: "s1" as never }, null],
  ["spot capture at a point", { ...HOME, adding: { at: { lat: 49.0974, lon: -121.9675 } } }, null],
  ["More tab", { ...HOME, tab: "more" }, null],
  ...MENU_PAGES.map((page): [string, Route, () => Promise<void>] =>
    [`More → ${page}`, { ...HOME, tab: "more" }, async () => { press(page); await settle(); }]),
  ["search with results", HOME, async () => {
    const f = screen.getByLabelText(/search/i);
    act(() => { fireEvent.focus(f); fireEvent.change(f, { target: { value: "Chill" } }); });
    await settle();
  }],
  ["Layers sheet", HOME, async () => { press("Choose layers"); await settle(); }],
  ["Layers sheet on Conditions", { ...HOME, tab: "conditions" }, async () => { press("Choose layers"); await settle(); }],
  ...["regulations", "conditions", "map"].map((v): [string, Route, null] => [`a water: ${v}`, WATER(v), null]),
];

describe.each([["phone", 390], ["desktop", 1280]] as const)("(T243) %s: every press named and reaching 44 × 44", (_l, width) => {
  for (const [name, start, go] of STATES)
    it(name, async () => {
      mount(start, width);
      await settle();
      if (go) await go();
      expect(findings()).toEqual([]);
    });
});

describe("(T243) the audit catches what it is for", () => {
  it("an unnamed press, a press with no declared box, one that falls short, and a container label hiding words", () => {
    render(<View>
      <Pressable accessibilityRole="button" {...hitProps({ w: 44, h: 44 })}><View /></Pressable>
      <Pressable accessibilityRole="button" accessibilityLabel="No box"><Text>No box</Text></Pressable>
      <Pressable accessibilityRole="switch" accessibilityLabel="Short" aria-checked {...{ dataSet: { hitslop: "30 30 0 0 0 0" } }}>
        <Text>Short</Text></Pressable>
      <View accessibilityLabel="Card"><Text>words a reader never hears</Text></View>
      <View accessibilityLabel="Synopsis: up to date"><Text>Synopsis</Text></View>
      <View accessibilityLabel="3 gauges near the 10th percentile"><View /></View>
    </View>);
    expect(findings()).toEqual([
      'button "": no accessible name',
      'button "No box": no data-hitslop: give it hitProps(its box)',
      'switch "Short": reaches 30×30',
      '"Card" hides "words a reader never hears"',
      '"3 gauges near the 10th percentile" is on a wordless div with no role: never said',
    ]);
  });
});
