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
import { clearPanelCache, HOME, type Route } from "@app/ui";
import type { Panel, SectionId, WaterSource } from "@app/data";
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

function mount(start: Route, width: number, extra: Record<string, unknown> = {}) {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: width });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: width > 700 ? 900 : 844 });
  render(<GestureHandlerRootView>
    <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
           tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={start} spots={[TAMIHI]} onSaveSpot={() => {}}
           locate={null} {...extra} />
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
  // (T258) and a labelled row WITH words and no role (a "Synopsis: Unknown." status row) is not said either: react-native-web
  // puts aria-label on a generic div, which ARIA prohibits and a web screen reader skips, so the status words in the label are
  // lost; a `role="group"` keeps the label. (A Text leaf — dir="auto" — carries its own words and is exempt.)
  const mutedRows = [...document.querySelectorAll("div[aria-label]")].filter((e) => !e.getAttribute("role")
    && (e.textContent ?? "").trim() && e.getAttribute("dir") !== "auto" && !e.closest('[aria-hidden="true"]'));
  return [...mutedRows.map((e) => `"${e.getAttribute("aria-label")}" is a labelled row with words and no role: the web drops its label`),
          ...p.bad.map((b) => `${b.role} "${b.name}": ${b.why}`), ...hid.map((h) => `"${h.label}" hides "${h.text}"`),
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

/**
 * (T258) the states T257 was rewriting during T243. Mounted in the Shell where a reader reaches them without a device
 * (the bell sheet, the first-run notice), and on their own, with the real component and the fixtures its own test uses,
 * where jsdom cannot lay the host out (the coach marks need measured controls; the day sheet, the sync error sheet and
 * a gauge's Conditions need data the fixture source does not hold). Each must also SHOW its words: a state that drew
 * nothing would pass the audit.
 */
const memory = (v: string | null = null) => ({ v, read() { return this.v; }, write(t: string) { this.v = t; } });

const SEC = 7 as SectionId;
const PANEL: Panel = { areaKm2: 50, members: [
  { station: "LEAD" as never, role: "up", areaKm2: 60, years: { discharge: 40, level: 40 }, regulated: false, sameRiver: true }] };
const GAUGE_SOURCE = {
  panelsFor: async () => new Map([[SEC, PANEL]]),
  panelRoutes: async () => [{ station: "LEAD", name: "Lead Creek near Town", lat: 49, lon: -122, role: "up", path: [SEC] }],
  lakeStationsFor: async () => new Map(), gaugeForSection: async () => null, traceToGauge: async () => [],
  gaugeSeries: async () => null, gaugeParameters: async () => ["discharge", "level"],
  gaugeNow: async () => ({ fetchedAt: Date.now() - 5 * 60_000, value: { discharge: 12.5, level: 1.2, percentile: 0.15, standing: "below", at: "2026-09-04T06:00:00Z" } }),
} as unknown as WaterSource;
const GAUGE_FEED = { index: async () => ({ stations: { LEAD: { percentile: 0.15, parameter: "discharge" as const, discharge: 0.15 } } }) };

async function coachRegistry() {
  const { renderHook } = await import("@testing-library/react");
  const { useCoachRegistry } = await import("./CoachMarks");
  const reg = renderHook(() => useCoachRegistry()).result.current;
  const at: Record<string, [number, number, number, number]> = {
    days: [14, 70, 200, 34], modes: [230, 70, 290, 34], map: [0, 0, 1024, 700], "tab:conditions": [300, 720, 120, 48],
    "tab:runs": [420, 720, 120, 48], sync: [400, 10, 44, 44], bell: [356, 10, 44, 44] };
  for (const [k, r] of Object.entries(at)) reg.set(k, { measureInWindow: (cb: (...n: number[]) => void) => cb(...r) } as never);
  return reg;
}

/** state, how it is put on screen, and the words that prove it is there */
const STATES_T258: [string, (width: number) => Promise<void>, RegExp][] = [
  ["the bell sheet (Notifications)", async (w) => {
    mount(HOME, w); await settle();
    act(() => { fireEvent.click(screen.getByTestId("inseason-bell")); }); await settle();
  }, /Notifications/],
  ["the first-run notice", async (w) => { mount(HOME, w, { firstRun: memory(null) }); await settle(); }, /Before you fish/],
  ["the day calendar sheet", async () => {
    const { DaySheet } = await import("./regs/DaySheet");
    const { dayRange } = await import("@app/core");
    render(<DaySheet open onClose={() => {}} palette={LIGHT} day={null} onDay={() => {}} range={dayRange(new Date(2026, 8, 4), null)} now={new Date(2026, 8, 4, 12)} />);
    await settle();
  }, /Showing /],
  ["the sync error sheet", async () => {
    const { SyncErrorSheet } = await import("./SyncErrorSheet");
    render(<SyncErrorSheet open palette={LIGHT} checking={false} onRetry={() => {}} onClose={() => {}}
      failed={[{ k: "gauges", checkedAt: Date.now() - 60_000, updatedAt: null, error: "HTTP 503", failedAt: Date.now() - 60_000, offline: false }]} />);
    await settle();
  }, /Retry/],
  ["the coach marks (walkthrough)", async () => {
    const { WalkthroughSheet } = await import("./Walkthrough");
    const reg = await coachRegistry();
    render(<WalkthroughSheet state={{ open: true, pending: false, finish: () => {}, reopen: () => {} }} palette={LIGHT} registry={reg} />);
    await screen.findByTestId("coach-spotlight", {}, { timeout: 3000 });
  }, /Next/],
  ["a water's Conditions with a gauge, donors and the day", async () => {
    clearPanelCache();
    const { ConditionsPanel } = await import("./ConditionsPanel");
    render(<ConditionsPanel source={GAUGE_SOURCE} section={SEC} palette={LIGHT} feed={GAUGE_FEED} scroll={false} day={new Date(2026, 8, 4, 12)} />);
    await screen.findByText("AT LEAD CREEK NEAR TOWN");
  }, /Lead Creek/],
  ["(T284) a spot's three blocks: that day, what you added, the next few days", async (w) => {
    Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: w });
    Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 900 });
    Object.defineProperty(window, "innerWidth", { configurable: true, value: w });
    act(() => { window.dispatchEvent(new Event("resize")); });
    const { SpotScreen } = await import("./SpotScreen");
    const day = (date: string, best: boolean) => ({ date, top: "Sat", sub: "31 Oct", score: "72%", read: "Similar",
      basis: "weather only", best, reasons: ["Air temperature: 4° cooler", "Wind: 3 km/h calmer"], label: "Sat, 31 Oct: 72%, similar to your visit.", why: [] });
    const photo = "data:image/gif;base64,R0lGODlhAQABAAAAACw=";
    render(<SpotScreen spot={{ ...TAMIHI, notes: "Three rainbow below the bridge.", photos: ["photos/s1-1.png", "photos/s1-2.png"] }}
      palette={LIGHT} onBack={() => {}} onEdit={() => {}} onDelete={() => {}} onShowOnMap={() => {}}
      photoStore={{ uri: async () => photo }}
      likeVisit={{ mode: "like", best: "Closest to your visit", hint: null,
        heading: "Like your visit?", intro: "How alike each of the next five days looks.", note: null,
        visit: { top: "Your visit", sub: "30 Aug", label: "Your visit, 30 Aug: the day the others are compared with." },
        days: [day("2026-10-30", false), day("2026-10-31", true)], age: "Forecast checked just now." }} />);
    await settle();
    // phone: one column (the three blocks share a parent); desktop: the next days beside the other two
    const col = (id: string) => screen.getByTestId(id).parentElement!;
    const same = col("spot-that-day") === col("spot-next-days");
    expect(same).toBe(w < 880);
    expect(col("spot-that-day")).toBe(col("spot-added"));
    // a photograph opens full screen: named, with next / close; the next one is reached, the first has no previous
    press("Photo 1 of 2"); await settle();
    expect(screen.queryByLabelText("Previous photo")).toBeNull();
    press("Next photo"); await settle();
    expect(screen.getAllByLabelText("Photo 2 of 2").length).toBeGreaterThan(0);
    expect(screen.queryByLabelText("Next photo")).toBeNull();
    press("Close photo"); await settle();
    expect(screen.queryByTestId("photo-viewer")).toBeNull();
    press("Photo 2 of 2"); await settle();   // left open: the audit below sees the viewer too
  }, /That day[\s\S]*What you added[\s\S]*The next few days/],
  ["(T294) a spot's visits, the shown visit's rating, and the days predicted from rated visits", async (w) => {
    Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: w });
    Object.defineProperty(window, "innerWidth", { configurable: true, value: w });
    act(() => { window.dispatchEvent(new Event("resize")); });
    const { SpotScreen } = await import("./SpotScreen");
    const { visitsView } = await import("@app/ui");
    const { addVisit } = await import("@app/data/spots");
    const t = TAMIHI.visitedAt;
    const v = (id: string, d: number, rating: number) => ({ id, createdAt: t + d, visitedAt: t + d, rating, notes: "",
      photos: [], reading: null, weather: null, trace: null, panel: null });
    const spot = addVisit(addVisit({ ...TAMIHI, rating: 5 }, v("v1", 86400e3 * 10, 2)), v("v2", 86400e3 * 20, 4));
    const day = (date: string, best: boolean) => ({ date, top: "Sat", sub: "31 Oct", score: "4.2★", read: "Good",
      basis: "From 3 rated visits", best, reasons: ["Most like your 5★ visit on 30 Aug (84% alike)", "Flow: the same"],
      label: "Sat, 31 Oct: about 4.2 of 5, good, from 3 rated visits.", why: [] });
    let rated: number | null = null;
    render(<SpotScreen spot={{ ...TAMIHI, rating: 5 }} palette={LIGHT} onBack={() => {}} onEdit={() => {}} onDelete={() => {}}
      visits={visitsView(spot, TAMIHI.id, Date.now())} onPickVisit={() => {}} onRate={(n) => { rated = n; }}
      onAddVisit={() => {}} onRemoveVisit={() => {}}
      likeVisit={{ mode: "predict", best: "Best going by your ratings", hint: null, heading: "Like your visit?",
        intro: "How good each of the next five days may be.", note: null,
        visit: { top: "From your ratings", sub: "5★ 30 Aug · 2★ 9 Sep · 4★ 19 Sep", label: "From your ratings." },
        days: [day("2026-10-30", false), day("2026-10-31", true)], age: "Forecast checked just now." }} />);
    await settle();
    expect(screen.getAllByRole("radio").length).toBeGreaterThanOrEqual(3);
    press("Rate this visit 3 of 5: OK");
    expect(rated).toBe(3);
    expect(screen.getAllByLabelText("Rated 5 of 5. Press to clear the rating").length).toBe(1);
  }, /YOUR VISITS[\s\S]*Add a visit[\s\S]*HOW WAS THIS VISIT\?[\s\S]*Great[\s\S]*BEST GOING BY YOUR RATINGS/],
];

describe.each([["phone", 390], ["desktop", 1280]] as const)("(T258) %s: the leftover states", (_l, width) => {
  for (const [name, put, see] of STATES_T258)
    it(name, async () => {
      await put(width);
      expect(document.body.textContent).toMatch(see);
      expect(findings()).toEqual([]);
    });
});

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
      <View accessible accessibilityLabel="Synopsis: No date recorded. Unknown."><Text>Synopsis</Text></View>
      <View accessible role="group" accessibilityLabel="Basemap: 2026-03-05. Up to date."><Text>Basemap</Text></View>
    </View>);
    expect(findings()).toEqual([
      '"Card" is a labelled row with words and no role: the web drops its label',
      '"Synopsis: up to date" is a labelled row with words and no role: the web drops its label',
      '"Synopsis: No date recorded. Unknown." is a labelled row with words and no role: the web drops its label',
      'button "": no accessible name',
      'button "No box": no data-hitslop: give it hitProps(its box)',
      'switch "Short": reaches 30×30',
      '"Card" hides "words a reader never hears"',
      '"3 gauges near the 10th percentile" is on a wordless div with no role: never said',
    ]);
  });
});
