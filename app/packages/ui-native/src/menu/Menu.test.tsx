/**
 * THE MENU, mounted (owner 2026-10-21; T48: the More tab holds what the hamburger's drawer held — moved, not copied): the
 * More tab's list, its pages, the report form and the regs tabs' report link — through react-native-web, with the app's
 * real back (`useBack`) and the menu's real state (`useMenu`).
 *
 * What this proves: every page draws its core VM's words exactly; every open level is a back layer (back closes the
 * top one, and at the browse root there is nothing left to close); a report is composed from what the reader sees and
 * leaves on, and goes out only on a press. What it does NOT: the native share sheet itself. (Back from the More tab to
 * the Map tab is the route's tab rule: apps/mobile back.test.tsx.)
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Linking, Share, Text } from "react-native";
import { FISH_CREDITS, MENU_WORDS, regsReportWhere, type DataInput, type GlossaryTerm, type RegsPage } from "@app/core";
import type { RegsLoader } from "@app/data/regs";
import { BackLayerContext, ERROR_LOG, routeWhere, useBack, useMenu, useRoute, type Back, type BackLayers,
         type MenuControls } from "@app/ui";
import { LIGHT, type ThemeName } from "../theme";
import { GlossaryPage } from "./GlossaryPage";
import { MenuContext } from "./MenuContext";
import { MoreScreen } from "./MoreScreen";
import { MenuHost, type MenuInfo } from "./MenuHost";
import { ReportLink } from "./ReportLink";
import { RegsScreen } from "../regs/RegsScreen";
import { createPinStore } from "../regs/pinStore";
import vedder1010 from "../regs/fixtures/vedder_1010.json";

afterEach(() => { cleanup(); vi.restoreAllMocks(); ERROR_LOG.clear(); });

const INFO: MenuInfo = { appBuild: "1.2.0 (34)", contact: null };
const NOW = Date.now();
/** The release the Data block reads in most tests: the edition named, the atlas and basemap dated, tiles paired. */
const DATA: DataInput = {
  sources: { synopsis: { title: "2025–2027 BC Freshwater Fishing Regulations Synopsis", date: null },
             basemap: { date: "2026-03-05" } },
  validUntil: null, tiles: "d768d6e135e62b99.c9f9604f12bf", tilesPaired: true,
  feedFetchedAt: new Date(NOW - 27 * 3.6e6).toISOString(), now: NOW, newRelease: null,
};

/** The Shell's wiring, and nothing else: the one back, the menu, the More tab and the host. */
function App({ info = INFO, loader = null, out, data = DATA, theme = "light", onTheme = () => {} }: {
  info?: MenuInfo; loader?: RegsLoader | null; out: { back?: Back }; data?: DataInput;
  theme?: ThemeName; onTheme?: (t: ThemeName) => void;
}) {
  const [route, go] = useRoute();
  const b = useBack(route, go, null);
  out.back = b;
  const menu = useMenu();
  return (
    <BackLayerContext.Provider value={b.layers}>
      <MenuContext.Provider value={menu}>
        <MoreScreen palette={LIGHT} data={data} back="Map" onBack={b.back} contact={info.contact}
                    theme={theme} onTheme={onTheme}
                    onPick={(k) => (k === "report" ? menu.report(routeWhere(route, null)) : menu.show(k))} />
        <MenuHost menu={menu} palette={LIGHT} loader={loader} info={info} tiles="d768d6e135e62b99.c9f9604f12bf"
                  tilesPaired feedFetchedAt={null} route={route} day={null} />
      </MenuContext.Provider>
    </BackLayerContext.Provider>
  );
}

/**
 * react-native-web keeps a closing Modal mounted until its exit animation ends, which jsdom never plays: end every
 * animation, as the browser would (jsdom has no AnimationEvent, so React listens under the prefixed name; both go).
 */
const settle = () => act(() => {
  for (const el of Array.from(document.body.querySelectorAll("*")))
    for (const t of ["animationend", "webkitAnimationEnd"]) el.dispatchEvent(new Event(t, { bubbles: true }));
});
const press = (label: string) => { act(() => { fireEvent.click(screen.getByLabelText(label)); }); settle(); };
const pick = (id: string) => { act(() => { fireEvent.click(screen.getByTestId(id)); }); settle(); };
const text = (id: string) => screen.getByTestId(id).textContent;
const said = (id: string) => screen.getByTestId(id).getAttribute("aria-label");

describe("the More tab (T48; owner 2026-10-22 refinement)", () => {
  it("top to bottom: back to the previous tab, the title, Data, Display, the pages as rows, Contact — the only list", () => {
    render(<App out={{}} />);
    expect({
      back: [text("more-back"), said("more-back")],
      headings: screen.getAllByRole("heading").map((e) => e.textContent),
      rows: screen.getAllByTestId(/^menu-item-/).map((e) => e.textContent),
    }).toEqual({
      back: ["Map", "Back to Map"],
      headings: ["More", "Data", "Display", "Contact"],
      rows: [
        "GlossaryWhat the regulations' words mean",
        "AboutWhat this app is, and what it is not",
        "Report a problemSomething wrong, missing or confusing",
      ],
    });
  });

  it("the Data block is always there: one summary badge, one line per source with its date, said aloud in full", () => {
    render(<App out={{}} />);
    expect({
      summary: text("data-summary"),
      lines: screen.getAllByTestId(/^data-line-/).map((e) => e.getAttribute("aria-label")),
      shown: text("data-line-feed"),
      block: said("more-data"),
    }).toEqual({
      summary: "1 likely out of date",
      lines: [
        "Regulations: 2025–2027 edition. Unknown",
        "In-season changes: Check the Province's notices. Not read",
        "River flows: Checked 27h ago. Likely out of date",
        "Weather: With each saved spot. Live",
        "Map: Matches the data. Up to date",
        "Basemap: Updated Mar 5, 2026. Up to date",
        "Satellite imagery: Online only. Live",
      ],
      shown: "River flowsChecked 27h ago",
      block: "Data sources & dates. 1 likely out of date",
    });
    expect(screen.queryByTestId("data-new-release")).toBeNull();
  });

  it("a newer release is said at the top of the block, and in its badge", () => {
    render(<App out={{}} data={{ ...DATA, newRelease: "r-newer" }} />);
    expect([text("data-summary"), text("data-new-release")])
      .toEqual(["Newer data available", "A newer data release is available. Reopen the app to use it."]);
  });

  it("the Data block opens the detailed references; back closes them to the tab", () => {
    const out: { back?: Back } = {};
    render(<App out={out} />);
    pick("more-data");
    expect(screen.getByTestId("menu-page-sources")).toBeTruthy();
    let r = false;
    act(() => { r = out.back!.back(); });
    settle();
    expect([r, screen.queryByTestId("menu-page-sources")]).toEqual([true, null]);
  });

  it("has no menu button, no drawer and nothing to close: it is a tab, not a surface", () => {
    const out: { back?: Back } = {};
    render(<App out={out} />);
    let r = true;
    act(() => { r = out.back!.back(); });
    expect([screen.queryByLabelText("Open menu"), screen.queryByTestId("menu-drawer"), r]).toEqual([null, null, false]);
  });

  it("Display: the three palettes as preview tiles, the one in use chosen, a press picks — no page to open", () => {
    const picked: string[] = [];
    render(<App out={{}} theme="dark" onTheme={(t) => picked.push(t)} />);
    expect(text("display-palette")).toBe(
      "LightDarkColour-blindThe colour-blind palette changes the hues and nothing else. It has a light ground only.");
    expect(screen.getAllByRole("radio").map((r) => `${r.textContent}${r.getAttribute("aria-checked") === "true" ? " *" : ""}`))
      .toEqual(["Light", "Dark *", "Colour-blind"]);
    expect(screen.getByRole("radiogroup").getAttribute("aria-label")).toBe("Colours");
    act(() => { fireEvent.click(screen.getByRole("radio", { name: "Colour-blind" })); });
    expect(picked).toEqual(["cvd"]);
  });

  it("Contact, inline at the bottom: a placeholder until the owner supplies the one value; then that address", () => {
    render(<App out={{}} />);
    expect(text("more-contact")).toBe("Questions, corrections and ideas are welcome."
      + "Contact details are coming soon. Until then, use Report a problem to share a note from your device.");
    cleanup();
    render(<App out={{}} info={{ appBuild: null, contact: "hello@example.org" }} />);
    expect(screen.getByLabelText("Email: hello@example.org")).toBeTruthy();
  });
});

describe("a page", () => {
  it("About: what the app is, the authority disclaimer, the official synopsis and every version", () => {
    const out: { back?: Back } = {};
    const open = vi.spyOn(Linking, "openURL").mockResolvedValue(true);
    render(<App out={out} />);
    pick("menu-item-about");
    expect(text("about-not-official")).toBe(MENU_WORDS.about.notOfficial);
    expect(screen.getAllByTestId(/^about-version-/).map((e) => e.textContent)).toEqual(
      ["1.2.0 (34)", "Development bundle", "d768d6e135e62b99.c9f9604f12bf", "Not stated"]);
    press("Open the official Synopsis");
    expect(open.mock.calls).toEqual([["https://www2.gov.bc.ca/assets/gov/sports-recreation-arts-and-culture/outdoor-"
      + "recreation/fishing-and-hunting/freshwater-fishing/fishing_synopsis.pdf"]]);
  });

  it("back closes the page to the More tab", () => {
    const out: { back?: Back } = {};
    render(<App out={out} />);
    pick("menu-item-about");
    let r = false;
    act(() => { r = out.back!.back(); });
    settle();
    expect([r, screen.queryByTestId("menu-page-about"), screen.getAllByTestId(/^menu-item-/).length]).toEqual([true, null, 3]);
    act(() => { r = out.back!.back(); });
    expect(r).toBe(false);
  });

  it("Data sources & dates: the release's own dates, 'not recorded' where it states none, each with its badge", () => {
    const loader = {
      release: "r-0efa5d05fc92",
      manifest: { data_release: "r-0efa5d05fc92", regulations: { valid_until: null, versions: null },
                  sources: { synopsis: { title: "2025–2027 BC Freshwater Fishing Regulations Synopsis", date: null },
                             fwa: { date: "2026-07-15" }, hydat: { date: "2026-07-17" } } },
    } as unknown as RegsLoader;
    render(<App out={{}} loader={loader} />);
    pick("more-data");
    const row = (k: string) => text(`source-${k}`);
    expect([row("synopsis"), row("fwa"), row("dfo"), row("tiles"), row("feed")]).toEqual([
      "2025–2027 BC Freshwater Fishing Regulations Synopsis"
        + "The Province of British Columbia's printed regulations, read water by waterDate not recorded in this release"
        + "Unknown",
      "B.C. Freshwater AtlasEvery stream and lake on the map, from the Province of British ColumbiaUpdated Jul 15, 2026"
        + "Up to date",
      "Fisheries and Oceans Canada salmon regulations"
        + "The federal salmon limits, openings and closures for B.C.'s fresh water, by regionDate not recorded in this release"
        + "Unknown",
      "Map tilesThe atlas drawn for the map, with every regulation's section in it"
        + "Version d768d6e135e62b99.c9f9604f12bfUp to date",
      "Live gauge readingsEnvironment and Climate Change Canada, and the BC River Forecast Centre's forecastsNot checked yet"
        + "Unknown",
    ]);
    // (T43) the app's one list of fish picture credits, every line whole, after the sources
    const pics = screen.getByTestId("sources-fish-credits");
    expect([...pics.querySelectorAll("div[dir=auto]")].map((e) => e.textContent)).toEqual(["FISH PICTURES", ...FISH_CREDITS]);
    // (N1) the icon set's ISC notice, which every copy must carry: name, version, licence, then the copyright verbatim
    const icons = screen.getByTestId("sources-icon-credits");
    expect([...icons.querySelectorAll("div[dir=auto]")].map((e) => e.textContent)).toEqual(["ICONS",
      "Lucide v0.460.0 (https://lucide.dev), ISC licence.",
      "Copyright (c) for portions of Lucide are held by Cole Bemis 2013-2022 as part of Feather (MIT). "
      + "All other copyright (c) for Lucide are held by Lucide Contributors 2022."]);
  });
});

describe("report a problem", () => {
  it("sends nothing until the reader writes and presses; shows exactly what will go, without what they switched off", () => {
    const share = vi.spyOn(Share, "share").mockResolvedValue({ action: "sharedAction" });
    ERROR_LOG.record("map:", new Error("style failed"));
    const errorLine = ERROR_LOG.recent()[0]!;
    render(<App out={{}} />);
    pick("menu-item-report");
    // the diagnostics, each visible with its switch
    expect(screen.getAllByTestId(/^report-diag-/).map((e) => e.getAttribute("data-testid"))).toEqual(
      ["report-diag-app", "report-diag-platform", "report-diag-tiles", "report-diag-where", "report-diag-errors"]);
    expect(screen.getByText("Tell us what is wrong. Nothing leaves this device until you send it yourself, and only "
      + "what you can see below.")).toBeTruthy();
    expect(text("report-empty")).toBe("Write a few words about the problem first.");
    press("Share…");                                   // inert while empty
    expect(share).not.toHaveBeenCalled();
    act(() => { fireEvent.change(screen.getByTestId("report-what"), { target: { value: "The closure is wrong." } }); });
    act(() => { fireEvent.click(screen.getByLabelText("Recent errors")); });        // the reader switches the log off
    const preview = text("report-preview")!;
    expect(preview).toBe("Can I Fish This? problem report\n\nThe closure is wrong.\n\n--\nApp build: 1.2.0 (34)\n"
      + `Platform: web\nMap tiles: d768d6e135e62b99.c9f9604f12bf\nWhere you were: Screen: map`);
    expect(preview.includes(errorLine)).toBe(false);
    press("Share…");
    expect(share.mock.calls).toEqual([[{ title: "Can I Fish This? problem report", message: preview }]]);
  });

  it("with a contact address, Send by email opens a mailto of the same message", () => {
    const open = vi.spyOn(Linking, "openURL").mockResolvedValue(true);
    render(<App out={{}} info={{ appBuild: null, contact: "hello@example.org" }} />);
    pick("menu-item-report");
    act(() => { fireEvent.change(screen.getByTestId("report-what"), { target: { value: "Typo" } }); });
    press("Send by email");
    expect(open.mock.calls).toEqual([["mailto:hello@example.org?subject=Can%20I%20Fish%20This%3F%20problem%20report"
      + "&body=Typo%0D%0A%0D%0A--%0D%0APlatform%3A%20web%0D%0AMap%20tiles%3A%20d768d6e135e62b99.c9f9604f12bf%0D%0A"
      + "Where%20you%20were%3A%20Screen%3A%20map"]]);
  });

  it("a share sheet that fails says so, and the report stays on screen to copy", async () => {
    vi.spyOn(Share, "share").mockRejectedValue(new Error("not supported"));
    render(<App out={{}} />);
    pick("menu-item-report");
    act(() => { fireEvent.change(screen.getByTestId("report-what"), { target: { value: "x" } }); });
    await act(async () => { fireEvent.click(screen.getByLabelText("Share…")); });
    expect(text("report-noshare")).toBe("Sharing is not available here. Copy the report below and send it to us.");
  });
});

describe("the glossary page", () => {
  const terms: GlossaryTerm[] = [
    { id: "bait", term: "Bait", says: "Anything an angler offers a fish to eat.", pages: [80], quote: "", source: "" },
    { id: "barbless", term: "Barbless hook", says: "A hook with no barb, or one pinched flat.", pages: [80], quote: "",
      source: "" },
  ];
  const ready = { state: "ready", value: { terms, pdfPages: { 80: 82 } }, error: null } as const;

  it("lists every term, narrows as the reader types, and opens one", () => {
    const onTerm = vi.fn();
    render(<GlossaryPage glossary={ready} term={null} onTerm={onTerm} onCloseTerm={() => {}} palette={LIGHT} />);
    expect(screen.getAllByTestId(/^glossary-row-/).map((e) => e.textContent)).toEqual(
      ["BaitAnything an angler offers a fish to eat.", "Barbless hookA hook with no barb, or one pinched flat."]);
    act(() => { fireEvent.change(screen.getByLabelText("Search the glossary"), { target: { value: "barbless" } }); });
    expect(screen.getAllByTestId(/^glossary-row-/).map((e) => e.getAttribute("data-testid"))).toEqual(["glossary-row-barbless"]);
    act(() => { fireEvent.change(screen.getByLabelText("Search the glossary"), { target: { value: "sturgeon" } }); });
    expect(screen.getByText("No term matches “sturgeon”.")).toBeTruthy();
    act(() => { fireEvent.change(screen.getByLabelText("Search the glossary"), { target: { value: "" } }); });
    act(() => { fireEvent.click(screen.getByTestId("glossary-row-bait")); });
    expect(onTerm.mock.calls).toEqual([["bait"]]);
  });

  it("draws an open term from the release's own words, and is a back layer while open", () => {
    const log: string[] = [];
    const layers: BackLayers = { register: () => { log.push("register"); return () => { log.push("unregister"); }; },
                                 back: () => true };
    const page = (term: string | null) => (
      <BackLayerContext.Provider value={layers}>
        <GlossaryPage glossary={ready} term={term} onTerm={() => {}} onCloseTerm={() => {}} palette={LIGHT} />
      </BackLayerContext.Provider>);
    const { rerender } = render(page("bait"));
    expect(text("glossary-term")!.startsWith("Anything an angler offers a fish to eat.")).toBe(true);
    rerender(page(null));
    expect(log).toEqual(["register", "unregister"]);
  });

  it("says the dev bundle holds no glossary rather than drawing an empty list", () => {
    render(<GlossaryPage glossary={{ state: "ready", value: null, error: null }} term={null} onTerm={() => {}}
                         onCloseTerm={() => {}} palette={LIGHT} />);
    expect(screen.getByText("The glossary comes with the regulations data, which this build does not hold.")).toBeTruthy();
  });
});

describe("the report link at the foot of the Regulations page", () => {
  const page = {
    target: { kind: "part", item: "gnis:3062", partIx: 1 },
    header: { name: "Vedder River", dayWord: "Today", date: "Oct 21", dateAria: "", dot: "open", action: null,
              place: { full: "Vedder Crossing → Fraser River", short: "→ Fraser" },
              tabs: [{ tab: "keep", label: "What you can keep" }, { tab: "gear", label: "Gear" },
                     { tab: "methods", label: "Methods" }, { tab: "licence", label: "Licence" }] },
  } as unknown as Pick<RegsPage, "target" | "header">;

  it("draws nothing without the Shell's menu (a Regs screen mounted alone is unchanged)", () => {
    render(<ReportLink page={page} palette={LIGHT} />);
    expect(screen.queryByTestId("report-link")).toBeNull();
  });

  it("opens Report a problem prefilled with the water, the part and the day — and NO tab: it reports the whole page", () => {
    // (T41 Q3 review) the link sits below Licence, so the tab in view at the press was always Licence — a wrong fact
    const report = vi.fn();
    const menu = { report } as unknown as MenuControls;
    render(<MenuContext.Provider value={menu}><ReportLink page={page} palette={LIGHT} /></MenuContext.Provider>);
    expect(text("report-link")).toBe("Report a problem with this page");
    press("Report a problem with this page");
    expect(report.mock.calls).toEqual([[{
      screen: "water · regulations", tab: null, place: "Vedder River", water: "gnis:3062", part: 1,
      partLabel: "Vedder Crossing → Fraser River", section: null, day: "Today, Oct 21" }]]);
    expect(report.mock.calls[0]![0]).toEqual(regsReportWhere(page, null));
  });

  it("the report it opens shows those facts as 'Where you were' lines", () => {
    function WithReport() {
      const [route, go] = useRoute();
      const b = useBack(route, go, null);
      const menu = useMenu();
      return (
        <BackLayerContext.Provider value={b.layers}>
          <MenuContext.Provider value={menu}>
            <ReportLink page={page} palette={LIGHT} />
            <MenuHost menu={menu} palette={LIGHT} loader={null} info={INFO} tiles={null} feedFetchedAt={null}
                      route={route} day={null} />
          </MenuContext.Provider>
        </BackLayerContext.Provider>);
    }
    render(<WithReport />);
    press("Report a problem with this page");
    expect(text("report-diag-where")).toBe("Where you wereScreen: water · regulationsWater: Vedder River "
      + "(gnis:3062)Part: 1 (Vedder Crossing → Fraser River)Day: Today, Oct 21");
  });
});

describe("the Regulations screen's report link (T41 Q3, Claude 2026-10-22: once, at the page's foot)", () => {
  it("is drawn ONCE, after the Licence section and before the picture credits — no tab section ends with one", () => {
    const report = vi.fn();
    const menu = { report } as unknown as MenuControls;
    const store = createPinStore();
    render(
      <MenuContext.Provider value={menu}>
        <RegsScreen page={vedder1010 as unknown as RegsPage} palette={LIGHT} store={store}
                    actions={{ onSheet() {}, onDay() {}, onBook() {} }} onDate={() => {}}
                    gear={<Text>gear section</Text>} methods={<Text>methods section</Text>}
                    licence={<Text>licence section</Text>} />
      </MenuContext.Provider>);
    expect([...screen.getByTestId("regs-content").children].map((e) => e.getAttribute("data-testid")))
      .toEqual(["keep-tab", "regs-gear", "regs-methods", "regs-licence", "report-link", "regs-fish-credits"]);   // (T43) the foot credits last
    expect(screen.getAllByText("Report a problem with this page")).toHaveLength(1);
    // (T41 Q3 review) it reports the whole page: whichever tab the row has selected, the report names no tab
    for (const t of ["methods", "keep", "licence", "gear"] as const) {
      act(() => store.selectTab(t));
      act(() => { fireEvent.click(screen.getByTestId("report-link")); });
    }
    expect(report.mock.calls.map(([w]) => [w.tab, w.place, w.day])).toEqual(
      Array(4).fill([null, "Vedder River", "Today, Oct 10"]));
  });
});
