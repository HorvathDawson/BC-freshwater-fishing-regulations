/**
 * T23 — the regs header drawn over the golden headers (fixtures/golden-headers.json, copied from golden.json by
 * conformance/src/regs/goldenSlices.ts): the name, the date pill, the place label and the four tabs, on every golden
 * state; then the presses, the label fit and the one-shape rule.
 *
 * (Owner call 3) The golden's header action is NOT drawn: its Map button (T71 — the water's one Map entry is the view
 * strip) nor its Parts button (T150 — the water's Map view lists the parts). No golden header's action reaches the app.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import type { HeaderVM, Tab } from "@app/core";
import { LIGHT } from "../theme";
import { pickPlace, RegsHeader, type TabSource } from "./RegsHeader";
import { createPinStore } from "./pinStore";
import headers from "./fixtures/golden-headers.json";

afterEach(cleanup);

type GHeader = { name: string; day: string; date: string; dateAria: string; dot: "open" | "closed" | "faint";
  place: { shown: string; full: string; short: string } | null; action: { kind: "map" | "parts"; label: "Map" | "Parts"; aria: string } | null;
  tabs: { sec: Tab; text: string }[] };
const G = headers as unknown as Record<string, GHeader>;

/** the golden header as the app's HeaderVM (T16 field names; its action dropped — owner call 3, T71 / T150) */
const headerOf = (id: string): HeaderVM => {
  const g = G[id]!;
  return { name: g.name, dayWord: g.day, date: g.date, dateAria: g.dateAria, dot: g.dot,
    place: g.place && { full: g.place.full, short: g.place.short },
    tabs: g.tabs.map((t) => ({ tab: t.sec, label: t.text })) };
};
const rgb = (hex: string) => {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255})`;
};

function mount(h: HeaderVM, o: { tabs?: TabSource } = {}) {
  const calls: string[] = [];
  const r = render(<RegsHeader header={h} palette={LIGHT} tabs={o.tabs ?? createPinStore()}
                               onDate={() => calls.push("date")} onTab={(t) => calls.push(`tab:${t}`)} />);
  return { r, calls };
}
/** what the header draws, in reading order — `buttons`: every button in it but the date pill (none: no action) */
const drawn = () => {
  const root = screen.getByTestId("regs-header");
  return {
    name: within(root).getByRole("heading").textContent,
    pill: [screen.getByTestId("regs-date").getAttribute("aria-label"), screen.getByTestId("regs-date").textContent],
    buttons: within(root).queryAllByRole("button").filter((b) => b.getAttribute("data-testid") !== "regs-date").length,
    place: screen.getByTestId("regs-place").textContent,
    tabs: screen.getAllByRole("tab").map((t) => [t.textContent, t.getAttribute("aria-selected")]),
  };
};

describe("<RegsHeader> over every golden header", () => {
  it.each(Object.keys(G))("%s: name, pill, no action button, place (full while unmeasured), tabs", (id) => {
    const g = G[id]!;
    mount(headerOf(id));
    expect(drawn()).toEqual({
      name: g.name,
      pill: [g.dateAria, `${g.day}${g.date}`],
      buttons: 0,
      place: g.place?.full ?? "",
      tabs: g.tabs.map((t, i) => [t.text, i === 0 ? "true" : "false"]),
    });
  });
});

describe("<RegsHeader>", () => {
  it("vedder_1010: the heading, the pill, NO Map button (T71: the view strip's), the place, the tabs, the open dot", () => {
    mount(headerOf("vedder_1010"));
    expect(drawn()).toEqual({
      name: "Vedder River",
      pill: ["Change date, now Oct 10", "TodayOct 10"],
      buttons: 0,
      place: "Mouth → Vedder Crossing Bridge",
      tabs: [["What you can keep", "true"], ["Gear", "false"], ["Methods", "false"], ["Licence", "false"]],
    });
    expect(screen.getByTestId("regs-dot").style.backgroundColor).toBe(rgb(LIGHT.open));
    expect(screen.getByRole("tablist", { name: "Section" })).toBeTruthy();
  });

  it("vedder_615: the weekday and the closed dot", () => {
    mount(headerOf("vedder_615"));
    expect(screen.getByTestId("regs-date").textContent).toBe("MonJun 15");
    expect(screen.getByTestId("regs-dot").style.backgroundColor).toBe(rgb(LIGHT.closed));
  });

  it("(T150) bc1_501, a water of many parts: no Parts button — only the part's full label; the name row holds the pill alone", () => {
    mount(headerOf("bc1_501"));
    expect(drawn().place).toBe("4 side channels, joining 55–59.1 km from the mouth");
    expect([screen.queryByRole("button", { name: /parts/i }), drawn().buttons]).toEqual([null, 0]);
  });

  it("header-one-shape: klwa_1010 (no label) has the same rows and test ids as vedder_1010, the label slot empty", () => {
    const shape = () => [...screen.getByTestId("regs-header").querySelectorAll("[data-testid]")].map((e) => e.getAttribute("data-testid"));
    mount(headerOf("vedder_1010"));
    const vedder = { rows: screen.getByTestId("regs-header").children.length, ids: shape() };
    cleanup();
    mount(headerOf("klwa_1010"));
    expect({ rows: screen.getByTestId("regs-header").children.length, ids: shape() }).toEqual(vedder);
    expect(screen.getByTestId("regs-place").textContent).toBe("");
  });

  it("(T71 / T150) no action slot on any water: the name row is the frame's title and the pill, so it never reflows", () => {
    const seen: string[][] = [];
    for (const id of ["bc1_501", "vedder_1010", "klwa_1010"]) {
      mount(headerOf(id));
      seen.push([id, String(!!screen.queryByTestId("regs-action-slot")), String(!!screen.queryByTestId("regs-action"))]);
      cleanup();
    }
    expect(seen).toEqual([["bc1_501", "false", "false"], ["vedder_1010", "false", "false"], ["klwa_1010", "false", "false"]]);
  });

  it("header-label-two-lines (prototype fitLabel): the full label when it lays out in two lines, else the short", () => {
    const cnr = { full: "From the CNR Mile 66 Trestle up to the CNR Bridge (mile 70.2)", short: "CNR Mile 66 Trestle → CNR Bridge (mile 70.2)" };
    expect([pickPlace(cnr, 33.44, 16.22), pickPlace(cnr, 48.66, 16.22), pickPlace(cnr, null, 16.22), pickPlace(cnr, 0, 16.22)])
      .toEqual([cnr.full, cnr.short, cnr.full, cnr.full]);
    // the edge: two lines + 1 px fits, a hair over does not
    expect([pickPlace(cnr, 16.22 * 2 + 1, 16.22), pickPlace(cnr, 16.22 * 2 + 1.01, 16.22)]).toEqual([cnr.full, cnr.short]);
  });

  it("presses: a tab, the pill; no Map button to press (T71)", () => {
    const { calls } = mount(headerOf("vedder_1010"));
    fireEvent.click(screen.getByRole("tab", { name: "Gear" }));
    fireEvent.click(screen.getByRole("button", { name: "Change date, now Oct 10" }));
    expect([calls, screen.queryAllByRole("button", { name: /map/i }).length]).toEqual([["tab:gear", "date"], 0]);
  });

  it("the selected tab is the tab source's (the pin store): a change re-renders the tabs", () => {
    const store = createPinStore();
    mount(headerOf("vedder_1010"), { tabs: store });
    expect(screen.getByRole("tab", { name: "What you can keep" }).getAttribute("aria-selected")).toBe("true");
    fireEvent.click(screen.getByRole("tab", { name: "Gear" }));                 // the press only reports…
    expect(screen.getByRole("tab", { name: "Gear" }).getAttribute("aria-selected")).toBe("false");
    act(() => store.selectTab("methods"));                                     // …the store decides what is selected
    expect(screen.getAllByRole("tab").map((t) => t.getAttribute("aria-selected"))).toEqual(["false", "false", "true", "false"]);
  });
});
