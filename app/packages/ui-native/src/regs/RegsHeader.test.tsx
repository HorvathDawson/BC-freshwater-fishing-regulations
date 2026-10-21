/**
 * T23 — the regs header drawn over the golden headers (fixtures/golden-headers.json, copied from golden.json by
 * conformance/src/regs/goldenSlices.ts): the name, the date pill, the Map / Parts button, the place label and the four
 * tabs, on every golden state; then the presses, the label fit and the one-shape rule.
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

/** the golden header as the app's HeaderVM (T16 field names) */
const headerOf = (id: string): HeaderVM => {
  const g = G[id]!;
  return { name: g.name, dayWord: g.day, date: g.date, dateAria: g.dateAria, dot: g.dot,
    place: g.place && { full: g.place.full, short: g.place.short }, action: g.action,
    tabs: g.tabs.map((t) => ({ tab: t.sec, label: t.text })) };
};
const rgb = (hex: string) => {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255})`;
};

function mount(h: HeaderVM, o: { tabs?: TabSource; action?: boolean } = {}) {
  const calls: string[] = [];
  const r = render(<RegsHeader header={h} palette={LIGHT} tabs={o.tabs ?? createPinStore()}
                               onDate={() => calls.push("date")} onTab={(t) => calls.push(`tab:${t}`)}
                               onAction={o.action === false ? undefined : (k) => calls.push(`action:${k}`)} />);
  return { r, calls };
}
/** what the header draws, in reading order */
const drawn = () => {
  const root = screen.getByTestId("regs-header"), action = screen.queryByTestId("regs-action");
  return {
    name: within(root).getByRole("heading").textContent,
    pill: [screen.getByTestId("regs-date").getAttribute("aria-label"), screen.getByTestId("regs-date").textContent],
    action: action && [action.getAttribute("aria-label"), action.textContent],
    place: screen.getByTestId("regs-place").textContent,
    tabs: screen.getAllByRole("tab").map((t) => [t.textContent, t.getAttribute("aria-selected")]),
  };
};

describe("<RegsHeader> over every golden header", () => {
  it.each(Object.keys(G))("%s: name, pill, action, place (full while unmeasured), tabs", (id) => {
    const g = G[id]!;
    mount(headerOf(id));
    expect(drawn()).toEqual({
      name: g.name,
      pill: [g.dateAria, `${g.day} ▾${g.date}`],
      action: g.action && [g.action.aria, g.action.label],
      place: g.place?.full ?? "",
      tabs: g.tabs.map((t, i) => [t.text, i === 0 ? "true" : "false"]),
    });
  });
});

describe("<RegsHeader>", () => {
  it("vedder_1010: the heading, the pill, the Map button, the place, the tabs, the open dot", () => {
    mount(headerOf("vedder_1010"));
    expect(drawn()).toEqual({
      name: "Vedder River",
      pill: ["Change date, now Oct 10", "Today ▾Oct 10"],
      action: ["View on map: where these rules apply, Mouth → Vedder Crossing Bridge", "Map"],
      place: "Mouth → Vedder Crossing Bridge",
      tabs: [["What you can keep", "true"], ["Gear", "false"], ["Methods", "false"], ["Licence", "false"]],
    });
    expect(screen.getByTestId("regs-dot").style.backgroundColor).toBe(rgb(LIGHT.open));
    expect(screen.getByRole("tablist", { name: "Section" })).toBeTruthy();
  });

  it("vedder_615: the weekday and the closed dot", () => {
    mount(headerOf("vedder_615"));
    expect(screen.getByTestId("regs-date").textContent).toBe("Mon ▾Jun 15");
    expect(screen.getByTestId("regs-dot").style.backgroundColor).toBe(rgb(LIGHT.closed));
  });

  it("bc1_501: the Parts button and the full label", () => {
    const { calls } = mount(headerOf("bc1_501"));
    expect(drawn().action).toEqual(["Other parts — now 4 side channels, joining 55–59.1 km from the mouth", "Parts"]);
    expect(drawn().place).toBe("4 side channels, joining 55–59.1 km from the mouth");
    fireEvent.click(screen.getByTestId("regs-action"));
    expect(calls).toEqual(["action:parts"]);
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

  it("no action handler (no map here): the slot stays, nothing in it", () => {
    mount(headerOf("vedder_1010"), { action: false });
    expect(screen.queryByTestId("regs-action")).toBeNull();
    expect(screen.queryByRole("button", { name: /^View on map/ })).toBeNull();
  });

  it("header-label-two-lines (prototype fitLabel): the full label when it lays out in two lines, else the short", () => {
    const cnr = { full: "From the CNR Mile 66 Trestle up to the CNR Bridge (mile 70.2)", short: "CNR Mile 66 Trestle → CNR Bridge (mile 70.2)" };
    expect([pickPlace(cnr, 33.44, 16.22), pickPlace(cnr, 48.66, 16.22), pickPlace(cnr, null, 16.22), pickPlace(cnr, 0, 16.22)])
      .toEqual([cnr.full, cnr.short, cnr.full, cnr.full]);
    // the edge: two lines + 1 px fits, a hair over does not
    expect([pickPlace(cnr, 16.22 * 2 + 1, 16.22), pickPlace(cnr, 16.22 * 2 + 1.01, 16.22)]).toEqual([cnr.full, cnr.short]);
  });

  it("presses: a tab, the pill, Map", () => {
    const { calls } = mount(headerOf("vedder_1010"));
    fireEvent.click(screen.getByRole("tab", { name: "Gear" }));
    fireEvent.click(screen.getByRole("button", { name: "Change date, now Oct 10" }));
    fireEvent.click(screen.getByRole("button", { name: "View on map: where these rules apply, Mouth → Vedder Crossing Bridge" }));
    expect(calls).toEqual(["tab:gear", "date", "action:map"]);
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
