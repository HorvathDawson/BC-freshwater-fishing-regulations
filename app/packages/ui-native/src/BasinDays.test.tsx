/**
 * (T377 item 1) The forecast-day card of one watershed group, drawn: each day a button named in full, a tap is the
 * horizon pick for that day, the map ramp's colour per day, a day no gauge reads shown (a dash and its key), and the
 * failure face / the "in no group" line — never a blank card.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { basinDaysVM, type DayHorizon } from "@app/core";
import { BasinDays } from "./BasinDays";
import { rampAt } from "./DonorPanel";
import { flowRampStops, LIGHT } from "./theme";

afterEach(cleanup);

const TUE = new Date(2026, 10, 3, 12);
const st = new Map<DayHorizon, number | null>([[0, 0.05], [1, 0.5], [2, null]]);
const ready = { state: "ready" as const, basin: { id: "CHWK", name: "Chilliwack River" },
                vm: basinDaysVM("Chilliwack River", st, 1, "discharge", TUE) };
const rgb = (hex: string) => {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255})`;
};

describe("BasinDays", () => {
  it("strip: the group's name, a button per day named in full; a tap picks that day", () => {
    const onPick = vi.fn();
    render(<BasinDays state={ready} palette={LIGHT} theme="light" onPick={onPick} layout="strip" />);
    expect(screen.getByText("Chilliwack River watershed")).toBeTruthy();
    const days = screen.getAllByRole("button");
    expect(days).toHaveLength(11);
    expect(days[0]!.getAttribute("aria-label")).toBe("Conditions now: far lower than normal");
    expect(days[1]!.getAttribute("aria-label")).toBe("Forecast 1 day ahead: normal, showing");
    expect(days[1]!.getAttribute("aria-pressed")).toBe("true");
    // (T427) the day shown, in words, on the title row's right: one row, the watershed left
    expect(screen.getByTestId("basin-days-shown").textContent).toBe("Wed 4 · normal");
    expect(screen.getByTestId("basin-days-shown").parentElement!.textContent)
      .toBe("Chilliwack River watershedWed 4 · normal");
    // the repeated subtitle is gone from the strip; the card has no border of its own, a rule line instead
    expect(screen.queryByText("Flow vs. past years, by day")).toBeNull();
    const card = screen.getByTestId("basin-days").style;
    expect([card.borderTopWidth, card.borderLeftWidth]).toEqual(["1px", ""]);
    // no day lit (Change): no verdict, the name alone
    cleanup();
    render(<BasinDays state={{ ...ready, vm: basinDaysVM("Chilliwack River", st, "change", "discharge", TUE) }}
                      palette={LIGHT} theme="light" layout="strip" />);
    expect(screen.queryByTestId("basin-days-shown")).toBeNull();
    cleanup();
    render(<BasinDays state={ready} palette={LIGHT} theme="light" onPick={onPick} layout="strip" />);
    fireEvent.click(screen.getByRole("button", { name: "Forecast 2 days ahead: no gauge reading" }));
    expect(onPick).toHaveBeenCalledWith(2);
  });

  it("each day wears the map ramp's colour; a day no gauge reads is a dash, and the key says so", () => {
    render(<BasinDays state={ready} palette={LIGHT} theme="light" layout="rows" />);
    fireEvent.click(screen.getByTestId("basin-days-toggle"));
    const swatch = (d: number) => screen.getByTestId(`basin-days-day-${d}`).querySelectorAll("div")[1] as HTMLElement;
    expect(swatch(0).style.backgroundColor).toBe(rgb(rampAt(flowRampStops("light"), 0.05)));
    expect(swatch(1).style.backgroundColor).toBe(rgb(rampAt(flowRampStops("light"), 0.5)));
    expect(screen.getByTestId("basin-days-day-2").textContent).toBe("Thu 5—No gauge reading");
    expect(screen.getByText("— no gauge in this watershed reads that day")).toBeTruthy();
  });

  it("(T872) rows: folded by default — the name and the change points; a tap opens the days and says so", () => {
    const onPick = vi.fn();
    render(<BasinDays state={ready} palette={LIGHT} theme="light" onPick={onPick} layout="rows" />);
    const toggle = screen.getByTestId("basin-days-toggle");
    expect(toggle.getAttribute("role")).toBe("button");
    expect(toggle.getAttribute("aria-expanded")).toBe("false");
    expect(toggle.getAttribute("aria-label"))
      .toBe("Chilliwack River watershed, by day: Now: Far lower than normal · Normal from Wed 4 · No gauge reading from Thu 5");
    expect(toggle.textContent)
      .toBe("Chilliwack River watershedNow: Far lower than normal · Normal from Wed 4 · No gauge reading from Thu 5");
    // shut: no day rows, no scale, no dash key — the card is two lines
    expect(screen.queryAllByRole("button")).toHaveLength(1);
    expect(screen.queryByText("Flow vs. past years, by day")).toBeNull();
    expect(screen.queryByText("— no gauge in this watershed reads that day")).toBeNull();
    fireEvent.click(toggle);
    expect(toggle.getAttribute("aria-expanded")).toBe("true");
    expect(screen.getByTestId("basin-days-scale").textContent).toBe("Flow vs. past years, by day");
    expect(screen.queryByTestId("basin-days-summary")).toBeNull();
    expect(screen.getAllByRole("button")).toHaveLength(12);
    expect(screen.getByText("— no gauge in this watershed reads that day")).toBeTruthy();
    // a day is still the horizon pick when open
    fireEvent.click(screen.getByRole("button", { name: "Forecast 2 days ahead: no gauge reading" }));
    expect(onPick).toHaveBeenCalledWith(2);
    fireEvent.click(toggle);
    expect(toggle.getAttribute("aria-expanded")).toBe("false");
    expect(screen.queryAllByRole("button")).toHaveLength(1);
  });

  it("(T872) the web: the fold is a real <button> in the tab order, so Enter and Space work as on any button", () => {
    // react-native-web draws role=button as a native <button>: the browser turns Enter / Space into its click (jsdom
    // does not synthesise that click, so the key is proven by the element; the staging walk pressed Enter for real)
    render(<BasinDays state={ready} palette={LIGHT} theme="light" layout="rows" />);
    const toggle = screen.getByTestId("basin-days-toggle");
    expect(toggle.tagName).toBe("BUTTON");
    expect(toggle.getAttribute("tabindex")).toBe("0");
    expect(toggle.getAttribute("type")).toBe("button");
  });

  it("(T872) another watershed group opens shut, however it was reached", () => {
    const { rerender } = render(<BasinDays state={ready} palette={LIGHT} theme="light" layout="rows" />);
    fireEvent.click(screen.getByTestId("basin-days-toggle"));
    expect(screen.getByTestId("basin-days-toggle").getAttribute("aria-expanded")).toBe("true");
    // the same group again (a refreshed feed, another day lit): stays open
    rerender(<BasinDays state={{ ...ready, vm: basinDaysVM("Chilliwack River", st, 0, "discharge", TUE) }} palette={LIGHT}
                        theme="light" layout="rows" />);
    expect(screen.getByTestId("basin-days-toggle").getAttribute("aria-expanded")).toBe("true");
    rerender(<BasinDays state={{ state: "ready", basin: { id: "SQUA", name: "Squamish River" },
                                 vm: basinDaysVM("Squamish River", st, 0, "discharge", TUE) }}
                        palette={LIGHT} theme="light" layout="rows" />);
    expect(screen.getByTestId("basin-days-toggle").getAttribute("aria-expanded")).toBe("false");
  });

  it("the place in no group is said; a failure is the failure face", () => {
    render(<BasinDays state={{ state: "none", line: "This water is in no watershed group" }} palette={LIGHT} theme="light"
                      layout="rows" />);
    expect(screen.getByTestId("basin-days-none").textContent).toBe("This water is in no watershed group");
    cleanup();
    render(<BasinDays state={{ state: "failed", error: new Error("x") }} palette={LIGHT} theme="light" layout="strip" />);
    expect(screen.getByRole("alert").textContent).toBe("Watershed days: couldn’t load");
  });
});
