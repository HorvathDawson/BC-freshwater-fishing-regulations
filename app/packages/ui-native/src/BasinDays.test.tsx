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
    expect(days[0]!.getAttribute("aria-label")).toBe("Conditions now: much below normal");
    expect(days[1]!.getAttribute("aria-label")).toBe("Forecast 1 day ahead: normal, showing");
    expect(days[1]!.getAttribute("aria-pressed")).toBe("true");
    // the day shown, in words, under the strip
    expect(screen.getByTestId("basin-days-shown").textContent).toBe("Wed 4: Normal");
    fireEvent.click(screen.getByRole("button", { name: "Forecast 2 days ahead: no gauge reading" }));
    expect(onPick).toHaveBeenCalledWith(2);
  });

  it("each day wears the map ramp's colour; a day no gauge reads is a dash, and the key says so", () => {
    render(<BasinDays state={ready} palette={LIGHT} theme="light" layout="rows" />);
    const swatch = (d: number) => screen.getByTestId(`basin-days-day-${d}`).querySelectorAll("div")[1] as HTMLElement;
    expect(swatch(0).style.backgroundColor).toBe(rgb(rampAt(flowRampStops("light"), 0.05)));
    expect(swatch(1).style.backgroundColor).toBe(rgb(rampAt(flowRampStops("light"), 0.5)));
    expect(screen.getByTestId("basin-days-day-2").textContent).toBe("Thu 5—No gauge reading");
    expect(screen.getByText("— no gauge in this watershed reads that day")).toBeTruthy();
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
