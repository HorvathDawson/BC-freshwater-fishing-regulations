/**
 * T49 — THE DAY CALENDAR (one component: the map's date sheet and the Regulations page's "Which day?" both draw it). A
 * month grid with today marked and the chosen day selected, ‹ › months, "Today", the quick ‹ › day steps, days outside
 * the data's calendar year disabled with the reason said, every cell a 44 px target labelled in words, and the arrow
 * keys on the web. It moves THE day (the Shell's `mapDay`, null = today) through `onDay` and holds no day of its own.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { dayRange } from "@app/core";
import { DayCalendar, MapDayStrip } from "./DayCalendar";
import { LIGHT } from "./theme";

afterEach(cleanup);

const NOW = new Date(2026, 9, 21, 9, 30);            // Wed Oct 21 2026, local
const YEAR = dayRange(NOW, null);
const said = (d: unknown) => (d instanceof Date ? `${d.toDateString()} ${d.getHours()}h` : String(d));

function mount(day: Date | null, range = YEAR) {
  const onDay = vi.fn();
  const r = render(<DayCalendar palette={LIGHT} day={day} onDay={onDay} range={range} now={NOW} />);
  return { onDay, picked: () => onDay.mock.calls.map((c) => said(c[0])), r };
}
const btn = (name: string) => screen.getByRole("button", { name });
const cells = () => screen.getByTestId("day-grid").querySelectorAll("[role=button]");

describe("<DayCalendar>", () => {
  it("draws the month of the day shown: its line, the month, the week heads and every day in words", () => {
    mount(new Date(2026, 9, 10, 12));
    expect(screen.getByTestId("day-calendar").textContent).toBe(
      "Showing Saturday, October 10" + "October 2026" + "SunMonTueWedThuFriSat"
      + Array.from({ length: 31 }, (_, i) => i + 1).join("") + "‹ Day beforeTodayDay after ›");
    expect([...cells()].map((c) => c.getAttribute("aria-label")).slice(8, 11))
      .toEqual(["Friday, October 9", "Saturday, October 10, chosen", "Sunday, October 11"]);
    expect(btn("Saturday, October 10, chosen").getAttribute("aria-selected")).toBe("true");
    expect(btn("Wednesday, October 21, today").getAttribute("aria-selected")).toBe("false");
  });

  it("every control is a 44 px target", () => {
    mount(null);
    const sizes = [...screen.getByTestId("day-calendar").querySelectorAll("[role=button]")]
      .map((b) => [(b as HTMLElement).style.minHeight || (b as HTMLElement).style.height,
                   (b as HTMLElement).style.minWidth || "flex"]);
    expect(new Set(sizes.map((s) => s.join(" ")))).toEqual(new Set(["44px 44px"]));
  });

  it("picking a day moves THE day (local noon); picking today, or Today, is null", () => {
    const { picked } = mount(new Date(2026, 9, 10, 12));
    fireEvent.click(btn("Thursday, October 15"));
    fireEvent.click(btn("Wednesday, October 21, today"));
    fireEvent.click(btn("Back to today"));
    expect(picked()).toEqual(["Thu Oct 15 2026 12h", "null", "null"]);
  });

  it("the quick steps move a day either way from the day shown", () => {
    const { picked } = mount(null);
    fireEvent.click(btn("One day earlier"));
    fireEvent.click(btn("One day later"));
    expect(picked()).toEqual(["Tue Oct 20 2026 12h", "Thu Oct 22 2026 12h"]);
  });

  it("‹ › change the month shown, not the day", () => {
    const { picked } = mount(null);
    fireEvent.click(btn("Next month"));
    expect(screen.getByRole("heading").textContent).toBe("November 2026");
    fireEvent.click(btn("Previous month"));
    fireEvent.click(btn("Previous month"));
    expect(screen.getByRole("heading").textContent).toBe("September 2026");
    expect(picked()).toEqual([]);
  });

  it("outside the data's calendar year: the month and day steps are disabled and the reason is said", () => {
    const { picked } = mount(new Date(2026, 0, 1, 12));
    expect(screen.getByRole("heading").textContent).toBe("January 2026");
    expect(btn("Previous month").getAttribute("aria-disabled")).toBe("true");
    expect(btn("One day earlier").getAttribute("aria-disabled")).toBe("true");
    fireEvent.click(btn("Previous month"));
    fireEvent.click(btn("One day earlier"));
    expect(screen.getByTestId("day-reason").textContent)
      .toBe("Only days in 2026 can be chosen: the regulations here are one calendar year.");
    expect(picked()).toEqual([]);
  });

  it("a release cut mid-year: the days after its last are disabled, labelled so, and do nothing", () => {
    const { picked } = mount(new Date(2026, 10, 20, 12), dayRange(NOW, "2026-11-20"));
    const off = btn("Saturday, November 21, not available");
    expect(off.getAttribute("aria-disabled")).toBe("true");
    fireEvent.click(off);
    expect(screen.getByTestId("day-reason").textContent)
      .toBe("Days after November 20 can't be chosen: these regulations end then.");
    expect(picked()).toEqual([]);
  });

  it("no reason line in a month with every day open", () => {
    mount(null);
    expect(screen.queryByTestId("day-reason")).toBeNull();
  });

  it("the keyboard (web): arrows move a day or a week, Page Up/Down a month, Home back to today", () => {
    const { picked } = mount(new Date(2026, 9, 10, 12));
    const grid = screen.getByTestId("day-grid");
    for (const key of ["ArrowRight", "ArrowLeft", "ArrowDown", "ArrowUp", "PageDown", "PageUp", "Home", "x"])
      fireEvent.keyDown(grid, { key });
    expect(picked()).toEqual(["Sun Oct 11 2026 12h", "Fri Oct 09 2026 12h", "Sat Oct 17 2026 12h", "Sat Oct 03 2026 12h",
                              "Tue Nov 10 2026 12h", "Thu Sep 10 2026 12h", "null"]);
    // only the chosen day is in the tab order; the arrows move within the grid
    expect([...cells()].filter((c) => c.getAttribute("tabindex") === "0").map((c) => c.getAttribute("aria-label")))
      .toEqual(["Saturday, October 10, chosen"]);
  });

  it("the keyboard across a month edge: focus follows the day into the new month, and the grid keeps taking keys", () => {
    // the Shell holds THE day; this holds it the same way so a key's day comes back as the prop
    function Held() {
      const [day, setDay] = useState<Date | null>(new Date(2026, 9, 29, 12));
      return <DayCalendar palette={LIGHT} day={day} onDay={setDay} range={YEAR} now={NOW} />;
    }
    render(<Held />);
    const focused = () => (document.activeElement as HTMLElement | null)?.getAttribute("aria-label") ?? "(none)";
    const press = (key: string) => {
      act(() => { fireEvent.keyDown(document.activeElement ?? document.body, { key }); });
      return [screen.getByRole("heading").textContent, focused()].join(" / ");
    };
    act(() => { (btn("Thursday, October 29, chosen") as HTMLElement).focus(); });
    expect(focused()).toBe("Thursday, October 29, chosen");
    expect([press("ArrowDown"), press("ArrowRight"), press("PageUp"), press("PageUp"), press("PageDown"),
            press("ArrowLeft"), press("Home")]).toEqual([
      "November 2026 / Thursday, November 5, chosen",
      "November 2026 / Friday, November 6, chosen",
      "October 2026 / Tuesday, October 6, chosen",
      "September 2026 / Sunday, September 6, chosen",
      "October 2026 / Tuesday, October 6, chosen",
      "October 2026 / Monday, October 5, chosen",
      "October 2026 / Wednesday, October 21, today, chosen",
    ]);
  });

  it("the month and day-step buttons keep keyboard focus when pressed: the same node, still focused", () => {
    function Held() {
      const [day, setDay] = useState<Date | null>(null);
      return <DayCalendar palette={LIGHT} day={day} onDay={setDay} range={YEAR} now={NOW} />;
    }
    render(<Held />);
    for (const name of ["Next month", "Previous month", "One day earlier", "One day later", "Back to today"]) {
      const before = btn(name);
      act(() => { before.focus(); });
      act(() => { fireEvent.click(before); });
      const after = btn(name);
      expect([name, after === before, document.activeElement === after]).toEqual([name, true, true]);
    }
  });
});

describe("<MapDayStrip> — the map's day, in Conditions' corner strip (T213)", () => {
  it("Today · Tomorrow · a calendar segment: the day shown filled; a press sets the day or opens the sheet", () => {
    const onDay = vi.fn(), open = vi.fn();
    const { rerender } = render(<MapDayStrip palette={LIGHT} day={null} range={YEAR} now={NOW} onDay={onDay} onOpen={open} />);
    const pressed = () => ["today", "tomorrow", "pick"].map((k) => screen.getByTestId(`map-day-${k}`).getAttribute("aria-pressed"));
    expect([screen.getByTestId("map-day").textContent, pressed()]).toEqual(["TodayTomorrowDate", ["true", "false", "false"]]);
    fireEvent.click(btn("Colour the map for tomorrow"));
    fireEvent.click(btn("Map coloured for today. Choose a day"));
    expect([onDay.mock.calls.map((c) => said(c[0])), open.mock.calls.length]).toEqual([["Thu Oct 22 2026 12h"], 1]);
    rerender(<MapDayStrip palette={LIGHT} day={new Date(2026, 5, 13, 12)} range={YEAR} now={NOW} onDay={onDay} onOpen={open} />);
    fireEvent.click(btn("Colour the map for today"));
    expect([screen.getByTestId("map-day").textContent, pressed(), said(onDay.mock.calls[1]![0])])
      .toEqual(["TodayTomorrowSat, Jun 13", ["false", "false", "true"], "null"]);
  });
});
