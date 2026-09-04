/**
 * The date picker, mounted.
 *
 * The map has always shown a date pill, because half of BC's freshwater regulations are
 * seasonal and a map of "open" is only true for one day. The pill was inert — `MapScreen`
 * declared `onDate` and `Shell` never passed one — so the app displayed the qualifier on
 * every answer and offered no way to change it.
 *
 * The failure mode worth testing is not "the buttons work". It is a date that does not
 * exist: 31 January with February selected has to become 28 February, because every window
 * comparison downstream would otherwise be asked about a day that is not on the calendar.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render } from "@testing-library/react";
import type { PlainDate } from "@app/core";
import { DateSheet, daysInMonth } from "./DateSheet";
import { LIGHT } from "./theme";

afterEach(cleanup);

const show = (value: PlainDate, onChange = vi.fn()) => ({
  onChange,
  ...render(<DateSheet open value={value} onChange={onChange} onClose={() => {}}
                       palette={LIGHT} />),
});

describe("daysInMonth", () => {
  it.each([
    [2026, 1, 31], [2026, 2, 28], [2026, 4, 30], [2026, 12, 31],
    // Leap years, both rules: 2024 is one, 2100 is not (divisible by 100, not by 400).
    [2024, 2, 29], [2100, 2, 28], [2000, 2, 29],
  ])("%i-%i has %i days", (y, m, n) => {
    expect(daysInMonth(y, m)).toBe(n);
  });
});

describe("<DateSheet>", () => {
  it("changing month never produces a day that does not exist", () => {
    // The whole reason this component owns the clamp rather than the caller.
    const { onChange, getByLabelText } = show({ year: 2026, month: 1, day: 31 });
    fireEvent.click(getByLabelText("FEB"));
    expect(onChange).toHaveBeenCalledWith({ year: 2026, month: 2, day: 28 });
  });

  it("keeps the day when the target month is long enough", () => {
    const { onChange, getByLabelText } = show({ year: 2026, month: 1, day: 15 });
    fireEvent.click(getByLabelText("MAR"));
    expect(onChange).toHaveBeenCalledWith({ year: 2026, month: 3, day: 15 });
  });

  it("clamps into a leap February rather than out of it", () => {
    const { onChange, getByLabelText } = show({ year: 2024, month: 1, day: 30 });
    fireEvent.click(getByLabelText("FEB"));
    expect(onChange).toHaveBeenCalledWith({ year: 2024, month: 2, day: 29 });
  });

  it("steps a day at a time, and stops at each end of the month", () => {
    const first = show({ year: 2026, month: 2, day: 1 });
    // Disabled rather than absent: the control keeps its place, so the row does not reflow
    // as you reach an end. Asserted through the DOM attribute rather than a jest-dom
    // matcher — this suite deliberately runs without that package (see app/deps.md).
    expect(first.getByLabelText("Previous day").getAttribute("aria-disabled")).toBe("true");
    fireEvent.click(first.getByLabelText("Next day"));
    expect(first.onChange).toHaveBeenCalledWith({ year: 2026, month: 2, day: 2 });
    cleanup();

    const last = show({ year: 2026, month: 2, day: 28 });
    expect(last.getByLabelText("Next day").getAttribute("aria-disabled")).toBe("true");
    // Absent, not "false": an enabled control carries no aria-disabled at all.
    expect(last.getByLabelText("Previous day").getAttribute("aria-disabled")).toBeNull();
  });

  it("tells a screen reader which month is chosen, not only the eye", () => {
    /*
     * `aria-checked`, NOT COLOUR. This started as `accessibilityState={{ selected }}`,
     * which is how the rest of the app was written — and react-native-web 0.21 dropped
     * `accessibilityState` entirely, so it rendered NO attribute at all. Ten components
     * were affected and nothing failed, because the selected state was also carried by a
     * background colour and every test looked at the DOM the same way a sighted user looks
     * at the screen. `role="radio"` takes `aria-checked`; `aria-selected` is for tabs.
     */
    const { getAllByRole } = show({ year: 2026, month: 9, day: 3 });
    const radios = getAllByRole("radio");
    expect(radios).toHaveLength(12);
    const on = radios.filter((b) => b.getAttribute("aria-checked") === "true");
    expect(on).toHaveLength(1);
    expect(on[0]!.getAttribute("aria-label")).toBe("SEP");
  });

  it("says why the date matters at all", () => {
    // A control with no explanation reads as a filter. This one changes what "open" means.
    const { getByText } = show({ year: 2026, month: 9, day: 3 });
    expect(getByText(/seasonal/)).toBeTruthy();
  });
});
