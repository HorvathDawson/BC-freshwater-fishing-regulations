/**
 * T51, T284, T819 item 9: "The next few days" on a saved spot, mounted in light and dark: one compact row per day — the
 * day, its single biggest difference and its score — the reasons behind a tap, the closest day marked; absent from a draft.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render } from "@testing-library/react";
import type { LikeVisitView } from "@app/ui";
import type { Spot } from "@app/data/spots";
import { SpotScreen } from "./SpotScreen";
import { DARK, LIGHT, type Palette } from "./theme";

afterEach(cleanup);

const VISIT = new Date(2026, 7, 30, 9).getTime();
const spot: Spot = { id: "s1", createdAt: VISIT, visitedAt: VISIT, updatedAt: 0, lat: 49.0974, lon: -121.9675,
  item: null, waterName: null, title: "Tamihi run", notes: "", photos: [], reading: null, weather: null, trace: null,
  panel: null };

const VIEW: LikeVisitView = {
  mode: "like", best: "Closest to your visit", hint: null,
  heading: "Like your visit?",
  intro: "How alike each of the next five days looks to the day you were here.",
  visit: { top: "Your visit", sub: "Sun 30 Aug", label: "Your visit, Sun 30 Aug: the day the others are compared with." },
  age: "Forecast checked 2h ago.",
  note: null,
  days: [
    { date: "2026-10-22", top: "Today", sub: "Thu 22 Oct", score: "100%", read: "Very similar", basis: null, best: true,
      reasons: ["Flow: the same"], lead: "Much the same as your visit",
      label: "Today, 22 Oct: 100%, very similar to your visit.",
      why: ["Flow: 6 in 10 days higher then, 6 in 10 days higher now. The same: 100% alike, 45% of the score."] },
    { date: "2026-10-24", top: "Sat 24 Oct", sub: "", score: "61%", read: "Different", basis: "weather only", best: false,
      reasons: ["Air temperature: 6° cooler", "Wind: the same"], lead: "Air temperature: 6° cooler",
      label: "Sat, 24 Oct: 61%, different from your visit, on the weather only.",
      why: ["No flow forecast for this day: scored on the weather only.", "Flow: no flow forecast for this day. Not scored."] },
  ],
};

const rgb = (hex: string) => {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255})`;
};

describe.each([["light", LIGHT], ["dark", DARK]] as [string, Palette][])("<LikeVisitStrip> (%s)", (_, palette) => {
  it("draws the visit and a row per day — the day, its biggest difference, its score — the closest marked", () => {
    const { getByTestId, queryByTestId } = render(
      <SpotScreen spot={spot} palette={palette} onBack={() => {}} likeVisit={VIEW} />);
    expect(getByTestId("like-visit").textContent).toBe(
      "How alike each of the next five days looks to the day you were here."
      + "Your visitSun 30 Aug"
      + "TodayThu 22 Oct" + "CLOSEST TO YOUR VISIT" + "Much the same as your visit" + "100%Very similar"
      + "Sat 24 Oct" + "Air temperature: 6° cooler" + "61%Different"
      + "Forecast checked 2h ago.");
    expect(getByTestId("spot-next-days").textContent).toContain("The next few days");
    expect(getByTestId("like-day-2026-10-22").style.borderLeftColor).toBe(rgb(palette.ink));
    expect(getByTestId("like-day-2026-10-24").style.borderLeftWidth).toBe("0px");
    expect(queryByTestId("like-why-2026-10-24")).toBeNull();
  });

  it("a tap on a row opens its reasons, and why flow is out; a second tap closes them", () => {
    const { getByLabelText, getByTestId, queryByTestId } = render(
      <SpotScreen spot={spot} palette={palette} onBack={() => {}} likeVisit={VIEW} />);
    const row = getByLabelText("Sat, 24 Oct: 61%, different from your visit, on the weather only.");
    expect(row.getAttribute("aria-expanded")).toBe("false");
    fireEvent.click(row);
    expect(getByTestId("like-why-2026-10-24").textContent).toBe("weather only" + "Air temperature: 6° cooler" + "Wind: the same");
    expect(row.getAttribute("aria-expanded")).toBe("true");
    fireEvent.click(row);
    expect(queryByTestId("like-why-2026-10-24")).toBeNull();
  });
});

describe("<SpotScreen> and the strip", () => {
  it("a draft has no strip; a visit with nothing recorded says so", () => {
    const draft = render(<SpotScreen spot={spot} palette={LIGHT} mode="draft" likeVisit={VIEW} />);
    expect(draft.queryByTestId("like-visit")).toBeNull();
    cleanup();
    const bare = render(<SpotScreen spot={spot} palette={LIGHT} onBack={() => {}} likeVisit={{ ...VIEW, days: null,
      note: "Nothing was recorded at this visit to compare with: no flow, no weather." }} />);
    expect(bare.getByTestId("like-note").textContent)
      .toBe("Nothing was recorded at this visit to compare with: no flow, no weather.");
  });
});
