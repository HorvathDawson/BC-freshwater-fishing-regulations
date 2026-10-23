/**
 * T51: the "Like your visit?" strip on a saved spot, mounted in light and dark: the columns said whole, a tap opens
 * the day's working, and it is absent from a draft.
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
  heading: "Like your visit?",
  intro: "How alike each of the next five days looks to the day you were here.",
  visit: { top: "Your visit", sub: "30 Aug", label: "Your visit, 30 Aug: the day the others are compared with." },
  age: "Forecast checked 2h ago.",
  note: null,
  days: [
    { date: "2026-10-22", top: "Today", sub: "22 Oct", score: "100%", read: "Very similar", basis: null,
      label: "Today, 22 Oct: 100%, very similar to your visit.",
      why: ["Flow: 42nd percentile then, 42nd percentile now. The same: 100% alike, 45% of the score."] },
    { date: "2026-10-24", top: "Sat", sub: "24 Oct", score: "61%", read: "Different", basis: "weather only",
      label: "Sat, 24 Oct: 61%, different from your visit, on the weather only.",
      why: ["No flow forecast for this day: scored on the weather only.", "Flow: no flow forecast for this day. Not scored."] },
  ],
};

const rgb = (hex: string) => {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255})`;
};

describe.each([["light", LIGHT], ["dark", DARK]] as [string, Palette][])("<LikeVisitStrip> (%s)", (_, palette) => {
  it("draws the visit and each day said whole, in the theme's ink", () => {
    const { getByTestId, getByLabelText } = render(
      <SpotScreen spot={spot} palette={palette} onBack={() => {}} likeVisit={VIEW} />);
    expect(getByTestId("like-visit").textContent).toBe(
      "How alike each of the next five days looks to the day you were here."
      + "Your visit30 Aug" + "Today22 Oct100%Very similar" + "Sat24 Oct61%Differentweather only"
      + "Forecast checked 2h ago.");
    const today = getByLabelText("Today, 22 Oct: 100%, very similar to your visit.");
    expect(today.getAttribute("aria-selected")).toBe("false");
    expect((today.querySelector("div") as HTMLElement).style.color).toBe(rgb(palette.ink));
    expect(getByLabelText("Your visit, 30 Aug: the day the others are compared with.")).toBeTruthy();
  });

  it("a tap shows what drives the day's score; a second tap closes it", () => {
    const { getByLabelText, getByTestId, queryByTestId } = render(
      <SpotScreen spot={spot} palette={palette} onBack={() => {}} likeVisit={VIEW} />);
    const sat = getByLabelText("Sat, 24 Oct: 61%, different from your visit, on the weather only.");
    fireEvent.click(sat);
    expect(getByTestId("like-why").textContent).toBe(
      "Sat, 24 Oct: 61%, different from your visit, on the weather only."
      + "No flow forecast for this day: scored on the weather only."
      + "Flow: no flow forecast for this day. Not scored.");
    expect(sat.getAttribute("aria-selected")).toBe("true");
    fireEvent.click(sat);
    expect(queryByTestId("like-why")).toBeNull();
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
