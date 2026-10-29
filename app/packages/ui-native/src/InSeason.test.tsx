/**
 * (T230) The in-season banner draws core's line whole; the notice lists the unmatched rows; the bell carries the count.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { inSeasonBanner, inSeasonNoticeRows } from "@app/core";
import { InSeasonBanners } from "./regs/InSeasonBanner";
import { InSeasonNotice } from "./InSeasonNotice";
import { TopBar } from "./TopBar";
import { LIGHT } from "./theme";

afterEach(cleanup);
const SRC = "https://www2.gov.bc.ca/x";
const change = { id: "is-a", region: "8", water: "Okanagan River", from: "2026-10-15", to: "2026-11-30",
                 text: "No fishing July 30 to September 15.", source: SRC, extents: [] };

describe("in-season", () => {
  it("<InSeasonBanners>: the owner's line, whole, then the note and the source link", () => {
    render(<InSeasonBanners banners={[inSeasonBanner({ change, partial: false })]} palette={LIGHT} />);
    expect(screen.getByTestId("inseason-banner").textContent).toBe(
      "In-season change (Oct 15–Nov 30): No fishing July 30 to September 15."
      + "Shown as posted; the answers on this page do not include it yet.source ›");
  });
  it("<InSeasonBanners>: nothing to draw, nothing drawn", () => {
    render(<InSeasonBanners banners={[]} palette={LIGHT} />);
    expect(screen.queryByTestId("inseason-banners")).toBeNull();
  });
  it("<InSeasonNotice>: each row's region · water · dates and the text", () => {
    render(<InSeasonNotice open rows={inSeasonNoticeRows([{ ...change, region: "4", water: "Kaslo River", to: null, from: "2025-04-04" }])}
                           palette={LIGHT} onClose={() => {}} />);
    expect(screen.getAllByTestId("inseason-notice-row").map((r) => r.textContent))
      .toEqual(["Region 4 · Kaslo River · since Apr 4, 2025No fishing July 30 to September 15.source ›"]);
  });
  it("<TopBar>: the bell with its count opens the notice; no count, no bell", () => {
    let opened = 0;
    const bar = (count: number) => (
      <TopBar palette={LIGHT} placeholder="p" sync={null} open={false} query="" onQuery={() => {}} onOpen={() => {}}
              onClose={() => {}} notices={{ count, label: `${count} in-season changes not on the map yet`, onPress: () => { opened++; } }} />);
    const { rerender } = render(bar(3));
    expect(screen.getByTestId("inseason-count").textContent).toBe("3");
    fireEvent.click(screen.getByLabelText("3 in-season changes not on the map yet"));
    expect(opened).toBe(1);
    rerender(bar(0));
    expect(screen.queryByTestId("inseason-bell")).toBeNull();
  });
});
