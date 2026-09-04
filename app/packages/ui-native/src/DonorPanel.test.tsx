/**
 * The working behind an estimate, mounted.
 *
 * The failure mode worth testing is not "it renders a list". It is the app claiming more
 * than it knows: a bare percentile where the measurement says ±12 points at best, a
 * confident label built on one distant gauge, or a blank where a reader is owed a reason.
 * Every case below is one of those.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import type { NoEstimate } from "@app/core";
import type { PanelAnswer } from "@app/ui";
import { DonorPanel } from "./DonorPanel";
import { LIGHT } from "./theme";

afterEach(cleanup);

const row = (station: string, over = {}) => ({
  station: station as never, role: "up" as const, percentile: 0.12,
  weight: 0.6, areaRatio: 2.4, trust: "close" as const, years: 40, ...over,
});

const answer = (over = {}): PanelAnswer => ({
  answer: { ok: true, value: { percentile: 0.15, plusMinus: 12, trust: "close",
                               spread: 6, donors: 2, ...over } },
  rows: [row("08MH001"), row("08MH024", { weight: 0.4, role: "down" })],
});

describe("the donor panel", () => {
  it("shows a RANGE and never a bare percentile", () => {
    // Measured over 9,495 nested gauge pairs: even a donor of nearly identical size is out
    // by 11.7 points at the median. A single number here would claim a precision that does
    // not exist at any distance.
    render(<DonorPanel palette={LIGHT} value={answer()} />);
    expect(screen.getByText(/\d+\w+–\d+\w+/)).toBeTruthy();
  });

  it("names every gauge that voted, with where it is and how far off in size", () => {
    render(<DonorPanel palette={LIGHT} value={answer()} />);
    expect(screen.getByText("08MH001")).toBeTruthy();
    expect(screen.getByText("08MH024")).toBeTruthy();
    expect(screen.getAllByText(/upstream|downstream/).length).toBe(2);
    expect(screen.getAllByText(/× apart|same size/).length).toBe(2);
  });

  it("says a station is quiet rather than dropping it or printing a zero", () => {
    // A gauge that is in the panel but not reporting today is a fact about the panel. A
    // missing row reads as "this gauge does not exist", and a 0 reads as a dry river.
    const v = answer();
    render(<DonorPanel palette={LIGHT}
                       value={{ ...v, rows: [row("08MH001", { percentile: null })] }} />);
    expect(screen.getByText("quiet")).toBeTruthy();
  });

  it("warns in words when the gauges disagree", () => {
    // Three gauges spanning the 10th to the 60th is a catchment doing more than one thing,
    // and their average is the least useful thing to show.
    render(<DonorPanel palette={LIGHT} value={answer({ spread: 44 })} />);
    expect(screen.getByText(/disagree by 44 points/)).toBeTruthy();
  });

  it("stays quiet about disagreement when there is none", () => {
    render(<DonorPanel palette={LIGHT} value={answer({ spread: 4 })} />);
    expect(screen.queryByText(/disagree/)).toBeNull();
  });

  const REASONS: NoEstimate[] = ["no-station", "no-record", "regulated", "too-uncertain",
                                 "offline"];
  it.each(REASONS)("explains a refusal (%s) instead of showing a blank", (why) => {
    // Silence is the design's best property, and a reader is owed the reason — each of
    // these is a different sentence and a different thing to do about it.
    render(<DonorPanel palette={LIGHT} value={{ answer: { ok: false, why }, rows: [] }} />);
    expect(screen.getByText("NO ESTIMATE")).toBeTruthy();
    const body = document.body.textContent ?? "";
    expect(body.length).toBeGreaterThan("NO ESTIMATE".length + 30);
  });

  it("counts one gauge as one gauge", () => {
    // "1 gauges" is the kind of thing that survives review and then ships.
    render(<DonorPanel palette={LIGHT}
                       value={{ ...answer({ donors: 1 }), rows: [row("08MH001")] }} />);
    expect(screen.getByText(/one gauge/)).toBeTruthy();
  });

  it("describes each row for a screen reader, not only for the eye", () => {
    // The weight is drawn as a bar, which is invisible to anything that cannot see it.
    render(<DonorPanel palette={LIGHT} value={answer()} />);
    expect(screen.getByLabelText(/08MH001, upstream, .*per cent of the answer/)).toBeTruthy();
  });
});
