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
import type { DonorRow, PanelAnswer } from "@app/ui";
import { DonorPanel } from "./DonorPanel";
import { LIGHT } from "./theme";

afterEach(cleanup);

// TYPED AS THE REAL ROW on purpose. It used to be an untyped object literal, so when the
// row grew a required field the compiler said nothing and eight render tests failed at
// runtime instead. A fixture that is not the shape it stands in for is not a fixture.
const row = (station: string, over: Partial<DonorRow> = {}): DonorRow => ({
  station: station as never, role: "up", percentile: 0.12,
  weight: 0.6, areaRatio: 2.4, areaKm2: 120, trust: "close", years: 40,
  factors: { share: 0.42, role: 1, record: 1 }, ...over,
});

const answer = (over = {}): PanelAnswer => ({
  answer: { ok: true, value: { percentile: 0.15, plusMinus: 12, trust: "close",
                               spread: 6, donors: 2, ...over } },
  rows: [row("08MH001"), row("08MH024", { weight: 0.4, role: "down" })],
  areaKm2: 50, routesReady: true,
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
    // ANCHORED TO THE FACT LINE ("upstream · 120 km² · 2.4× apart"), not to the words
    // anywhere on screen: the explanation under each row also says "downstream", and the
    // screen-reader label for the row repeats the size too. A bare word match counts all
    // of them and then reports a number that says nothing about the layout.
    const facts = screen.getAllByText(/^(upstream|downstream) · drains .*(apart|same size)$/);
    expect(facts.length).toBe(2);
    expect(facts.map((f) => f.textContent)).toEqual([
      "upstream · drains 120 km² · 2.4× apart",
      "downstream · drains 120 km² · 2.4× apart",
    ]);
  });

  it("says a station is quiet rather than dropping it or printing a zero", () => {
    // A gauge that is in the panel but not reporting today is a fact about the panel. A
    // missing row reads as "this gauge does not exist", and a 0 reads as a dry river.
    const v = answer();
    render(<DonorPanel palette={LIGHT}
                       value={{ ...v, rows: [row("08MH001", { percentile: null })] }} />);
    // Not "0%", which reads as "this gauge is not in the panel" — a different claim.
    expect(screen.getByText("not counted today")).toBeTruthy();
    expect(screen.getByText("Not reporting today.")).toBeTruthy();
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
    render(<DonorPanel palette={LIGHT} value={{ answer: { ok: false, why }, rows: [], areaKm2: null,
                                routesReady: true }} />);
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

  it("says what each gauge contributes, as a number and not only as a bar", () => {
    // The ask this answers: a reader could see one bar longer than another and had no way
    // to say how much longer, or to quote it.
    render(<DonorPanel palette={LIGHT} value={answer()} />);
    // LABELLED. Three bare percentages used to sit in one row — the weight, the gauge's
    // own reading and the catchment overlap — and nothing said which was which.
    expect(screen.getByText("60% of this estimate")).toBeTruthy();
    expect(screen.getByText("40% of this estimate")).toBeTruthy();
  });

  it("never rounds a real contribution down to nothing", () => {
    // "0%" reads as "this gauge is not in the panel", which is a different claim.
    const v = answer();
    render(<DonorPanel palette={LIGHT}
                       value={{ ...v, rows: [row("08MH001", { weight: 0.003 })] }} />);
    expect(screen.getByText("<1% of this estimate")).toBeTruthy();
  });

  it("explains the contribution in the model's own terms", () => {
    const v = answer();
    render(<DonorPanel palette={LIGHT} value={{ ...v, rows: [
      row("08MH001", { factors: { share: 0.42, role: 0.85, record: 0.5 }, years: 10 }),
    ] }} />);
    expect(screen.getByText(/its watershed overlaps this one by 42%/)).toBeTruthy();
    expect(screen.getByText(/it sits downstream, so it carries extra water/)).toBeTruthy();
    expect(screen.getByText(/its record is only 10 years, so it counts 50%/)).toBeTruthy();
  });

  it("gives the spot's own catchment, so a share can be checked", () => {
    render(<DonorPanel palette={LIGHT} value={answer()} />);
    expect(screen.getByText(/this spot drains 50 km²/)).toBeTruthy();
  });

  it("leads with a sentence, not a percentile", () => {
    // The ask: someone with no statistics should be able to read the top of this panel.
    render(<DonorPanel palette={LIGHT} value={answer({ percentile: 0.15 })} />);
    expect(screen.getByText("Low for the time of year")).toBeTruthy();
    expect(screen.getByText(/Lower than \d days in 10/)).toBeTruthy();
    // ...and the precise number is still there, one size down, for a reader who wants it.
    expect(screen.getByText(/\d+\w+–\d+\w+ percentile/)).toBeTruthy();
  });

  it("sounds less certain when the interval is wide", () => {
    render(<DonorPanel palette={LIGHT} value={answer({ plusMinus: 22 })} />);
    expect(screen.getByText(/very roughly/)).toBeTruthy();
  });

  it("says how far each gauge is in reaches when it knows the route", () => {
    const v = answer();
    render(<DonorPanel palette={LIGHT} value={{ ...v, rows: [
      row("08MH001", { route: { station: "08MH001" as never, name: "VEDDER", lat: 49,
                                lon: -122, role: "up", path: ["a", "b", "c"] as never } }),
      row("08MH024", { route: { station: "08MH024" as never, name: null, lat: 49,
                                lon: -122, role: "up", path: ["a"] as never } }),
    ] }} />);
    expect(screen.getByText(/2 reaches away/)).toBeTruthy();
    expect(screen.getByText(/on this reach/)).toBeTruthy();
  });

  it("draws no map when the caller has no tiles, and still lists the gauges", () => {
    // The three callers do not all have tiles. The words are the panel; the map is an
    // addition to them, and its absence must not take the answer with it.
    render(<DonorPanel palette={LIGHT} value={answer()} />);
    expect(screen.getByText("08MH001")).toBeTruthy();
    expect(screen.queryByText(/reaches between here/)).toBeNull();
  });

  it("says a gauge is quiet in the key, never that it contributed nothing", () => {
    // "0%" in the key reads as "this gauge is not in the panel" — a different claim, and
    // the wrong one. It is in the panel; it is not reporting.
    const v = answer();
    render(<DonorPanel palette={LIGHT}
                       value={{ ...v, rows: [row("08MH001", { percentile: null,
                                                              weight: 0 })] }} />);
    expect(screen.getAllByText(/not counted today|Not reporting today/).length)
      .toBeGreaterThan(0);
    expect(screen.queryByText("0% of this estimate")).toBeNull();
  });

  it("draws no map until the routes are in", () => {
    /*
     * A MapLibre map reads its opening camera ONCE. Mounted before the walks finish, it is
     * framed on the tap alone — one pin of five — and no later camera moves it. So the map
     * waits, and this is the test that keeps it waiting.
     */
    const v = answer();
    const tiles = { atlas: "x", basemap: "y" } as never;
    render(<DonorPanel palette={LIGHT} theme="light" at={tiles} from={{ lat: 49, lon: -122 }}
                       value={{ ...v, routesReady: false }} />);
    expect(screen.queryByText(/reaches between here/)).toBeNull();
    // ...and the answer itself is on screen the whole time. The map is an addition to the
    // words, never a gate on them.
    expect(screen.getByText("08MH001")).toBeTruthy();
  });

  it("says what each gauge itself reads, in the same words as the headline", () => {
    // A bare "p33rd" beside a row could be taken for the answer rather than for one
    // gauge's input to it.
    render(<DonorPanel palette={LIGHT}
                       value={{ ...answer(), rows: [row("08MH001", { percentile: 0.2 })] }} />);
    expect(screen.getByText(/Reads lower than 8 days in 10/)).toBeTruthy();
  });

  it("describes each row for a screen reader, not only for the eye", () => {
    // The weight is drawn as a bar, which is invisible to anything that cannot see it.
    render(<DonorPanel palette={LIGHT} value={answer()} />);
    expect(screen.getByLabelText(/08MH001, upstream, .*per cent of the answer/)).toBeTruthy();
  });
});
