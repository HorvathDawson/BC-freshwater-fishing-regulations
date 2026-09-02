/**
 * The gauge badge, mounted.
 *
 * One failure mode dominates: showing a live number's worth of confidence for a station
 * that closed in 2004, or for one measuring a river forty times the size. Every test here
 * checks that the four states stay four.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render } from "@testing-library/react";
import { GaugeBadge } from "./GaugeBadge";
import type { GaugeLink } from "@app/data";
import { LIGHT } from "./theme";

afterEach(cleanup);

const link = (over: Partial<GaugeLink> = {}): GaugeLink => ({
  station: "08MH016" as GaugeLink["station"], name: "Chilliwack River at Vedder Crossing",
  trust: "good", reachMagnitude: 0, gaugeMagnitude: 1200,
  live: true, areaKm2: 1233, lon: -121.96, lat: 49.09, section: null, ...over,
});

describe("<GaugeBadge>", () => {
  it("says plainly that a water is not measured, and why", () => {
    const { getByText } = render(
      <GaugeBadge gauge={null} palette={LIGHT} waterName="Sowaqua Creek" />);
    expect(getByText(/No hydrometric station drains enough of Sowaqua Creek/)).toBeTruthy();
  });

  it("does not offer a number for a water nothing measures", () => {
    const { queryByText } = render(<GaugeBadge gauge={null} palette={LIGHT} />);
    expect(queryByText(/km²/)).toBeNull();
  });

  it("names the station rather than showing its id alone", () => {
    const { getByText } = render(<GaugeBadge gauge={link()} palette={LIGHT} />);
    expect(getByText(/Chilliwack River at Vedder Crossing/)).toBeTruthy();
  });

  it("separates a station that stopped reporting from one that has not", () => {
    const dead = render(<GaugeBadge gauge={link({ live: false })} palette={LIGHT} />);
    expect(dead.getByText(/stopped reporting/)).toBeTruthy();
    expect(dead.getByText(/record here, not a reading/)).toBeTruthy();
    cleanup();

    const alive = render(<GaugeBadge gauge={link()} palette={LIGHT} />);
    expect(alive.queryByText(/stopped reporting/)).toBeNull();
  });

  it("marks a weak link as nearby rather than as this water", () => {
    const { getByLabelText } = render(
      <GaugeBadge gauge={link({ trust: "weak" })} palette={LIGHT} />);
    expect(getByLabelText("Gauged nearby")).toBeTruthy();
  });

  it("uses the same words for trust the rest of the app does", () => {
    // trustWord() is the one definition; a second phrasing here is the drift rule 23 bans.
    const { getByText } = render(
      <GaugeBadge gauge={link({ trust: "fair" })} palette={LIGHT} />);
    expect(getByText(/a major branch of it/)).toBeTruthy();
  });

  it("omits a drainage area nobody published instead of printing zero", () => {
    const { queryByText } = render(
      <GaugeBadge gauge={link({ areaKm2: null })} palette={LIGHT} />);
    expect(queryByText(/km²/)).toBeNull();
  });
});

describe("<GaugeBadge> when liveness is unknown", () => {
  // Liveness comes from the feed now, so "offline" and "the station shut down" arrive as
  // the same absence. Telling an offline reader every gauge has stopped is the failure.
  it("does not claim a station stopped when we simply could not check", () => {
    const { getByLabelText, queryByText } = render(
      <GaugeBadge gauge={link({ live: null })} palette={LIGHT} />);
    expect(getByLabelText("Gauged — not checked")).toBeTruthy();
    expect(queryByText(/stopped reporting/)).toBeNull();
  });

  it("says plainly that the feed was unreachable", () => {
    const { getByText } = render(
      <GaugeBadge gauge={link({ live: null })} palette={LIGHT} />);
    expect(getByText(/could not reach the live feed/)).toBeTruthy();
  });

  it("still keeps 'stopped' available for a station that really has", () => {
    const { getByText } = render(
      <GaugeBadge gauge={link({ live: false })} palette={LIGHT} />);
    expect(getByText(/stopped reporting/)).toBeTruthy();
  });

  it("keeps all three states distinct", () => {
    const head = (live: boolean | null) => {
      const r = render(<GaugeBadge gauge={link({ live })} palette={LIGHT} />);
      const el = r.container.querySelector("[aria-label^='Gauged']");
      const label = el?.getAttribute("aria-label") ?? null;
      cleanup();
      return label;
    };
    expect(new Set([head(true), head(false), head(null)]).size).toBe(3);
  });
});

describe("<GaugeBadge> record length", () => {
  it("shows how long the record is, because a percentile without it overclaims", () => {
    const { getByText } = render(
      <GaugeBadge gauge={link()} palette={LIGHT}
                  record={{ fromYear: 1913, toYear: 2026, years: 97 }} />);
    expect(getByText(/97 years of record · 1913–2026/)).toBeTruthy();
  });

  it("says nothing at all when the record length is unknown", () => {
    // A blank is honest. Inventing "unknown years" reads as a defect rather than a gap.
    const { queryByText } = render(<GaugeBadge gauge={link()} palette={LIGHT} />);
    expect(queryByText(/years of record/)).toBeNull();
  });

  it("keeps the record next to the trust sentence, not instead of it", () => {
    const { getByText } = render(
      <GaugeBadge gauge={link()} palette={LIGHT}
                  record={{ fromYear: 1913, toYear: 2026, years: 97 }} />);
    expect(getByText(/describes this water/)).toBeTruthy();
    expect(getByText(/97 years/)).toBeTruthy();
  });
});
