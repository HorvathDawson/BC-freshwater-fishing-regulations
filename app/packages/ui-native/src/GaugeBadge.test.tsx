/**
 * The gauge badge, mounted.
 *
 * One failure mode dominates: showing a live number's worth of confidence for a station
 * that closed in 2004, or for one measuring a river forty times the size. Every test here
 * checks that the four states stay four.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render } from "@testing-library/react";
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

  /**
   * THE WHOLE SENTENCE, EXACTLY — every band against every liveness state.
   *
   * This replaces a test that rendered `trust: "fair"` and asserted
   * `getByText(/a major branch of it/)`. It passed for as long as the badge was rendering
   * "It a major branch of it." — because the fragment it matched was the fragment the
   * component had just interpolated, and the assertion was therefore true of the broken
   * string as well as the good one. A regex over a substring you supplied cannot fail.
   *
   * So: exact strings, and one row per combination. If a phrase changes in core, these
   * fail and are meant to — that is the drift rule 23 asks for, made visible.
   */
  it.each([
    ["good", true,  "It describes this water."],
    ["fair", true,  "It describes a major branch of it."],
    ["weak", true,  "It shows the trend, not the level."],
    ["good", null,  "It describes this water. We could not reach the live feed, so whether " +
                    "it is reporting today is unknown."],
    ["fair", false, "It describes a major branch of it, but the station has stopped " +
                    "reporting — there is a record here, not a reading."],
  ] as const)("says the whole sentence for %s / live=%s", (trust, live, sentence) => {
    const { getByText } = render(
      <GaugeBadge gauge={link({ trust, live })} palette={LIGHT} />);
    expect(getByText(sentence)).toBeTruthy();
  });

  it("never renders a sentence that begins with a bare 'It ' fragment", () => {
    // The shape of the original bug, pinned directly: whatever the band, the first clause
    // has to be a verb phrase. "It a major branch of it." is what this refuses.
    for (const trust of ["good", "fair", "weak"] as const) {
      const { container } = render(
        <GaugeBadge gauge={link({ trust })} palette={LIGHT} />);
      expect(container.textContent ?? "").not.toMatch(/\bIt (?:a|the|an) \w/);
      cleanup();
    }
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

describe("<ConditionsScreen>", () => {
  const src = (over: Record<string, unknown> = {}) => ({
    gaugeForSection: async () => null,
    gaugeNow: async () => null,
    gaugeSeries: async () => null,
    gaugeParameters: async () => [],
    traceToGauge: async () => [],
    // The screen asks which water the reach belongs to, so the Regulations toggle has
    // somewhere to go and the screen has a title. A tile carries a section id and nothing
    // else. Null here is the unnamed case — a reach on water with no registry item — and
    // the screen must render without a title rather than without a screen.
    itemForSection: async () => null,
    waterFor: async () => null,
    panelsFor: async () => new Map(),
    panelRoutes: async () => [],
    ...over,
  } as never);

  it("names the water at the top, the way the Regulations face does", async () => {
    // Two faces of one water must look like two faces of one water. This one used to open
    // on a chart and a column of numbers with nothing saying which river.
    const { ConditionsScreen } = await import("./ConditionsScreen");
    const { findByText } = render(
      <ConditionsScreen section={"1:0" as never} palette={LIGHT} onBack={() => {}}
                        source={src({ waterFor: async () => ({ item: "gnis:1",
                                                               name: "Coquihalla River",
                                                               kind: "stream" }) })} />);
    expect(await findByText("Coquihalla River")).toBeTruthy();
  });

  it("says WHY there is no reading, rather than showing a blank", async () => {
    const { ConditionsScreen } = await import("./ConditionsScreen");
    const { findByText } = render(
      <ConditionsScreen source={src()} section={"1:0" as never} palette={LIGHT}
                        onBack={() => {}} />);
    expect(await findByText(/No gauge is entitled to speak/)).toBeTruthy();
  });

  it("offers the other question, and leaves rather than answering it here", async () => {
    // ONE CONDITIONS SCREEN. The rules sheet used to render its own conditions face, so the
    // app answered "what is the water doing" two ways depending on which tab you came from.
    // Both toggles now navigate to the single screen that owns each answer.
    const { ConditionsScreen } = await import("./ConditionsScreen");
    const onRegulations = vi.fn();
    const { findByLabelText } = render(
      <ConditionsScreen source={src()} section={"1:0" as never} palette={LIGHT}
                        onBack={() => {}} onRegulations={onRegulations} />);
    fireEvent.click(await findByLabelText("Regulations"));
    // BY SECTION, not by item. Resolving the water here would make the button do nothing
    // for the first few hundred milliseconds after the screen appears; the shell already
    // owns that lookup for a map tap and does it once.
    expect(onRegulations).toHaveBeenCalledWith("1:0");
  });

  it("offers a way back, because it is a detail view not a tab", async () => {
    const { ConditionsScreen } = await import("./ConditionsScreen");
    const onBack = vi.fn();
    const { findByLabelText } = render(
      <ConditionsScreen source={src()} section={"1:0" as never} palette={LIGHT}
                        onBack={onBack} />);
    fireEvent.click(await findByLabelText("Back"));
    expect(onBack).toHaveBeenCalled();
  });
});
