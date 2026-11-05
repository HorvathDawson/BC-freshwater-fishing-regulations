/**
 * T377 — peak markers on the Runs histogram: each chosen species' busiest pentad in its own colour (the season's, in ink,
 * when nothing is stacked), placed over that pentad's bar, named in core's words; no run, no marker.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { RUN_WORDS } from "@app/core";
import { RunDaySlider } from "./RunsControls";
import { LIGHT } from "./theme";

afterEach(cleanup);

/** a palette colour as the browser reads it back */
const css = (c: string) => { const d = document.createElement("div"); d.style.backgroundColor = c; return d.style.backgroundColor; };

const z = { spawn: 0, pass: 0 };
const season = (at: number, n = 73) => Array.from({ length: n }, (_, i) => (i === at ? { spawn: 0.5, pass: 0.2 } : i === at + 1 ? { spawn: 0.1, pass: 0 } : z));
const slider = (p: Partial<Parameters<typeof RunDaySlider>[0]>) => render(
  <RunDaySlider pentad={0} today={0} playing={false} onPentad={() => {}} onPlay={() => {}} season={season(40)}
                theme="light" palette={LIGHT} {...p} />);

describe("(T377) the Runs histogram's peaks", () => {
  it("one marker per chosen species, at its own busiest bar, in its colour, named whole", () => {
    slider({ bySpecies: [{ name: "Chinook", colour: LIGHT.accent, season: season(30) },
                         { name: "Coho", colour: LIGHT.sub, season: season(50) }] });
    const chinook = screen.getByTestId("run-peak-Chinook"), coho = screen.getByTestId("run-peak-Coho");
    expect(chinook.style.left).toBe(`${(30.5 / 73) * 100}%`);
    expect(coho.style.left).toBe(`${(50.5 / 73) * 100}%`);
    expect(getComputedStyle(chinook).backgroundColor).toBe(css(LIGHT.accent));
    expect(chinook.getAttribute("aria-label")).toBe(RUN_WORDS.slider.peak("Chinook", 30));
    expect(screen.queryByTestId("run-peak-season")).toBeNull();
  });

  it("unstacked: the season's own peak in ink", () => {
    slider({});
    const m = screen.getByTestId("run-peak-season");
    expect(m.style.left).toBe(`${(40.5 / 73) * 100}%`);
    expect(m.getAttribute("aria-label")).toBe(RUN_WORDS.slider.peak(null, 40));
  });

  it("a species with no run is no marker", () => {
    slider({ bySpecies: [{ name: "Pink (odd)", colour: LIGHT.accent, season: Array.from({ length: 73 }, () => z) }] });
    expect(screen.queryByTestId("run-peak-Pink (odd)")).toBeNull();
  });
});
