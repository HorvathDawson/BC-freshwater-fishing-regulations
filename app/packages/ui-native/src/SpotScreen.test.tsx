/**
 * A saved spot, mounted.
 *
 * The whole risk on this screen is TIME. Every figure was true once and is displayed
 * later, so the tests below all ask the same question in different ways: can a reader
 * mistake a record for a reading? A stale discharge shown bare is worse than no discharge,
 * because the reader will wade on it.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render } from "@testing-library/react";
import type { PhotoEdit } from "@app/ui";
import { SpotScreen } from "./SpotScreen";
import type { Spot } from "@app/data/spots";
import { LIGHT } from "./theme";

afterEach(cleanup);

// LOCAL parts, so the dated lines below read the same in any time zone the suite runs in.
const VISIT = new Date(2026, 7, 30, 9).getTime();
const spot = (over: Partial<Spot> = {}): Spot => ({
  id: "s1", createdAt: VISIT, visitedAt: VISIT, updatedAt: 0,
  lat: 49.0974, lon: -121.9675, item: null, waterName: null,
  title: "Tamihi run", notes: "", photos: [],
  reading: null, weather: null, trace: null, panel: null, ...over,
});

describe("<SpotScreen>", () => {
  it("dates every spot, because a spot is a record and not a reading", () => {
    const { getByTestId } = render(
      <SpotScreen spot={spot()} palette={LIGHT} onBack={() => {}} />);
    expect(getByTestId("spot-when").textContent).toBe("2026-08-30 at 09:00 · 49.0974, -121.9675");
  });

  it("says WHY there is no discharge instead of showing a dash", () => {
    // A gauge that does not speak for this water is not a missing value — it is an answer.
    const { getByText, queryByText } = render(
      <SpotScreen spot={spot()} palette={LIGHT} onBack={() => {}} />);
    expect(getByText("No gauge is entitled to speak for this water, so no reading was recorded. A station " +
                     "draining a far larger watershed would have given a number, and the number would have been " +
                     "wrong.")).toBeTruthy();
    expect(queryByText("m³/s")).toBeNull();
  });

  it("marks a value that was filled in after the fact", () => {
    const { getByText, getByTestId } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} spot={spot({
        reading: { station: "08MH016", discharge: 41.2, level: 1.42, percentile: 0.62,
                   at: "2026-08-30T09:00", backfilled: true },
      })} />);
    expect(getByTestId("spot-observed").textContent)
      .toBe("08MH016 · observed 2026-08-30 09:00 · filled in afterwards");
    expect(getByText("41.2")).toBeTruthy();
  });

  it("does not claim a backfill when the reading was live", () => {
    const { getByTestId } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} spot={spot({
        reading: { station: "08MH016", discharge: 41.2, level: null, percentile: null,
                   at: "2026-08-30T09:00", backfilled: false },
      })} />);
    expect(getByTestId("spot-observed").textContent).toBe("08MH016 · observed 2026-08-30 09:00");
  });

  it("offers a refresh only when something is actually stale", () => {
    const fresh = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} onRefresh={() => {}} spot={spot({
        weather: { window: null, tempC: 14, windKph: 8, windDir: 210, rain3h: 0, pressureHpa: 1014,
                   code: 3, at: "2026-08-30T09:00", backfilled: false },
        reading: { station: "08MH016", discharge: 41.2, level: null, percentile: null,
                   at: "2026-08-30T09:00", backfilled: false },
        // S1: the week before is part of what a fresh spot has
        leadUp: { weather: [], station: null, gauge: null },
      })} />);
    expect(fresh.queryByLabelText("Refresh this spot")).toBeNull();
    cleanup();

    const stale = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} onRefresh={() => {}} spot={spot()} />);
    expect(stale.queryByLabelText("Refresh this spot")).toBeTruthy();
  });

  it("gets out of the way — back never deletes", () => {
    const onBack = vi.fn(), onDelete = vi.fn();
    const { getByLabelText } = render(
      <SpotScreen spot={spot()} palette={LIGHT} onBack={onBack} onDelete={onDelete} />);
    fireEvent.click(getByLabelText("Back"));
    expect(onBack).toHaveBeenCalled();
    expect(onDelete).not.toHaveBeenCalled();
  });
});

describe("<SpotScreen> cloud window", () => {
  const hour = (cloud: number, hPa: number, rh = 70, t = 14) =>
    ({ at: "2026-08-30T08:00", tempC: t, cloudPct: cloud, pressureHpa: hPa,
       humidityPct: rh });
  const w = (over: Partial<NonNullable<Spot["weather"]>> = {}) => ({
    tempC: 14, windKph: 8, windDir: 210, rain3h: 0, pressureHpa: 1014, code: 3,
    window: { before: hour(35, 1014), at: hour(60, 1013), after: hour(85, 1012) },
    at: "2026-08-30T09:00", backfilled: false, ...over,
  });

  it("shows the hour either side, so the direction is readable", () => {
    const { getByTestId, getByLabelText } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} spot={spot({ weather: w() })} />);
    expect(getByLabelText("Cloud −1 h").textContent).toBe("35%");
    expect(getByTestId("spot-trend").textContent)
      .toBe("clouding over · pressure falling — across the hour either side");
  });

  it("calls it clearing when the sky opened up", () => {
    const { getByTestId } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} spot={spot({ weather: w({
        window: { before: hour(90, 1014), at: hour(60, 1014), after: hour(20, 1014) },
      }) })} />);
    expect(getByTestId("spot-trend").textContent)
      .toBe("clearing · pressure steady — across the hour either side");
  });

  it("reports the barometer's direction, which decides as much as the sky", () => {
    const { getByTestId } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} spot={spot({ weather: w({
        window: { before: hour(60, 1016), at: hour(60, 1014), after: hour(60, 1011) },
      }) })} />);
    expect(getByTestId("spot-trend").textContent)
      .toBe("cloud holding · pressure falling — across the hour either side");
  });

  it("shows humidity and pressure at each hour, not only at the visit", () => {
    const { getByLabelText } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} spot={spot({ weather: w() })} />);
    for (const col of ["−1 h", "visit", "+1 h"])
      for (const row of ["Pressure", "Humidity", "Cloud", "Temp"])
        expect(getByLabelText(`${row} ${col}`)).toBeTruthy();
  });

  it("does not invent a trend from a change inside the model's own noise", () => {
    const { getByTestId } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} spot={spot({ weather: w({
        window: { before: hour(58, 1014), at: hour(60, 1014), after: hour(63, 1014) },
      }) })} />);
    expect(getByTestId("spot-trend").textContent)
      .toBe("cloud holding · pressure steady — across the hour either side");
  });

  it("claims no trend at all when one end of the window is missing", () => {
    const { queryByTestId, getByLabelText } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} spot={spot({ weather: w({
        window: { before: null, at: hour(60, 1013), after: hour(85, 1012) },
      }) })} />);
    expect(queryByTestId("spot-trend")).toBeNull();
    expect(getByLabelText("Cloud visit").textContent).toBe("60%");    // the figures still show
  });

  it("says nothing rather than implying clear sky when no window was fetched", () => {
    const { queryByLabelText } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}}
                  spot={spot({ weather: w({ window: null }) })} />);
    expect(queryByLabelText("Cloud visit")).toBeNull();
  });

  it("dates the spot by the visit, not by when the record was typed up", () => {
    const { getByTestId } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} spot={spot({
        createdAt: new Date(2026, 8, 1, 20).getTime(),
        visitedAt: new Date(2026, 7, 30, 9).getTime(),
      })} />);
    expect(getByTestId("spot-when").textContent).toBe("2026-08-30 at 09:00 · 49.0974, -121.9675");
  });
});

describe("<SpotScreen> editing and deleting", () => {
  it("never deletes on the first tap", () => {
    const onDelete = vi.fn();
    const { getByLabelText, getByText } = render(
      <SpotScreen spot={spot()} palette={LIGHT} onBack={() => {}} onDelete={onDelete} />);
    fireEvent.click(getByLabelText("Delete spot"));
    expect(onDelete).not.toHaveBeenCalled();
    expect(getByText("Delete this spot?")).toBeTruthy();
  });

  it("says what is lost, because nothing else holds that day's conditions", () => {
    const { getByLabelText, getByText } = render(
      <SpotScreen spot={spot()} palette={LIGHT} onBack={() => {}} onDelete={() => {}} />);
    fireEvent.click(getByLabelText("Delete spot"));
    expect(getByText("The gauge reading, the weather and the rules that were in force are recorded nowhere " +
                     "else. This cannot be undone.")).toBeTruthy();
  });

  it("lets the confirm be backed out of", () => {
    const onDelete = vi.fn();
    const { getByLabelText, queryByText } = render(
      <SpotScreen spot={spot()} palette={LIGHT} onBack={() => {}} onDelete={onDelete} />);
    fireEvent.click(getByLabelText("Delete spot"));
    fireEvent.click(getByLabelText("Keep it"));
    expect(queryByText("Delete this spot?")).toBeNull();
    expect(onDelete).not.toHaveBeenCalled();
  });

  it("deletes only after the second, explicit tap", () => {
    const onDelete = vi.fn();
    const { getByLabelText } = render(
      <SpotScreen spot={spot()} palette={LIGHT} onBack={() => {}} onDelete={onDelete} />);
    fireEvent.click(getByLabelText("Delete spot"));
    fireEvent.click(getByLabelText("Delete"));
    expect(onDelete).toHaveBeenCalledTimes(1);
  });

  it("is read-only until Edit is pressed", () => {
    const view = render(
      <SpotScreen spot={spot()} palette={LIGHT} onBack={() => {}} onEdit={() => {}} />);
    expect(view.queryByLabelText("Notes")).toBeNull();
    expect(view.queryByLabelText("Save changes")).toBeNull();
    cleanup();

    const editing = render(
      <SpotScreen spot={spot()} palette={LIGHT} mode="edit" title="t" notes="n"
                  onTitle={() => {}} onNotes={() => {}} onSave={() => {}}
                  onDiscard={() => {}} />);
    expect(editing.getByLabelText("Notes")).toBeTruthy();
    expect(editing.getByLabelText("Save changes")).toBeTruthy();
  });

  it("leaves an untouched edit without interrupting anybody", () => {
    const onDiscard = vi.fn();
    const { getByLabelText, queryByText } = render(
      <SpotScreen spot={spot()} palette={LIGHT} mode="edit" title="t" notes="n"
                  dirty={false} onTitle={() => {}} onNotes={() => {}}
                  onSave={() => {}} onDiscard={onDiscard} />);
    fireEvent.click(getByLabelText("Cancel editing"));
    expect(queryByText("You have unsaved changes.")).toBeNull();
    expect(onDiscard).toHaveBeenCalled();
  });

  it("asks before throwing away a real change, and offers to save instead", () => {
    const onDiscard = vi.fn(), onSave = vi.fn();
    const { getByLabelText, getByText } = render(
      <SpotScreen spot={spot()} palette={LIGHT} mode="edit" title="new" notes="n"
                  dirty onTitle={() => {}} onNotes={() => {}}
                  onSave={onSave} onDiscard={onDiscard} />);
    fireEvent.click(getByLabelText("Cancel editing"));
    expect(getByText("You have unsaved changes.")).toBeTruthy();
    expect(onDiscard).not.toHaveBeenCalled();

    // Distinctly labelled from the footer's "Save changes": two buttons with one name is
    // how a person presses the wrong one.
    fireEvent.click(getByLabelText("Save and close"));
    expect(onSave).toHaveBeenCalled();
  });

  it("discards only when that is what was chosen", () => {
    const onDiscard = vi.fn();
    const { getByLabelText } = render(
      <SpotScreen spot={spot()} palette={LIGHT} mode="edit" title="new" notes="n"
                  dirty onTitle={() => {}} onNotes={() => {}}
                  onSave={() => {}} onDiscard={onDiscard} />);
    fireEvent.click(getByLabelText("Cancel editing"));
    fireEvent.click(getByLabelText("Discard"));
    expect(onDiscard).toHaveBeenCalledTimes(1);
  });
});

describe("<SpotScreen> draft mode shows what the saved spot will show", () => {
  // The defect this replaced: the capture flow had its OWN renderer, which showed a
  // temperature and nothing else. People saved a spot and then found more in it than they
  // had been asked to confirm.
  const rich = spot({
    reading: { station: "08MH016", discharge: 41.2, level: 1.4, percentile: 0.62,
               at: "2026-08-30T09:00", backfilled: false },
    weather: { tempC: 14, windKph: 8, windDir: 210, rain3h: 2, pressureHpa: 1013, code: 3,
               window: { before: { at: "08:00", tempC: 13, cloudPct: 35,
                                   pressureHpa: 1014, humidityPct: 71 },
                         at: { at: "09:00", tempC: 14, cloudPct: 60,
                               pressureHpa: 1013, humidityPct: 68 },
                         after: { at: "10:00", tempC: 16, cloudPct: 85,
                                  pressureHpa: 1012, humidityPct: 64 } },
               at: "2026-08-30T09:00", backfilled: false },
  });

  const shown = (mode: "draft" | "view") => {
    const r = render(
      <SpotScreen spot={rich} palette={LIGHT} mode={mode}
                  title="t" notes="n" onTitle={() => {}} onNotes={() => {}}
                  onSave={() => {}} onBack={() => {}} />);
    const grab = (l: string) => r.queryByLabelText(l)?.textContent ?? null;
    const out = {
      cloud: grab("Cloud visit"), pressure: grab("Pressure visit"),
      humidity: grab("Humidity visit"), temp: grab("Temp visit"),
      wind: !!r.queryByText("wind 8 km/h"), rain: !!r.queryByText("rain 2 mm"),
      // `percentileLabel`, shared with the map dot and the sheet — this read "p62"
      // while the map read "p62nd" about the same station.
      pctile: !!r.queryByText("· p62nd against the record"),
      observed: r.queryAllByTestId("spot-observed").map((e) => e.textContent),
    };
    cleanup();
    return out;
  };

  it("renders identically before and after saving", () => {
    expect(shown("draft")).toEqual(shown("view"));
  });

  it("and that shared rendering is not empty", () => {
    const d = shown("draft");
    expect(d.cloud).toBe("60%");
    expect(d.pressure).toBe("1013");
    expect(d.humidity).toBe("68%");
    expect(d.wind && d.rain && d.pctile).toBe(true);
    // Two observation times: one for the gauge reading, one for the weather. Both are
    // separately dated on purpose — they come from different services at different hours.
    expect(d.observed).toEqual(["08MH016 · observed 2026-08-30 09:00", "observed 2026-08-30 09:00"]);
  });
});

describe("<SpotScreen> naming and locating", () => {
  it("falls back to the water it is on rather than showing 'Untitled'", () => {
    const { getByText, queryByText } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}}
                  spot={spot({ title: "", waterName: "Chilliwack River" })} />);
    expect(getByText("Chilliwack River")).toBeTruthy();
    expect(queryByText(/Untitled/)).toBeNull();
  });

  it("falls back to the coordinates when there is not even a water", () => {
    const { getByText } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}}
                  spot={spot({ title: "", waterName: null })} />);
    expect(getByText("49.0974, -121.9675")).toBeTruthy();
  });

  it("offers to add a title, but only when there is not one", () => {
    const untitled = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} onEdit={() => {}}
                  spot={spot({ title: "" })} />);
    expect(untitled.getByLabelText("Add a title")).toBeTruthy();
    cleanup();

    const named = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} onEdit={() => {}} spot={spot()} />);
    expect(named.queryByLabelText("Add a title")).toBeNull();
  });

  it("takes you to the spot on the map", () => {
    const onShowOnMap = vi.fn();
    const { getByLabelText } = render(
      <SpotScreen spot={spot()} palette={LIGHT} onBack={() => {}}
                  onShowOnMap={onShowOnMap} />);
    fireEvent.click(getByLabelText("Show on map"));
    expect(onShowOnMap).toHaveBeenCalled();
  });
});

describe("<SpotScreen> shows the estimate it recorded", () => {
  /*
   * A SAVED SPOT RENDERS THE COMPONENT THE APP RENDERED, on frozen data.
   *
   * It used to be a hand-rolled copy: one station's number, a unit, and a percentile
   * spelled differently from the map dot and the sheet. So a spot recorded something the
   * app had never said — one gauge where the reader had been shown a panel of four.
   */
  const frozen = {
    answer: { ok: true as const,
              value: { percentile: 0.12, plusMinus: 12, trust: "close" as const,
                       spread: 6, donors: 2 } },
    rows: [{ station: "08MH001" as never, role: "up" as const, percentile: 0.12,
             weight: 0.7, areaRatio: 2.4, areaKm2: 120, trust: "close" as const,
             years: 40, regulated: false, sameRiver: true,
             factors: { share: 1, role: 1, record: 1 } }],
    areaKm2: 50, routesReady: true,
  };

  it("renders the estimate as the app rendered it", () => {
    const r = render(<SpotScreen spot={spot({ panel: frozen } as never)}
                                palette={LIGHT} onBack={() => {}} />);
    expect(r.getByText("Low for the time of year")).toBeTruthy();
    expect(r.getByText("08MH001")).toBeTruthy();
  });

  it("dates the estimate's season by the visit, not by the day it is opened", () => {
    // Visited 30 August. Read in October, the frozen percentile is still a percentile for late
    // August; labelling it with today's season would compare that day's river to the wrong weeks.
    const r = render(<SpotScreen spot={spot({ panel: frozen } as never)}
                                palette={LIGHT} onBack={() => {}} />);
    expect(r.getByText(/percentile for/).textContent).toBe("1st–24th percentile for late August · this spot drains 50 km²");
  });

  it("falls back to the bare reading for a spot saved before this existed", () => {
    const r = render(<SpotScreen spot={spot({ panel: null })} palette={LIGHT}
                                onBack={() => {}} />);
    // Still a true record of that day — just less of one.
    expect(r.queryByText("Low for the time of year")).toBeNull();
  });
});

describe("<SpotScreen> never drops a spot silently (P01)", () => {
  it("shows a failed save in full and stays open", () => {
    const { getByRole } = render(
      <SpotScreen spot={spot()} palette={LIGHT} mode="edit" title="x" notes="" onTitle={() => {}} onNotes={() => {}}
                  error="This spot was not saved (disk full). It is still on screen; try again." />);
    expect(getByRole("alert").textContent).toBe("This spot was not saved (disk full). It is still on screen; try again.");
  });

  it("asks before a tab change drops an unsaved edit: keep editing, or discard", () => {
    const keep = vi.fn(), discard = vi.fn();
    const props = { spot: spot(), palette: LIGHT, mode: "edit" as const, title: "x", notes: "",
                    onTitle: () => {}, onNotes: () => {}, dirty: true, askToDiscard: true,
                    onKeepEditing: keep, onDiscard: discard };
    const a = render(<SpotScreen {...props} />);
    fireEvent.click(a.getByText("Keep editing"));
    expect(keep).toHaveBeenCalledTimes(1);
    expect(discard).not.toHaveBeenCalled();
    a.unmount();
    const b = render(<SpotScreen {...props} />);
    fireEvent.click(b.getByText("Discard"));
    expect(discard).toHaveBeenCalledTimes(1);
  });
});

describe("<SpotScreen> the week before (S1)", () => {
  const WEEK = ["2026-08-23", "2026-08-24", "2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28", "2026-08-29"];
  const leadUp = {
    weather: WEEK.map((date, i) => ({ date, maxC: 16 + i, minC: 8, precipMm: i === 1 ? 6 : 0, windMaxKph: 10,
                                      code: i === 1 ? 61 : 1 })),
    station: "08MH001",
    gauge: WEEK.map((date, i) => ({ date, discharge: i === 2 ? null : 50 - i * 5, level: null,
                                    dischargePlace: i === 2 ? null : i === 6 ? "below" as const : 0.5,
                                    levelPlace: null, ...(i === 2 ? { none: true as const } : {}) })),
  };

  it("draws seven days of weather, each said whole, with the week's rain and its source", () => {
    const { getByLabelText, getByTestId } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} spot={spot({ leadUp })} />);
    expect(getByLabelText("Mon Aug 24: rain, high 17°C, low 8°C, 6 mm of rain, wind up to 10 km/h").textContent)
      .toBe("Mon 24Rain17° / 8°6 mm");
    expect(getByTestId("leadup-rain").textContent).toBe("6 mm of rain in the week before, on 1 day.");
    expect(getByTestId("leadup-weather-source").textContent).toBe("Weather from Open-Meteo.com (CC BY 4.0)");
  });

  it("draws the station's daily means with their place, a gap as a dash, and the week's direction", () => {
    const { getByLabelText, getByTestId } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} spot={spot({ leadUp })} />);
    expect(getByLabelText("Sun Aug 23: 50.0 m³/s, the 50th percentile for the day").textContent).toBe("50.0p50th");
    expect(getByLabelText("Tue Aug 25: no daily mean recorded").textContent).toBe("—");
    expect(getByLabelText("Sat Aug 29: 20.0 m³/s, at or below the 10th percentile for the day").textContent)
      .toBe("20.0≤p10");
    expect(getByTestId("leadup-unit").textContent).toBe("m³/s");
    expect(getByTestId("leadup-trend").textContent).toBe("Flow fell over the week: 50.0 → 20.0 m³/s");
    expect(getByTestId("leadup-gauge-meta").textContent).toBe("08MH001 · daily means");
  });

  it("says why a half is missing, and draws no strip for it", () => {
    const { getByTestId, queryByTestId } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}}
                  spot={spot({ leadUp: { weather: null, station: "08MH001", gauge: null } })} />);
    expect(getByTestId("leadup-weather-note").textContent).toBe("The week's weather was not recorded.");
    expect(getByTestId("leadup-gauge-note").textContent)
      .toBe("The station's daily means for the week could not be read.");
    expect(queryByTestId("leadup-trend")).toBeNull();
  });

  it("offers a refresh for a spot saved before the week was recorded, and says why whole", () => {
    const { getByLabelText, getByText, queryByText } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} onRefresh={() => {}} spot={spot({
        weather: { window: null, tempC: 14, windKph: 8, windDir: 210, rain3h: 0, pressureHpa: 1014, code: 3,
                   at: "2026-08-30T09:00", backfilled: false },
      })} />);
    expect(getByLabelText("Refresh this spot")).toBeTruthy();
    expect(getByText("The week before this visit was not recorded — it can be filled in from the days themselves."))
      .toBeTruthy();
    expect(queryByText("The week before")).toBeNull();
  });

  it("offers a refresh for days not available yet, and says under each row which (owner bug 2026-10-22)", () => {
    const later = {
      ...leadUp,
      weather: leadUp.weather.map((d, i) => (i === 6 ? { date: d.date, maxC: null, minC: null, precipMm: null,
                                                         windMaxKph: null, code: null } : d)),
      gauge: leadUp.gauge.map((d, i) => (i >= 5 ? { date: d.date, discharge: null, level: null, dischargePlace: null,
                                                    levelPlace: null } : d)),
    };
    const onRefresh = vi.fn();
    const { getByLabelText, getByTestId, getByText } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} onRefresh={onRefresh} spot={spot({
        weather: { window: null, tempC: 14, windKph: 8, windDir: 210, rain3h: 0, pressureHpa: 1014, code: 3,
                   at: "2026-08-30T09:00", backfilled: false },
        leadUp: later,
      })} />);
    expect(getByText("Some days of the week before were not available yet when this was saved — they can be filled " +
                     "in now.")).toBeTruthy();
    expect(getByTestId("leadup-weather-pending").textContent)
      .toBe("1 day had no weather from Open-Meteo yet. Refresh fills it in.");
    expect(getByTestId("leadup-gauge-pending").textContent)
      .toBe("The station had not published 2 of these days yet. Refresh fills them in.");
    expect(getByLabelText("Sat Aug 29: no daily mean published yet").textContent).toBe("—not yet");
    fireEvent.click(getByLabelText("Refresh this spot"));
    expect(onRefresh).toHaveBeenCalledTimes(1);
  });
});

describe("<SpotScreen> photographs (S1)", () => {
  const store = { uri: async (ref: string) => (ref === "photos/s1-1.jpg" ? "file:///doc/spots/photos/s1-1.jpg" : null) };
  const settle = () => act(async () => { for (let i = 0; i < 4; i++) await Promise.resolve(); });
  const edit = (over: Partial<PhotoEdit> = {}): PhotoEdit => ({
    refs: ["photos/s1-1.jpg", "photos/s1-2.jpg"], sources: ["library", "camera"], busy: false, error: null,
    begin: vi.fn(), add: vi.fn(async () => {}), remove: vi.fn(async () => {}), discard: vi.fn(async () => {}),
    committed: vi.fn(), ...over,
  });

  it("draws a saved spot's photos from their references, and says so when one is gone from the device", async () => {
    const { container, getByText } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} photoStore={store}
                  spot={spot({ photos: ["photos/s1-1.jpg", "photos/s1-9.jpg"] })} />);
    await settle();
    expect([...container.querySelectorAll("img")].map((i) => i.getAttribute("src")))
      .toEqual(["file:///doc/spots/photos/s1-1.jpg"]);
    expect(getByText("This photo is no longer on this device.")).toBeTruthy();
  });

  it("offers no add or remove until the spot is being edited", () => {
    const { queryByLabelText } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} photoStore={store} photoEdit={edit()}
                  spot={spot({ photos: ["photos/s1-1.jpg"] })} />);
    expect(queryByLabelText("Add from photos")).toBeNull();
    expect(queryByLabelText("Remove photo 1")).toBeNull();
  });

  it("adds from the library or the camera, and removes one photo, while editing", () => {
    const pe = edit();
    const { getByLabelText } = render(
      <SpotScreen palette={LIGHT} mode="edit" title="t" notes="" onTitle={() => {}} onNotes={() => {}}
                  onSave={() => {}} onDiscard={() => {}} photoStore={store} photoEdit={pe}
                  spot={spot({ photos: ["photos/s1-1.jpg"] })} />);
    fireEvent.click(getByLabelText("Add from photos"));
    fireEvent.click(getByLabelText("Take a photo"));
    fireEvent.click(getByLabelText("Remove photo 2"));
    expect([(pe.add as ReturnType<typeof vi.fn>).mock.calls, (pe.remove as ReturnType<typeof vi.fn>).mock.calls])
      .toEqual([[["library"], ["camera"]], [["photos/s1-2.jpg"]]]);
  });

  it("offers adding in the draft, before there is any photo", () => {
    const { getByLabelText } = render(
      <SpotScreen palette={LIGHT} mode="draft" title="t" notes="" onTitle={() => {}} onNotes={() => {}}
                  onSave={() => {}} onBack={() => {}} photoEdit={edit({ refs: [], sources: ["library"] })}
                  spot={spot()} />);
    expect(getByLabelText("Add from photos")).toBeTruthy();
  });

  it("says a photo that could not be added, whole", () => {
    const { getByRole } = render(
      <SpotScreen palette={LIGHT} mode="edit" title="t" notes="" onTitle={() => {}} onNotes={() => {}}
                  onSave={() => {}} onDiscard={() => {}}
                  photoEdit={edit({ error: "The photo was not added (camera permission refused). Nothing else changed." })}
                  spot={spot()} />);
    expect(getByRole("alert").textContent)
      .toBe("The photo was not added (camera permission refused). Nothing else changed.");
  });
});
