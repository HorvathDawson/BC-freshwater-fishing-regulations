/**
 * A saved spot, mounted.
 *
 * The whole risk on this screen is TIME. Every figure was true once and is displayed
 * later, so the tests below all ask the same question in different ways: can a reader
 * mistake a record for a reading? A stale discharge shown bare is worse than no discharge,
 * because the reader will wade on it.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render } from "@testing-library/react";
import { SpotScreen } from "./SpotScreen";
import type { Spot } from "@app/data/spots";
import { LIGHT } from "./theme";

afterEach(cleanup);

const spot = (over: Partial<Spot> = {}): Spot => ({
  id: "s1", createdAt: Date.parse("2026-08-30T09:00:00Z"), visitedAt: Date.parse("2026-08-30T09:00:00Z"), updatedAt: 0,
  lat: 49.0974, lon: -121.9675, item: null, section: null, waterName: null,
  title: "Tamihi run", notes: "", photos: [],
  reading: null, weather: null, trace: null, panel: null, ...over,
});

describe("<SpotScreen>", () => {
  it("dates every spot, because a spot is a record and not a reading", () => {
    const { getByText } = render(
      <SpotScreen spot={spot()} palette={LIGHT} onBack={() => {}} />);
    expect(getByText(/2026-08-30/)).toBeTruthy();
  });

  it("says WHY there is no discharge instead of showing a dash", () => {
    // A gauge that does not speak for this water is not a missing value — it is an answer.
    const { getByText, queryByText } = render(
      <SpotScreen spot={spot()} palette={LIGHT} onBack={() => {}} />);
    expect(getByText(/No gauge is entitled to speak/)).toBeTruthy();
    expect(queryByText("m³/s")).toBeNull();
  });

  it("marks a value that was filled in after the fact", () => {
    const { getByText } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} spot={spot({
        reading: { station: "08MH016", discharge: 41.2, level: 1.42, percentile: 0.62,
                   at: "2026-08-30T09:00", backfilled: true },
      })} />);
    expect(getByText(/filled in afterwards/)).toBeTruthy();
    expect(getByText("41.2")).toBeTruthy();
  });

  it("does not claim a backfill when the reading was live", () => {
    const { queryByText } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} spot={spot({
        reading: { station: "08MH016", discharge: 41.2, level: null, percentile: null,
                   at: "2026-08-30T09:00", backfilled: false },
      })} />);
    expect(queryByText(/filled in afterwards/)).toBeNull();
  });

  it("offers a refresh only when something is actually stale", () => {
    const fresh = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} onRefresh={() => {}} spot={spot({
        weather: { window: null, tempC: 14, windKph: 8, windDir: 210, rain3h: 0, pressureHpa: 1014,
                   code: 3, at: "2026-08-30T09:00", backfilled: false },
        reading: { station: "08MH016", discharge: 41.2, level: null, percentile: null,
                   at: "2026-08-30T09:00", backfilled: false },
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
    const { getByText, getByLabelText } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} spot={spot({ weather: w() })} />);
    expect(getByLabelText("Cloud −1 h")).toBeTruthy();
    expect(getByText(/clouding over/)).toBeTruthy();
  });

  it("calls it clearing when the sky opened up", () => {
    const { getByText } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} spot={spot({ weather: w({
        window: { before: hour(90, 1014), at: hour(60, 1014), after: hour(20, 1014) },
      }) })} />);
    expect(getByText(/clearing/)).toBeTruthy();
  });

  it("reports the barometer's direction, which decides as much as the sky", () => {
    const { getByText } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} spot={spot({ weather: w({
        window: { before: hour(60, 1016), at: hour(60, 1014), after: hour(60, 1011) },
      }) })} />);
    expect(getByText(/pressure falling/)).toBeTruthy();
  });

  it("shows humidity and pressure at each hour, not only at the visit", () => {
    const { getByLabelText } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} spot={spot({ weather: w() })} />);
    for (const col of ["−1 h", "visit", "+1 h"])
      for (const row of ["Pressure", "Humidity", "Cloud", "Temp"])
        expect(getByLabelText(`${row} ${col}`)).toBeTruthy();
  });

  it("does not invent a trend from a change inside the model's own noise", () => {
    const { getByText, queryByText } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} spot={spot({ weather: w({
        window: { before: hour(58, 1014), at: hour(60, 1014), after: hour(63, 1014) },
      }) })} />);
    expect(queryByText(/clouding over|clearing/)).toBeNull();
    expect(getByText(/cloud holding/)).toBeTruthy();
  });

  it("claims no trend at all when one end of the window is missing", () => {
    const { queryByText, getByLabelText } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} spot={spot({ weather: w({
        window: { before: null, at: hour(60, 1013), after: hour(85, 1012) },
      }) })} />);
    expect(queryByText(/clouding over|clearing|holding|rising|falling/)).toBeNull();
    expect(getByLabelText("Cloud visit")).toBeTruthy();    // the figures still show
  });

  it("says nothing rather than implying clear sky when no window was fetched", () => {
    const { queryByLabelText } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}}
                  spot={spot({ weather: w({ window: null }) })} />);
    expect(queryByLabelText("Cloud visit")).toBeNull();
  });

  it("dates the spot by the visit, not by when the record was typed up", () => {
    const { getByText } = render(
      <SpotScreen palette={LIGHT} onBack={() => {}} spot={spot({
        createdAt: Date.parse("2026-09-01T20:00:00Z"),
        visitedAt: Date.parse("2026-08-30T09:00:00Z"),
      })} />);
    expect(getByText(/2026-08-30/)).toBeTruthy();
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
    expect(getByText(/recorded nowhere else/)).toBeTruthy();
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
      wind: !!r.queryByText(/wind 8 km\/h/), rain: !!r.queryByText(/rain 2 mm/),
      // `percentileLabel`, shared with the map dot and the sheet — this read "p62"
      // while the map read "p62nd" about the same station.
      pctile: !!r.queryByText(/p62nd against the record/),
      observed: r.queryAllByText(/observed 2026-08-30/).length,
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
    expect(d.observed).toBe(2);
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
    expect(r.getByText(/Low for the time of year/)).toBeTruthy();
    expect(r.getByText("08MH001")).toBeTruthy();
  });

  it("falls back to the bare reading for a spot saved before this existed", () => {
    const r = render(<SpotScreen spot={spot({ panel: null })} palette={LIGHT}
                                onBack={() => {}} />);
    // Still a true record of that day — just less of one.
    expect(r.queryByText(/for the time of year/)).toBeNull();
  });
});
