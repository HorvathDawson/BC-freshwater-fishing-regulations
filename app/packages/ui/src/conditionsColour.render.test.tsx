/**
 * (T348, owner 2026-11-02: "we got the new river forecast in but the colors didn't update and still showed blank") A NEW
 * FEED REPAINTS IN PLACE. The update service takes a changed index (`feed.offer`) and bumps the sync `epoch`
 * (`useSync`, `onFeedChange`); the Conditions colouring re-asks the feed on that epoch and hands the map a new values
 * table — no restart, no wait for the next poll. Mounted, because the bug is a hook that never re-asks.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { buildStandingClasses, type PanelMember, type StationId, type WaterSource } from "@app/data";
import { useConditionsColour, type ConditionsColourVM } from "./conditionsColour";
import type { PanelFeed } from "./panel";

afterEach(cleanup);

const st = (s: string) => s as StationId;
const member = (station: string, areaKm2: number): PanelMember =>
  ({ station: st(station), role: "up", areaKm2, years: { discharge: 40, level: 40 }, regulated: false, sameRiver: true });
const CLASSES = buildStandingClasses("r-test", [{ sid: 5, panel: 1, areaKm2: 120 }],
  new Map([[1, [member("A", 100)]]]), new Map(), new Map());
const source = { standingClasses: async () => CLASSES } as unknown as WaterSource;

function Probe({ feed, epoch, horizon, seen }: { feed: PanelFeed; epoch: number; horizon: 0 | 1;
                                                 seen: (v: ConditionsColourVM) => void }) {
  seen(useConditionsColour({ source, feed, paired: true, onConditions: true, quantity: "discharge", horizon,
                             visible: [], epoch }));
  return null;
}

describe("T348: a new feed reaches the map without a restart", () => {
  it("the forecast arrives (a new index, the epoch bumped): the forecast day's sections go from blank to coloured", async () => {
    // the index before the forecast: A reports now, no forecast day
    let idx: unknown = { stations: { A: { percentile: 0.3, discharge: 0.3, parameter: "discharge" } } };
    const feed = { index: async () => idx } as unknown as PanelFeed;
    let last: ConditionsColourVM | null = null;
    const seen = (v: ConditionsColourVM) => { last = v; };
    const value = () => last?.standing?.values?.section(0);
    const r = render(<Probe feed={feed} epoch={0} horizon={1} seen={seen} />);
    await waitFor(() => expect(last?.standing?.values).toBeTruthy());
    const before = value()!;
    expect(before >= 0).toBe(false);                                  // +1 day: nothing to say yet — no colour
    // the update service took the new index (the forecast's +1 day in it) and said so
    idx = { stations: { A: { percentile: 0.3, discharge: 0.3, parameter: "discharge", ahead: { 1: { discharge: 0.8 } } } } };
    await act(async () => { r.rerender(<Probe feed={feed} epoch={1} horizon={1} seen={seen} />); });
    await waitFor(() => expect(value()! >= 0).toBe(true));          // coloured, in place
  });
});
