/** (T563) A percentile alert's lines are worked out again from the record the app holds when it opens on a release. */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, renderHook, waitFor } from "@testing-library/react";
import { percentileByDay, thresholdAt, type Band, type GaugeAlertRule } from "@app/core";
import { createNotifySettings, NOTIFY_DEFAULT, type NotifyState, type WaterSource } from "@app/data";
import { useFreshAlertLines, type NotifyControls } from "./notify";

afterEach(cleanup);
const year = (add: number): Band[] => Array.from({ length: 366 }, (_, i) => [i + 1 + add, i + 1 + add, i + 1 + add, i + 1 + add, i + 1 + add] as Band);
const rule = (id: string, pct: number, add: number): GaugeAlertRule => ({
  id, station: "08MH001", stationName: "Vedder", quantity: "discharge", direction: "above", on: true, water: null, createdAt: 1,
  threshold: { kind: "percentile", pct, byDay: percentileByDay(year(add), pct) },
});
const DEC31 = "2026-12-31T12:00:00-08:00";

function controls(rules: GaugeAlertRule[]) {
  let text: string | null = JSON.stringify({ ...NOTIFY_DEFAULT, on: true, rules } satisfies NotifyState);
  const settings = createNotifySettings({ read: () => text, write: (t) => { text = t; } });
  return { settings, platform: {} } as unknown as NotifyControls;
}
const source = (record: Band[] | null) => ({
  gaugeSeries: async () => ({ value: { parameter: "discharge", yearBand: record }, fetchedAt: null }),
}) as unknown as WaterSource;

describe("alert lines follow the record in hand", () => {
  it("a percentile alert saved on an older record is worked out again from the newer one", async () => {
    const c = controls([rule("a", 75, 0)]);
    renderHook(() => useFreshAlertLines(source(year(10)), c));
    await waitFor(() => expect(thresholdAt(c.settings.read().rules[0]!.threshold, DEC31)).toBe(375));
    expect(c.settings.read().rules[0]!.threshold).toMatchObject({ kind: "percentile", pct: 75 });
  });

  it("a record that cannot be read leaves the old line", async () => {
    const c = controls([rule("a", 75, 0)]);
    renderHook(() => useFreshAlertLines(source(null), c));
    await new Promise((r) => setTimeout(r, 30));
    expect(thresholdAt(c.settings.read().rules[0]!.threshold, DEC31)).toBe(365);
  });
});
