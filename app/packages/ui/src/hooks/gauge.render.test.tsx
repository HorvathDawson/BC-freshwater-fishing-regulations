/** (T360) THE CONDITIONS DOTS follow the Gauges switch: on, the roster; off, none, even with the same question asked. */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import type { WaterSource } from "@app/data";
import { useGaugeGeoJSON } from "./gauge";

afterEach(cleanup);

const source = { gaugePoints: async () => [{ station: "08MH001", lat: 49.1, lon: -122.0 }] } as unknown as WaterSource;
const feed = { index: async () => ({ stations: { "08MH001": { percentile: 40, parameter: "discharge" } } }) as never };

function Probe({ on, out }: { on: boolean; out: { v?: string | null } }) {
  out.v = useGaugeGeoJSON(source, feed, on);
  return null;
}

describe("useGaugeGeoJSON", () => {
  it("switched off after it has drawn: no dots (was the last roster, still drawn)", async () => {
    const out: { v?: string | null } = {};
    const r = render(<Probe on out={out} />);
    await act(async () => { await new Promise((x) => setTimeout(x, 0)); });
    expect(out.v).not.toBeNull();
    r.rerender(<Probe on={false} out={out} />);
    expect(out.v).toBeNull();
    r.rerender(<Probe on out={out} />);
    expect(out.v).not.toBeNull();
  });
});
