/**
 * Mounted, not inspected. These render through react-native-web — the mobile-web target —
 * so what is asserted is a real tree, in the renderer a browser uses.
 */
import { describe, expect, it } from "vitest";
import { render, cleanup } from "@testing-library/react";
import { afterEach } from "vitest";
import type { Status } from "@app/core";
import { StatusPill } from "./StatusPill";
import { LIGHT, CVD } from "./theme";

afterEach(cleanup);

const status = (o: Status["outcome"], p: Status["provenance"] = "specific"): Status =>
  ({ outcome: o, provenance: p, from: [] });

describe("StatusPill", () => {
  it("shows the word core produced, for every outcome", () => {
    for (const [outcome, word] of [["closed", "CLOSED"], ["restricted", "RESTRICTED"],
                                   ["unknown", "UNKNOWN"], ["open", "OPEN"]] as const) {
      const { getByText } = render(
        <StatusPill status={status(outcome)} palette={LIGHT} />,
      );
      expect(getByText(word)).toBeTruthy();
      cleanup();
    }
  });

  it("water nobody wrote a rule about says so, rather than looking unregulated", () => {
    const { getByText } = render(
      <StatusPill status={status("open", "general")} palette={LIGHT} />,
    );
    expect(getByText("OPEN · GENERAL RULES")).toBeTruthy();
  });

  it("carries the outcome as an accessible label, not only as colour", () => {
    const { getByLabelText } = render(<StatusPill status={status("closed")} palette={LIGHT} />);
    expect(getByLabelText("CLOSED")).toBeTruthy();
  });

  it("renders the same words under the colour-blind palette", () => {
    const { getByText } = render(<StatusPill status={status("closed")} palette={CVD} />);
    expect(getByText("CLOSED")).toBeTruthy();
  });
});

describe("the flow ramp is one definition", () => {
  it("the legend reads the same colours the map paints", async () => {
    // These were two lists: the legend wrote seven hex values inline while the map's
    // `standing` mode resolved to three shades of one blue. The legend promised red
    // through cyan and the map drew a wash of blue — a legend that disagrees with its map
    // teaches a scale that is not there.
    const { flowRamp } = await import("./theme");
    const { STYLE_META, resolveTheme } = await import("@app/map");
    for (const theme of ["light", "dark", "cvd"]) {
      const v = resolveTheme(theme) as Record<string, string>;
      const mode = STYLE_META.colorModes.stream!.standing! as
        { stops: [number, { token: string }][] };
      // Positive stops only: the -1 sentinel is a state, not a point on the scale.
      expect(flowRamp(theme)).toEqual(
        mode.stops.filter(([at]) => at >= 0).map(([, r]) => v[r.token]));
    }
  });

  it("has a stop for every step, none of them undefined", async () => {
    const { flowRamp } = await import("./theme");
    for (const theme of ["light", "dark", "cvd"]) {
      const ramp = flowRamp(theme);
      expect(ramp).toHaveLength(7);
      for (const c of ramp) expect(c).toMatch(/^#[0-9a-fA-F]{6}$/);
    }
  });

  it("never paints low flow the colour of a closure", async () => {
    // Red means CLOSED on this map. A river that is merely low must not borrow it.
    const { flowRamp } = await import("./theme");
    const { resolveTheme } = await import("@app/map");
    for (const theme of ["light", "dark", "cvd"]) {
      const v = resolveTheme(theme) as Record<string, string>;
      expect(flowRamp(theme)[0]!.toLowerCase())
        .not.toBe(String(v["color.status.closed"]).toLowerCase());
    }
  });
});
