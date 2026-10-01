import { describe, expect, it } from "vitest";
import { REGULATIONS, regulationsFor } from "./regulations";
import { WATER_STATUS, WATER_STATUSES, waterStatus } from "./status";

describe("a water's status — one answer for the map and every list", () => {
  it("names every status once, each with the map token that paints it", () => {
    expect([...WATER_STATUSES].sort()).toEqual(Object.keys(WATER_STATUS).sort());
    expect(WATER_STATUS.closed.token).toBe("color.status.closed");
    expect(WATER_STATUS.own.token).toBe("color.status.own");
    expect(WATER_STATUS.base.token).toBe("color.status.base");
    // Three states, three colours: no two statuses may share a token.
    expect(new Set(WATER_STATUSES.map((s) => WATER_STATUS[s].token)).size).toBe(3);
  });

  it("is NOT ASKED while regulations are not integrated — never a guessed 'base'", () => {
    expect(REGULATIONS).toBe("not-integrated");
    expect(regulationsFor("gnis:8634")).toBeNull();
    expect(waterStatus(regulationsFor("gnis:8634"))).toBeNull();
    // Even a record in hand decides nothing until the integration says the data is real.
    expect(waterStatus({ item: "gnis:1", rules: [], licensing: [] })).toBeNull();
  });
});
