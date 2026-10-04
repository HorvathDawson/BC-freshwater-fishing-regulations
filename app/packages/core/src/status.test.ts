import { describe, expect, it } from "vitest";
import * as status from "./status";
import { WATER_STATUS, WATER_STATUSES } from "./status";

describe("a water's status — one answer for the map and every list", () => {
  it("names every status once, each with the map token that paints it", () => {
    expect([...WATER_STATUSES].sort()).toEqual(Object.keys(WATER_STATUS).sort());
    expect(WATER_STATUS.closed.token).toBe("color.status.closed");
    expect(WATER_STATUS.own.token).toBe("color.status.own");
    expect(WATER_STATUS.base.token).toBe("color.status.base");
    // Three states, three colours: no two statuses may share a token.
    expect(new Set(WATER_STATUSES.map((s) => WATER_STATUS[s].token)).size).toBe(3);
  });

  it("has ONE definition — the index's; no reading of a water's records decides a status", () => {
    // `waterStatus(regs)` once read own/base from a water's regulation records, dormant behind
    // the not-integrated flag: a second definition of "own" beside the pipeline's (AGENTS 23).
    expect("waterStatus" in status).toBe(false);
    expect(Object.keys(status).sort()).toEqual(
      ["WATER_STATUS", "WATER_STATUSES", "statusOfCode", "statusOn", "waterStatusOn"]);
  });
});
