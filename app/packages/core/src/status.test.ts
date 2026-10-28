import { describe, expect, it } from "vitest";
import * as status from "./status";
import { WATER_STATUS, WATER_STATUSES } from "./status";

describe("a water's status — one answer for the map and every list", () => {
  it("names every status once, each with the map token that paints it", () => {
    expect([...WATER_STATUSES].sort()).toEqual(Object.keys(WATER_STATUS).sort());
    expect(WATER_STATUS.closed.token).toBe("color.status.closed");
    expect(WATER_STATUS.own.token).toBe("color.status.own");
    expect(WATER_STATUS.base.token).toBe("color.status.base");
    // (T54) the fourth: an access answer, not a regulation one
    expect(WATER_STATUS.no_access).toEqual({ label: "No public access", token: "color.status.no_access" });
    // Four states, four colours: no two statuses may share a token.
    expect(new Set(WATER_STATUSES.map((s) => WATER_STATUS[s].token)).size).toBe(4);
  });

  it("has ONE definition — the index's; no reading of a water's records decides a status", () => {
    // `waterStatus(regs)` once read own/base from a water's regulation records, dormant behind
    // the not-integrated flag: a second definition of "own" beside the pipeline's (AGENTS 23).
    expect("waterStatus" in status).toBe(false);
    expect(Object.keys(status).sort()).toEqual(
      ["NO_ACCESS_PROFILE", "UNLISTED_PROFILE", "WATER_STATUS", "WATER_STATUSES", "WATER_STATUS_LINE", "dayProfiles",
       "profileLabel", "statusOfCode", "statusOn", "waterStatusOn"]);
  });
});
