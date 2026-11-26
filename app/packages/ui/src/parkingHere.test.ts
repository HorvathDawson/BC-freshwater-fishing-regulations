/** (T1163 G5 / G6) the parking pin's "Use my location": what it sets and what it says, whole strings (AGENTS 33). */
import { describe, expect, it } from "vitest";
import { environmentError, MY_LOCATION, setFailureSink } from "@app/core";
import type { Locate } from "./mapRail";
import { parkingHere } from "./parkingHere";

const make = (here: Locate["here"]): Locate => ({ permission: async () => "granted", here });

describe("parkingHere", () => {
  it("a good fix sets the pin and says nothing", async () => {
    expect(await parkingHere(make(async () => ({ lat: 49.1, lon: -122.3, accuracy: 30 }))))
      .toEqual({ parking: { lat: 49.1, lon: -122.3 }, notice: null });
  });
  it("a rough fix sets the pin and says how rough", async () => {
    expect(await parkingHere(make(async () => ({ lat: 49.1, lon: -122.3, accuracy: 3240 }))))
      .toEqual({ parking: { lat: 49.1, lon: -122.3 },
                 notice: "Your phone placed you only to about 3.2 km, so this pin may not be where you are." });
  });
  it("a fix outside B.C. sets nothing", async () => {
    expect(await parkingHere(make(async () => ({ lat: 43.65, lon: -79.38, accuracy: 20 }))))
      .toEqual({ parking: null, notice: MY_LOCATION.far });
  });
  it("a refusal sets nothing and says location is off (the line that carries Open Settings)", async () => {
    expect(await parkingHere(make(async () => { throw environmentError(new Error("location permission refused")); })))
      .toEqual({ parking: null, notice: "Location is off for this app. Turn it on in your device settings, then tap again." });
  });
  it("no fix sets nothing and says so, with no Settings line", async () => {
    expect(await parkingHere(make(async () => { throw new Error("timed out"); })))
      .toEqual({ parking: null, notice: "Could not find your location." });
  });
  it("a refusal is not reported (it is the environment's); a defect is", async () => {
    const seen: unknown[] = [];
    const undo = setFailureSink((e) => seen.push(e));
    try {
      await parkingHere(make(async () => { throw environmentError(new Error("location permission refused")); }));
      expect(seen).toEqual([]);
      const defect = new Error("boom");
      expect(await parkingHere(make(async () => { throw defect; })))
        .toEqual({ parking: null, notice: "Could not find your location." });
      expect(seen).toEqual([defect]);
    } finally { undo(); }
  });
});
