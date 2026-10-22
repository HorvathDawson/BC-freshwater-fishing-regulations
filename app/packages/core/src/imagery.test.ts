/**
 * SATELLITE IS ONLINE-ONLY (owner 2026-10-21, decisions.md): Esri World Imagery shows while the device can reach it,
 * and the drawn map shows otherwise, with the reader's choice kept. The reachability rule and the words are pure, here.
 */
import { describe, expect, it } from "vitest";
import { basemapInUse, IMAGERY_CREDIT, IMAGERY_START, imageryProbeUrl, imageryReach, imageryUp,
         isImageryOutage, SATELLITE_OFFLINE, withImageryCredit, type ImageryEvent } from "./imagery";

const run = (events: ImageryEvent[], start = IMAGERY_START) => events.reduce(imageryReach, start);

describe("imagery reachability", () => {
  it("starts up: a device that has not said it is offline is assumed online, and nothing has failed", () => {
    expect(IMAGERY_START).toEqual({ online: true, failed: false });
    expect(imageryUp(IMAGERY_START)).toBe(true);
  });

  it("offline is down; online again is up by itself", () => {
    expect([run([{ type: "offline" }]), run([{ type: "offline" }, { type: "online" }])].map(imageryUp))
      .toEqual([false, true]);
  });

  it("a failed tile or probe is down even while the device says it is online; a probe that answers brings it back", () => {
    expect([run([{ type: "failed" }]), run([{ type: "failed" }, { type: "reached" }]),
            run([{ type: "failed" }, { type: "online" }])].map(imageryUp))
      .toEqual([false, true, true]);
  });

  it("a reached probe does not override the device saying it is offline", () => {
    expect(imageryUp(run([{ type: "offline" }, { type: "reached" }]))).toBe(false);
  });

  it("a 404 is a hole in the imagery (blankTile=false), not an outage; anything else is", () => {
    expect([404, 0, undefined, 401, 403, 500, 503].map(isImageryOutage))
      .toEqual([false, true, true, true, true, true, true]);
  });
});

describe("the basemap actually drawn", () => {
  it("satellite only while it is reachable; the drawn map otherwise; the drawn map whatever the reach", () => {
    expect([basemapInUse("satellite", true), basemapInUse("satellite", false),
            basemapInUse("map", true), basemapInUse("map", false)])
      .toEqual(["satellite", "map", "map", "map"]);
  });

  it("the Layers sheet's reason line, whole", () => {
    expect(SATELLITE_OFFLINE).toBe(
      "Satellite needs a connection. Showing the map until you are back online; satellite returns by itself.");
  });
});

describe("the imagery credit", () => {
  it("Esri's required credit: Powered by Esri and the service's copyrightText, whole", () => {
    expect(IMAGERY_CREDIT).toBe(
      "Satellite imagery: Powered by Esri · Source: Esri, Vantor, Earthstar Geographics, and the GIS User Community");
  });

  it("is listed while satellite is drawn, and not otherwise", () => {
    const base = ["Basemap © OpenStreetMap contributors · © Protomaps", "Weather from Open-Meteo.com (CC BY 4.0)"];
    expect(withImageryCredit(base, "satellite")).toEqual([...base, IMAGERY_CREDIT]);
    expect(withImageryCredit(base, "map")).toEqual(base);
  });
});

describe("the probe", () => {
  it("asks for the one world tile at z0 of the template, keeping its query", () => {
    expect(imageryProbeUrl(
      "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}?blankTile=false"))
      .toBe("https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/0/0/0?blankTile=false");
  });
});
