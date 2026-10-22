/** The map rail's words, pinned whole (owner 2026-10-21): a changed word fails here with the whole table in view. */
import { describe, expect, it } from "vitest";
import { MAP_RAIL, SPOT_CAPTURE_AT, SPOT_DROP, SPOT_PICK, SPOTS_MENU } from "./mapRail";

describe("the map rail's words", () => {
  it("the rail, the menu and the drop sheet say these, and nothing else", () => {
    const { showLabel, ...menu } = SPOTS_MENU;
    const { where, ...drop } = SPOT_DROP;
    expect({ MAP_RAIL, menu, drop, SPOT_PICK, SPOT_CAPTURE_AT }).toEqual({
      MAP_RAIL: { label: "Map tools", layers: "Layers", layersLabel: "Choose layers" },
      menu: {
        title: "Spots",
        addMine: "At my location", addMineAsk: "Asks for your location first",
        mineOff: "Location access is turned off for this app, so At my location is not offered.",
        mineNone: "Your location is not available here, so At my location is not offered.",
        addPick: "Pick on the map", addPickDetail: "Go to the map and tap the place",
        noLocation: "Your location could not be read, so no spot was started.",
        show: "Show",
        emptyTitle: "No spots yet",
        emptyBody: "Pin an exact point on a stream and the app records the gauge, its standing against the record " +
                   "and the weather that day. Notes and photographs go on top. Press and hold the map to drop one.",
        refresh: "Refresh", refreshing: "Refreshing…", refreshLabel: "Refresh spots",
      },
      drop: { title: "New spot", detail: "Next you pick the water it is on, then when you were there.",
              go: "Continue", cancel: "Cancel" },
      SPOT_PICK: { banner: "Tap where your spot is", cancel: "Cancel" },
      SPOT_CAPTURE_AT: { title: "Pick the water", detail: "Tap the stream or lake your spot is on, near the pin." },
    });
    expect([showLabel("Tamihi run"), where(49.09744, -121.96752)])
      .toEqual(["Show Tamihi run on the map", "49.0974, -121.9675"]);
  });
});
