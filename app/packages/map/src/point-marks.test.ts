import { describe, expect, it } from "vitest";
import { colour } from "./chrome";
import { labelAnchors, markBoxImage, markHit, marksData, marksLayers, pinsData, titledPin } from "./point-marks";

describe("T317 point marks", () => {
  it("the label box is a square card with an ink edge, only its middle stretching", () => {
    const card = colour("light", "color.ui.card"), ink = colour("light", "color.ui.ink");
    const rgba = (h: string) => [1, 3, 5].map((i) => parseInt(h.slice(i, i + 2), 16)).concat(255);
    const g = markBoxImage(card, ink);
    const px = (x: number, y: number) => Array.from(g.data.slice((y * g.width + x) * 4, (y * g.width + x) * 4 + 4));
    expect([px(0, 0), px(12, 12)]).toEqual([rgba(ink), rgba(card)]);
    expect([g.stretchX, g.content]).toEqual([[[3, 21]], [3, 3, 21, 21]]);
  });
  it("a tap's mark is the first feature's `i` (a phone may send it as a string); none is null", () => {
    expect([markHit([{ properties: { i: 2 } }]), markHit([{ properties: { i: "1" } }]), markHit([{}])]).toEqual([2, 1, null]);
  });
  it("(T929) every label's point IS its mark's cut point, wherever the camera is; only its anchor changes", async () => {
    const marks = [{ lon: -121.9, lat: 49.1, label: "Mid", a11y: "", on: false },
                   { lon: -121.8, lat: 49.0, label: "Right edge", a11y: "", on: false },
                   { lon: -121.7, lat: 49.2, label: "Off screen", a11y: "", on: false },
                   { lon: -121.6, lat: 49.3, label: null, a11y: "", on: true }];
    const xs: Record<number, number> = { [-121.9]: 200, [-121.8]: 380, [-121.7]: -600, [-121.6]: 10 };
    const anchors = await labelAnchors(marks, 400, (lon) => [xs[lon]!, 50]);
    expect(anchors).toEqual(["bottom", "bottom-right", "bottom-left", null]);
    type F = { geometry: { coordinates: number[] }; properties: { i: number; kind: string; anchor?: string } };
    const labels = (marksData(marks, anchors).features as F[]).filter((f) => f.properties.kind === "label");
    // the anchor test: each label at its own mark, exactly — an off-screen mark's label stays off screen with it
    expect(labels.map((f) => [f.geometry.coordinates, f.properties.anchor]))
      .toEqual([[[-121.9, 49.1], "bottom"], [[-121.8, 49.0], "bottom-right"], [[-121.7, 49.2], "bottom-left"]]);
    // a camera somewhere else changes no label's point (the tester's "they move around with you as you pan")
    const panned = await labelAnchors(marks, 400, (lon) => [xs[lon]! - 350, 50]);
    expect((marksData(marks, panned).features as F[]).filter((f) => f.properties.kind === "label")
      .map((f) => f.geometry.coordinates)).toEqual(labels.map((f) => f.geometry.coordinates));
    // not measured yet: centred, at the mark
    expect((marksData(marks).features as F[]).filter((f) => f.properties.kind === "label").map((f) => f.properties.anchor))
      .toEqual(["bottom", "bottom", "bottom"]);
  });
  it("(T929) the label layer hangs each label by its feature's anchor; nothing else moves it sideways", () => {
    const label = marksLayers({ ink: "#000000", card: "#ffffff" }, "box")[1] as { layout: Record<string, unknown> };
    expect(label.layout["text-anchor"]).toEqual(["get", "anchor"]);
    expect((label.layout["text-offset"] as number[])[0]).toBe(0);
  });
  it("pins carry their tone (the accent by default) and the marker is last, over them", () => {
    const accent = colour("light", "color.ui.accent");
    const fc = pinsData({ lat: 1, lon: 2 }, [{ lat: 3, lon: 4 }], accent);
    expect(fc.features.map((f) => (f as { properties: { kind: string; tone: string } }).properties))
      .toEqual([expect.objectContaining({ kind: "pin", tone: accent }), expect.objectContaining({ kind: "marker" })]);
  });
});

describe("(T621, G19) the words of a titled pin", () => {
  const pins = [{ lat: 1, lon: 2 }, { lat: 3, lon: 4, title: "Mouth" }];
  it("is the pin its hit names, whether `i` is a number (web) or a string (phone); none without words", () => {
    expect(titledPin([{ properties: { title: "Mouth", i: 1 } }], pins)?.title).toBe("Mouth");
    expect(titledPin([{ properties: { title: "Mouth", i: "1" } }], pins)?.title).toBe("Mouth");
    expect([titledPin([{ properties: { title: "", i: 0 } }], pins), titledPin([{ properties: { i: 0 } }], pins),
            titledPin([], pins), titledPin([{ properties: { title: "x", i: 9 } }], pins)]).toEqual([null, null, null, null]);
  });
});
