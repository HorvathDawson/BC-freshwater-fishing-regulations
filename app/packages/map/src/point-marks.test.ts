import { describe, expect, it } from "vitest";
import { colour } from "./chrome";
import { markBoxImage, markHit, marksData, pinsData, shiftedLabels, titledPin } from "./point-marks";

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
  it("a label is moved only where it would hang over an edge; an unlabelled mark has no label point", async () => {
    const marks = [{ lon: 0, lat: 0, label: "Mid", a11y: "", on: false }, { lon: 1, lat: 0, label: "Edge", a11y: "", on: false },
                   { lon: 2, lat: 0, label: null, a11y: "", on: true }];
    const at = await shiftedLabels(marks, 400, (lon) => [lon === 0 ? 200 : 380, 50], (x, y) => [x, y]);
    expect(at).toEqual([[0, 0], [300, 50], null]);
    expect(marksData(marks, at).features.length).toBe(5);
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
