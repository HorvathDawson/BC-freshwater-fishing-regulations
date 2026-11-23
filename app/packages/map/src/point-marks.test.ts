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
    // (T997) three labels at one screen height: the last hangs below its ring (`hangBelow`), the others above
    expect(anchors).toEqual(["bottom", "bottom-right", "top-right", null]);
    type F = { geometry: { coordinates: number[] }; properties: { i: number; kind: string; anchor?: string } };
    const labels = (marksData(marks, anchors).features as F[]).filter((f) => f.properties.kind === "label");
    // the anchor test: each label at its own mark, exactly — an off-screen mark's label stays off screen with it
    expect(labels.map((f) => [f.geometry.coordinates, f.properties.anchor]))
      .toEqual([[[-121.9, 49.1], "bottom"], [[-121.8, 49.0], "bottom-right"], [[-121.7, 49.2], "top-right"]]);
    // a camera somewhere else changes no label's point (the tester's "they move around with you as you pan")
    const panned = await labelAnchors(marks, 400, (lon) => [xs[lon]! - 350, 50]);
    expect((marksData(marks, panned).features as F[]).filter((f) => f.properties.kind === "label")
      .map((f) => f.geometry.coordinates)).toEqual(labels.map((f) => f.geometry.coordinates));
    // not measured yet: centred, at the mark; (T997) the southernmost (49.0) below its ring, north being up
    expect((marksData(marks).features as F[]).filter((f) => f.properties.kind === "label").map((f) => f.properties.anchor))
      .toEqual(["bottom", "top", "bottom"]);
  });
  it("(T929) the label layer hangs each label by its feature's anchor; nothing moves it sideways; (T997) below or above", () => {
    const label = marksLayers({ ink: colour("light", "color.ui.ink"), card: colour("light", "color.ui.card") }, "box")[1] as { layout: Record<string, unknown> };
    expect(label.layout["text-anchor"]).toEqual(["get", "anchor"]);
    const k = (16 + 3 + 1.5) / 12;
    expect(label.layout["text-offset"]).toEqual(["case", ["==", ["get", "below"], true], ["literal", [0, k]], ["literal", [0, -k]]]);
    // never collided away: a hidden end label would leave that end named by nothing (#51 review)
    expect([label.layout["text-allow-overlap"], label.layout["icon-allow-overlap"]]).toEqual([true, true]);
    // (#37) but placed: a road or place name under the words gives way to them
    expect([label.layout["text-ignore-placement"], label.layout["icon-ignore-placement"]]).toEqual([false, false]);
  });
  it("(T997 round 2, T465) the lit part's two end labels never cover each other, wherever the two rings are on the screen", async () => {
    // a label box as the layer draws it: 16 px from its ring, above (`bottom*`) or below (`top*`); as tall as two
    // wrapped lines and 180 px wide, centred or from its corner as `labelAnchor` hangs it
    const W = 180, H = 2 * 15 + 2 * (3 + 1.5), LIFT = 16;
    const box = ([x, y]: [number, number], a: string) => {
      const left = a.endsWith("-left") ? x : a.endsWith("-right") ? x - W : x - W / 2;
      const top = a.startsWith("top") ? y + LIFT : y - LIFT - H;
      return { left, right: left + W, top, bottom: top + H };
    };
    const meet = (p: ReturnType<typeof box>, q: ReturnType<typeof box>) =>
      p.left < q.right && q.left < p.right && p.top < q.bottom && q.top < p.bottom;
    const mk = (lon: number) => ({ lon, lat: 49, label: "End", a11y: "", on: false });
    let met = 0, tried = 0;
    // every pair of ring positions on a 390 x 300 map, 15 px apart, the two coincident included (a 23 m part zoomed out)
    for (let x1 = 0; x1 <= 390; x1 += 30) for (let y1 = 0; y1 <= 300; y1 += 30)
      for (let dx = -120; dx <= 120; dx += 15) for (let dy = -90; dy <= 90; dy += 15) {
        const at: Record<number, [number, number]> = { 1: [x1, y1], 2: [x1 + dx, y1 + dy] };
        const a = await labelAnchors([mk(1), mk(2)], 390, (lon) => at[lon]!);
        tried++;
        if (meet(box(at[1]!, a[0]!), box(at[2]!, a[1]!))) met++;
      }
    expect([tried > 10_000, met]).toEqual([true, 0]);
    // and before the map has measured them: by latitude, the southern one below — the coincident pair too
    const F = (fc: { features: unknown[] }) => (fc.features as { properties: { kind: string; anchor?: string } }[])
      .filter((f) => f.properties.kind === "label").map((f) => f.properties.anchor);
    expect([F(marksData([{ ...mk(1), lat: 49.1 }, mk(2)])), F(marksData([mk(1), mk(1)])), F(marksData([mk(1)]))])
      .toEqual([["bottom", "top"], ["bottom", "top"], ["bottom"]]);
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
