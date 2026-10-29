/** (T173) The fish obstacle icons as pixels, and the style naming exactly the images that exist. */
import { describe, expect, it } from "vitest";
import { OBSTACLE_ICON_KINDS } from "@app/core";
import { generatedImages, isGeneratedImage, nativeImages } from "./images";
import { obstacleIconImage, obstacleImageId } from "./obstacle-icons";
import { rgb } from "./raster";
import { colour } from "./chrome";
import { MAP_STYLE } from "./style";

const INK = colour("light", "color.obstacle.mark"), PAPER = colour("light", "color.obstacle.ring");

const px = (img: ReturnType<typeof obstacleIconImage>, x: number, y: number) => {
  const k = (y * img.width + x) * 4;
  return [...img.data.slice(k, k + 4)];
};

describe("obstacle icons", () => {
  it("48 px at 3x (16 css px): ink in the middle of a stroke, paper in the halo, clear beyond it", () => {
    const img = obstacleIconImage("other", INK, PAPER);
    expect([img.width, img.height, img.pixelRatio]).toEqual([48, 48, 3]);
    expect(px(img, 24, 24)).toEqual([...rgb(INK, [0, 0, 0]), 255]);            // the dot's centre: ink
    expect(px(img, 24, 24 + 3 * 4.1 | 0)[0]).toBeGreaterThan(200); // 4.1 units out: the halo (3.4 dot + 1.4 halo)
    expect(px(img, 24, 24 + 3 * 6.5 | 0)[3]).toBe(0);              // clear
    expect(px(img, 0, 0)[3]).toBe(0);
  });

  it("every kind draws something, and the six types are not the same picture", () => {
    const sums = OBSTACLE_ICON_KINDS.map((k) => {
      const d = obstacleIconImage(k, INK, PAPER).data;
      let n = 0; for (let i = 3; i < d.length; i += 4) n += d[i]! > 0 ? 1 : 0;
      return [n, [...d.slice(0, 4000)].join()] as const;
    });
    for (const [n] of sums) expect(n).toBeGreaterThan(60);
    expect(new Set(sums.map(([, h]) => h)).size).toBe(OBSTACLE_ICON_KINDS.length);
  });

  it("each theme adds them, the phone gets them under per-theme names, and the style names only images that exist", () => {
    for (const theme of ["light", "dark", "cvd"]) {
      const ids = generatedImages(theme).map((g) => g.id);
      for (const k of OBSTACLE_ICON_KINDS) {
        expect(ids).toContain(obstacleImageId(k));
        expect(Object.keys(nativeImages(theme))).toContain(`${obstacleImageId(k)}.${theme}`);
      }
    }
    const layer = MAP_STYLE.layers.find((l) => l.id === "obstacle") as unknown as { type: string; layout: Record<string, unknown>; minzoom: number };
    expect([layer.type, layer.minzoom, layer.layout["icon-allow-overlap"]]).toEqual(["symbol", 10, false]);
    const names = JSON.stringify(layer.layout["icon-image"]).match(/obstacle-[a-z]+/g)!;
    expect(new Set(names)).toEqual(new Set(OBSTACLE_ICON_KINDS.map(obstacleImageId)));
    for (const n of names) expect(isGeneratedImage(n)).toBe(true);
    expect(MAP_STYLE.layers.some((l) => l.id === "obstacle_ring")).toBe(false);
  });
});
