/**
 * (T672, T462 audit W3) THE BAIT BAN AND FLY ONLY HUBS LIST A WATER BY ITS YEAR. Over models built by hand (which day's
 * sentences say what), then over the regs fixture release: whatever day the site is built on, the two hubs list the same
 * waters — every water whose year strip holds the state on some day.
 */
import { describe, expect, it } from "vitest";
import { openRegsStore } from "@app/data/regs";
import { openRegsFixture } from "@app/data/release/node";
import { buildAll } from "./build";
import { buildDayOf, type PartModel, type WaterModel } from "./model";
import { topicLines, YEAR_TOPICS } from "./topics";

const part = (o: Partial<PartModel>): PartModel => ({ year: null, marks: [], partIx: 0, label: "x", status: "open", today: "", sections: [], ...o });
const water = (...parts: PartModel[]): WaterModel =>
  ({ ord: 0, id: "gnis:1", name: "Eaton Creek", kind: "stream", regions: [2], region: 2, focus: "2", status: "open", answer: "", parts,
     neighbours: [], mid: null, book: [] });
const gear = (text: string) => [{ title: "Gear", items: [{ text, book: [] }] }];

describe("topicLines: the year-read topics", () => {
  it("a water whose strip is fly only on some day is on the fly-only hub on a day its sentences do not say so", () => {
    const m = water(part({ marks: [{ bit: 0, label: "open" }, { bit: 4, label: "fly fishing only" }], sections: gear("Hooks: Single barbless hook") }));
    expect([...topicLines(m)]).toEqual([["fly-only", ["fly fishing only"]]]);
  });
  it("the day's sentences alone put no water on either hub (they are another day's truth, or a part the strip does not hold)", () => {
    const m = water(part({ marks: [{ bit: 0, label: "open" }], sections: gear("Fly fishing only · bait ban") }));
    expect(topicLines(m).has("fly-only") || topicLines(m).has("bait-ban")).toBe(false);
  });
  it("artificial flies only is fly only; a bait ban is its own hub; any part of the water counts; each label once", () => {
    const m = water(part({ marks: [{ bit: 16, label: "bait ban" }] }),
                    part({ partIx: 1, marks: [{ bit: 8, label: "artificial flies only" }, { bit: 16, label: "bait ban" }] }));
    expect([...topicLines(m)]).toEqual([["bait-ban", ["bait ban"]], ["fly-only", ["artificial flies only"]]]);
  });
  it("the other topics still read the page's sentences", () => {
    const m = water(part({ sections: [{ title: "What you can keep", items: [{ text: "Release every steelhead.", book: [] }] }] }));
    expect([...topicLines(m).keys()]).toEqual(["steelhead"]);
  });
});

describe("on the regs fixture release", () => {
  it("the strip bits the hubs read are the release's own fly_only / artificial_fly_only / bait_ban", async () => {
    const strip = (await (await openRegsStore(openRegsFixture(), { tiles: null })).statics()).strip;
    const bit = (state: string) => strip.find((s) => s.state === state)!.bit;
    expect(YEAR_TOPICS).toEqual({ "fly-only": [bit("fly_only"), bit("artificial_fly_only")], "bait-ban": [bit("bait_ban")] });
  });

  it("the two hubs list the same waters whatever day the site is built on, and each is on the hub by its year strip", async () => {
    const hubs = async (iso: string) => {
      const b = await buildAll({ loader: openRegsFixture(), on: buildDayOf(new Date(`${iso}T12:00:00`)), bare: true, gauges: null });
      const listed = new Map<string, string[]>();
      for (const p of b.build.pages) if (/^\/region\/\d+\/(fly-only|bait-ban)$/.test(p.path))
        listed.set(p.path, [...p.html.matchAll(/<li><a href="(\/water\/[^"]+)">/g)].map((x) => x[1]!));
      const year = new Map<string, string[]>();
      for (const m of b.build.models) for (const [topic, bits] of Object.entries(YEAR_TOPICS))
        if (m.parts.some((p) => p.marks.some((k) => bits!.includes(k.bit))))
          for (const r of m.regions) year.set(`/region/${r}/${topic}`, [...(year.get(`/region/${r}/${topic}`) ?? []), `/water/${b.build.slugMap.waters[m.id]!.slug}`]);
      return { listed, year };
    };
    const sorted = (m: Map<string, string[]>) => [...m].map(([k, v]) => [k, [...v].sort()] as const).sort((a, b) => a[0].localeCompare(b[0]));
    const winter = await hubs("2026-01-15"), summer = await hubs("2026-07-15"), fall = await hubs("2026-11-11");
    expect(sorted(winter.listed)).toEqual(sorted(winter.year));
    expect(sorted(summer.listed)).toEqual(sorted(winter.listed));
    expect(sorted(fall.listed)).toEqual(sorted(winter.listed));
    expect(winter.listed.size, "the fixture has such hubs").toBeGreaterThan(0);
  }, 240_000);
});
