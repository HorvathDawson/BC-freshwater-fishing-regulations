/**
 * THE LAYER REGISTRY (owner 2026-10-21 late, layers menu direction B): availability is derived, one water colouring
 * for every feature, coming-soon entries listed greyed, the always-shown line names Indigenous land, and what the map is
 * handed falls back while the choice is kept. Whole-object asserts; the derivation is mutation-pinned (a fact that is
 * ignored makes a case here fail — memory "checks seeded from input launder").
 */
import { describe, expect, it } from "vitest";
import { LAYER_DEFAULTS, LAYERS, availability, chooseLayer, chooseShow, layersVM, restoreLayers, saveLayers, waterLegend,
         type LayerFacts } from "./layers";

const NONE: LayerFacts = { carries: { stocked: false, surveyed: false }, online: true };
const FED: LayerFacts = { carries: { stocked: true, surveyed: true }, online: true };
const OFFLINE: LayerFacts = { ...NONE, online: false };
const tiles = (ts: readonly { id: string; chosen: boolean; enabled: boolean; tag: string | null }[]) =>
  ts.map((t) => `${t.id}${t.chosen ? " *" : ""}${t.enabled ? "" : " (off)"}${t.tag ? ` [${t.tag}]` : ""}`);

describe("the registry", () => {
  it("lists every entry once, each set with exactly one default", () => {
    expect(LAYERS.map((e) => `${e.kind}:${e.id}`)).toEqual([
      "show:show.flow", "show:show.level", "show:show.temperature",
      "ground:ground.map", "ground:ground.satellite", "ground:ground.corridors",
      "water:water.regulations", "water:water.stocked", "water:water.depth",
      "overlay:overlay.spots", "overlay:overlay.contours", "overlay:overlay.private", "overlay:overlay.obstacles",
      "overlay:overlay.pools", "overlay:overlay.relief",
      "overlay:overlay.gauges", "overlay:overlay.arrows",
      "always:always.protected", "always:always.indigenous", "always:always.noaccess", "always:always.admin",
    ]);
    expect(LAYERS.filter((e) => e.kind === "ground" && e.default).map((e) => e.id)).toEqual(["ground.map"]);
    expect(LAYERS.filter((e) => e.kind === "water" && e.default).map((e) => e.id)).toEqual(["water.regulations"]);
    expect(LAYERS.filter((e) => e.kind === "show" && e.default).map((e) => e.id)).toEqual(["show.flow"]);
    expect(new Set(LAYERS.map((e) => e.id)).size).toBe(LAYERS.length);
  });

  it("an always-shown entry sets nothing (it is never a choice)", () => {
    expect(LAYERS.filter((e) => e.kind === "always" && e.sets)).toEqual([]);
  });

  it("the defaults: Flow, the drawn map, regulations, the Map tab's switches off, the Conditions map's gauges and arrows on", () => {
    expect(LAYER_DEFAULTS).toEqual({ show: "show.flow", ground: "ground.map", water: "water.regulations",
      on: { "overlay.spots": true, "overlay.contours": false, "overlay.private": false, "overlay.obstacles": false, "overlay.pools": false, "overlay.relief": false,
            "overlay.gauges": true, "overlay.arrows": true } });
  });

  it("which view each entry belongs to: Show and the gauges and arrows on Conditions only; the water colouring, "
     + "private land and contours on the Map tab only; the ground on both", () => {
    const of = (v: "map" | "conditions") => LAYERS.filter((e) => e.views.includes(v)).map((e) => e.id);
    expect([of("conditions"), of("map")]).toEqual([
      ["show.flow", "show.level", "show.temperature", "ground.map", "ground.satellite",
       "overlay.spots", "overlay.gauges", "overlay.arrows", "always.protected", "always.indigenous", "always.noaccess"],
      ["ground.map", "ground.satellite", "ground.corridors", "water.regulations", "water.stocked", "water.depth",
       "overlay.spots", "overlay.contours", "overlay.private", "overlay.obstacles", "overlay.pools", "overlay.relief",
       "always.protected", "always.indigenous", "always.noaccess", "always.admin"],
    ]);
  });
});

describe("availability: built -> release -> network", () => {
  const e = (id: string) => LAYERS.find((x) => x.id === id)!;
  it("each branch, with its reason", () => {
    expect([
      availability(e("ground.corridors"), FED),
      availability(e("water.stocked"), NONE),
      availability(e("water.stocked"), FED),
      availability(e("water.depth"), { ...NONE, carries: { stocked: true, surveyed: false } }),
      availability(e("ground.satellite"), OFFLINE),
      availability(e("ground.satellite"), NONE),
      availability(e("ground.map"), OFFLINE),
    ]).toEqual([
      { state: "coming-soon", reason: "Coming soon." },
      { state: "no-data", reason: "No data yet: the release carries no values for it. It turns on by itself when one does." },
      { state: "ok" },
      { state: "no-data", reason: "No data yet: the release carries no values for it. It turns on by itself when one does." },
      { state: "online-only", reason:
          "Satellite needs a connection. Showing the map until you are back online; satellite returns by itself." },
      { state: "ok" },
      { state: "ok" },
    ]);
  });

  it("MUTATION PIN: each needs is read from its OWN fact (stocked from stocked, depth from surveyed)", () => {
    const only = (k: "stocked" | "surveyed") => ({ ...NONE, carries: { stocked: k === "stocked", surveyed: k === "surveyed" } });
    expect([availability(e("water.stocked"), only("stocked")).state, availability(e("water.stocked"), only("surveyed")).state,
            availability(e("water.depth"), only("surveyed")).state, availability(e("water.depth"), only("stocked")).state])
      .toEqual(["ok", "no-data", "ok", "no-data"]);
  });
});

describe("layersVM: the sheet, whole", () => {
  it("the default sheet on the Map tab, with no lake values in the release", () => {
    const vm = layersVM("map", LAYER_DEFAULTS, NONE);
    expect({ ...vm, ground: { ...vm.ground, tiles: tiles(vm.ground.tiles) },
             water: { ...vm.water, tiles: tiles(vm.water.tiles) }, overlays: { ...vm.overlays, rows: tiles(vm.overlays.rows) } })
      .toEqual({
        title: "Layers",
        show: { title: "Show", tiles: [] },
        ground: { title: "Map", note: null,
                  tiles: ["ground.map *", "ground.satellite", "ground.corridors (off) [Coming soon]"] },
        water: { title: "Colour the water by", drawn: "regulations",
                 note: "Stocked and Depth have no data yet. They turn on by themselves when a release carries their values.",
                 tiles: ["water.regulations *", "water.stocked (off) [No data yet]", "water.depth (off) [No data yet]"] },
        overlays: { title: "On the map",
                    rows: ["overlay.spots *", "overlay.contours", "overlay.private", "overlay.obstacles",
                           "overlay.pools (off) [Coming soon]",
                           "overlay.relief (off) [Coming soon]"] },
        always: "Always shown: parks and ecological reserves, Indigenous land (reserves), no-access and permit-only land, "
          + "and region and management-unit lines. They are never switched off.",
        changed: 0,
        effective: { basemap: "map", water: "regulations", show: "discharge",
                     groups: { depth: false, ownership: false, obstacles: false }, layers: { gauges: false, stream__arrows: false }, pins: { spots: true } },
      });
  });

  it("every tile carries its words", () => {
    const vm = layersVM("map", LAYER_DEFAULTS, NONE);
    expect([...vm.ground.tiles, ...vm.water.tiles, ...vm.overlays.rows].map((t) => `${t.label} · ${t.note}`)).toEqual([
      "Map · Drawn · works offline", "Satellite · Esri imagery · online only", "Corridors · Detail imagery along a stream",
      "Regulations · Open, closed or its own rules, on the day", "Stocked · When a lake was last stocked",
      "Depth · Which lakes were surveyed",
      "Spots · Your saved spots",
      "Depth contours · Traced lines on surveyed lakes", "Private land · Title, not access. Off by default",
      "Fish obstacles · Falls, cascades, culverts and dams. From zoom 10",
      "Pools and runs · Slow and fast water, read from lidar", "Relief · Lidar hillshade",
    ]);
  });

  it("a stale Stocked choice with no values: Regulations drawn and shown chosen, the badge counts nothing", () => {
    const vm = layersVM("map", { ...LAYER_DEFAULTS, water: "water.stocked" }, NONE);
    expect([vm.water.drawn, tiles(vm.water.tiles), vm.changed, vm.effective.water])
      .toEqual(["regulations", ["water.regulations *", "water.stocked (off) [No data yet]",
                                "water.depth (off) [No data yet]"], 0, "regulations"]);
  });

  it("a release with values: Depth draws, turns the contours on with it, and counts as one change", () => {
    const vm = layersVM("map", { ...LAYER_DEFAULTS, water: "water.depth" }, FED);
    expect([vm.water.drawn, vm.water.note, tiles(vm.water.tiles), vm.effective, vm.changed]).toEqual([
      "depth", null, ["water.regulations", "water.stocked", "water.depth *"],
      { basemap: "map", water: "depth", show: "discharge", groups: { depth: true, ownership: false, obstacles: false },
        layers: { gauges: false, stream__arrows: false }, pins: { spots: true } }, 1]);
  });

  it("satellite offline: Map tagged SHOWING, Satellite kept chosen but greyed and tagged, the reason said", () => {
    const vm = layersVM("map", { ...LAYER_DEFAULTS, ground: "ground.satellite" }, OFFLINE);
    expect([tiles(vm.ground.tiles), vm.ground.note, vm.effective.basemap, vm.changed]).toEqual([
      ["ground.map [Showing]", "ground.satellite * (off) [Needs a connection]", "ground.corridors (off) [Coming soon]"],
      "Satellite needs a connection. Showing the map until you are back online; satellite returns by itself.",
      "satellite", 1]);
  });

  it("switches: private land and contours set their style groups, and each counts", () => {
    const vm = layersVM("map", { ...LAYER_DEFAULTS, on: { ...LAYER_DEFAULTS.on, "overlay.private": true,
                                                          "overlay.contours": true } }, NONE);
    expect([vm.effective.groups, vm.changed]).toEqual([{ depth: true, ownership: true, obstacles: false }, 2]);
  });

  it("(T173) fish obstacles: off by default, on sets its style group and counts one change, on the Map tab only", () => {
    const c = chooseLayer(LAYER_DEFAULTS, "overlay.obstacles", NONE);
    expect([c.on["overlay.obstacles"], layersVM("map", c, NONE).effective.groups, layersVM("map", c, NONE).changed,
            layersVM("conditions", c, NONE).effective.groups.obstacles, layersVM("conditions", c, NONE).changed])
      .toEqual([true, { depth: false, ownership: false, obstacles: true }, 1, false, 0]);
  });

  it("a coming-soon switch somehow on draws nothing and counts nothing", () => {
    const vm = layersVM("map", { ...LAYER_DEFAULTS, on: { ...LAYER_DEFAULTS.on, "overlay.pools": true } }, NONE);
    expect([tiles(vm.overlays.rows)[4], vm.changed]).toEqual(["overlay.pools (off) [Coming soon]", 0]);
  });

  it("on Conditions, whole: Show (Flow · Level · Temperature), the ground, the gauges and arrows; no water colouring", () => {
    const vm = layersVM("conditions", LAYER_DEFAULTS, NONE);
    expect({ title: vm.title, show: { ...vm.show, tiles: tiles(vm.show.tiles) }, ground: tiles(vm.ground.tiles),
             water: vm.water.tiles, overlays: { ...vm.overlays, rows: tiles(vm.overlays.rows) }, always: vm.always,
             changed: vm.changed, effective: vm.effective }).toEqual({
      title: "Layers",
      show: { title: "Show", tiles: ["show.flow *", "show.level", "show.temperature"] },
      ground: ["ground.map *", "ground.satellite"],
      water: [],
      overlays: { title: "On the map", rows: ["overlay.spots *", "overlay.gauges *", "overlay.arrows *"] },
      always: "Always shown: parks and ecological reserves, Indigenous land (reserves), and no-access and permit-only land. "
        + "They are never switched off.",
      changed: 0,
      // a Map-tab switch is never drawn here: what the menu does not list, the map does not draw
      effective: { basemap: "map", water: "regulations", show: "discharge", groups: { depth: false, ownership: false, obstacles: false },
                   layers: { gauges: true, stream__arrows: true }, pins: { spots: true } },
    });
  });

  it("Show's words", () => {
    expect(layersVM("conditions", LAYER_DEFAULTS, NONE).show.tiles.map((t) => `${t.label} · ${t.note}`)).toEqual([
      "Flow · How much water, against its record for the date",
      "Level · How high the water stands, from level gauges only",
      "Temperature · At the gauges that measure it; the rivers stay plain",
    ]);
  });

  it("Level: one quantity, chosen once — the Conditions map is handed level and counts one change", () => {
    const level = chooseLayer(LAYER_DEFAULTS, "show.level", NONE);
    const vm = layersVM("conditions", level, NONE);
    expect([level.show, tiles(vm.show.tiles), vm.effective.show, vm.changed])
      .toEqual(["show.level", ["show.flow", "show.level *", "show.temperature"], "level", 1]);
  });

  it("THE BADGE IS PER VIEW: each map counts the choices it lists (the ground is shared by both)", () => {
    const c = { ...LAYER_DEFAULTS, show: "show.level", ground: "ground.satellite",
                on: { ...LAYER_DEFAULTS.on, "overlay.private": true, "overlay.gauges": false } };
    expect([layersVM("conditions", c, NONE).changed, layersVM("map", c, NONE).changed]).toEqual([3, 2]);
  });

  it("MUTATION PIN: a Map-tab switch never reaches the Conditions map, and a Conditions switch never the Map tab", () => {
    const c = { ...LAYER_DEFAULTS, on: { ...LAYER_DEFAULTS.on, "overlay.private": true, "overlay.arrows": false } };
    expect([layersVM("conditions", c, NONE).effective.groups, layersVM("conditions", c, NONE).effective.layers,
            layersVM("map", c, NONE).effective.groups, layersVM("map", c, NONE).effective.layers]).toEqual([
      { depth: false, ownership: false, obstacles: false }, { gauges: true, stream__arrows: false },
      { depth: false, ownership: true, obstacles: false }, { gauges: false, stream__arrows: false }]);
  });

  it("chooseShow: the panel's own chart quantity sets the same one choice", () => {
    expect([chooseShow(LAYER_DEFAULTS, "level").show, chooseShow(LAYER_DEFAULTS, "temperature").show,
            chooseShow(chooseShow(LAYER_DEFAULTS, "level"), "discharge").show])
      .toEqual(["show.level", "show.temperature", "show.flow"]);
  });
});

describe("the waiting line", () => {
  it("one colouring or several, each a whole sentence", () => {
    expect([layersVM("map", LAYER_DEFAULTS, { ...NONE, carries: { stocked: true, surveyed: false } }).water.note,
            layersVM("map", LAYER_DEFAULTS, FED).water.note]).toEqual([
      "Depth has no data yet. It turns on by itself when a release carries its values.", null]);
  });
});

describe("chooseLayer", () => {
  it("a tile chooses; a switch flips; an unavailable entry is never chosen", () => {
    expect([
      chooseLayer(LAYER_DEFAULTS, "ground.satellite", NONE).ground,
      chooseLayer(LAYER_DEFAULTS, "water.stocked", NONE).water,
      chooseLayer(LAYER_DEFAULTS, "water.stocked", FED).water,
      chooseLayer(LAYER_DEFAULTS, "overlay.private", NONE).on["overlay.private"],
      chooseLayer(chooseLayer(LAYER_DEFAULTS, "overlay.private", NONE), "overlay.private", NONE).on["overlay.private"],
      chooseLayer(LAYER_DEFAULTS, "overlay.pools", FED).on["overlay.pools"],
      chooseLayer(LAYER_DEFAULTS, "always.indigenous", FED) === LAYER_DEFAULTS,
      chooseLayer(LAYER_DEFAULTS, "show.temperature", NONE).show,
      chooseLayer(LAYER_DEFAULTS, "overlay.gauges", NONE).on["overlay.gauges"],
      chooseLayer(LAYER_DEFAULTS, "overlay.spots", NONE).on["overlay.spots"],
    ]).toEqual(["ground.satellite", "water.regulations", "water.stocked", true, false, false, true, "show.temperature",
                false, false]);
  });
});

describe("SPOTS (owner 2026-10-22: \"toggle our spots on and off in the layer menu\")", () => {
  it("one switch, listed on every map, on by default; off hands every map no spot pins and counts as one change", () => {
    const off = chooseLayer(LAYER_DEFAULTS, "overlay.spots", NONE);
    const row = (v: "map" | "conditions" | "water", c = LAYER_DEFAULTS) =>
      layersVM(v, c, NONE).overlays.rows.filter((r) => r.id === "overlay.spots").map((r) => `${r.label} · ${r.note}${r.chosen ? " *" : ""}`);
    expect([row("map"), row("conditions"), row("water"), row("map", off), row("conditions", off),
            layersVM("map", LAYER_DEFAULTS, NONE).effective.pins, layersVM("conditions", LAYER_DEFAULTS, NONE).effective.pins,
            layersVM("water", LAYER_DEFAULTS, NONE).effective.pins,
            layersVM("map", off, NONE).effective.pins, layersVM("conditions", off, NONE).effective.pins,
            layersVM("water", off, NONE).effective.pins,
            layersVM("map", off, NONE).changed, layersVM("conditions", off, NONE).changed]).toEqual([
      ["Spots · Your saved spots *"], ["Spots · Your saved spots *"], ["Spots · Your saved spots *"],
      ["Spots · Your saved spots"], ["Spots · Your saved spots"],
      { spots: true }, { spots: true }, { spots: true }, { spots: false }, { spots: false }, { spots: false }, 1, 1]);
  });
});

describe("the choice, kept (owner 2026-10-22: a switch stays as it was left)", () => {
  it("round-trips whole through its text", () => {
    const c = chooseLayer(chooseLayer(chooseLayer(LAYER_DEFAULTS, "overlay.spots", NONE), "ground.satellite", NONE),
                          "show.level", NONE);
    expect(restoreLayers(saveLayers(c))).toEqual({ show: "show.level", ground: "ground.satellite",
      water: "water.regulations", on: { ...LAYER_DEFAULTS.on, "overlay.spots": false } });
  });

  it("nothing saved, unreadable text, or ids the registry no longer has: the defaults, kept where they still mean "
     + "something", () => {
    expect([
      restoreLayers(null),
      restoreLayers("{not json"),
      restoreLayers("[1,2]"),
      restoreLayers(JSON.stringify({ show: "show.gone", ground: "always.admin", water: 7,
                                     on: { "overlay.spots": false, "overlay.gone": true, "ground.map": false,
                                           "overlay.private": "yes" } })),
    ]).toEqual([LAYER_DEFAULTS, LAYER_DEFAULTS, LAYER_DEFAULTS,
                { ...LAYER_DEFAULTS, on: { ...LAYER_DEFAULTS.on, "overlay.spots": false } }]);
  });
});

describe("waterLegend: the key to the colouring drawn", () => {
  const rows = (w: "regulations" | "stocked" | "depth") =>
    waterLegend(w).map((r) => `${Object.values(r.swatch).join(":")} = ${r.label}`);
  it("each colouring, whole; a lake measure ends with what draws neutral and why", () => {
    expect([rows("regulations"), rows("stocked"), rows("depth")]).toEqual([
      // (T54) the fourth: access, not regulation — in the regulations key only
      ["status:closed = Closed", "status:own = Has its own regulations", "status:base = Base regulations only",
       "status:no_access = No public access"],
      ["stock:season = Stocked this season", "stock:year = Stocked this year", "stock:recent = Stocked 1–3 years ago",
       "stock:decade = Stocked 3–10 years ago", "stock:old = Stocked over 10 years ago",
       "neutral = Not stocked, or no record. Streams and wetlands draw plain: stocking is recorded for lakes only."],
      ["survey:digitised = Contours traced", "survey:sheet = Surveyed, sheet only",
       "neutral = Not surveyed. Streams and wetlands draw plain: depth surveys cover lakes only."],
    ]);
  });
});
