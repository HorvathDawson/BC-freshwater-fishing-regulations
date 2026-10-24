/**
 * T20 — the fish manifest, emitted to core (captions/credits, no assets) and to the mobile
 * app (the literal `require()` map Metro resolves statically).
 *
 * Every assertion is on a WHOLE value: a full caption, a full credit, the full key list, the
 * full set of files. The golden cross-check proves the core manifest against the
 * prototype's OUTPUT (golden.json), not against the manifest it was emitted from.
 */
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { emit, fishTips, CORE_OUT, MOBILE_OUT, MANIFEST, TIPS_SOURCE } from "./emit-fish.mjs";
import { FISH, FISH_CREDIT_OF, FISH_CREDITS, FISH_MANIFEST_SHA256, FISH_TIPS } from "../packages/core/src/regs/fish.generated";

const app = fileURLToPath(new URL("..", import.meta.url));
const assets = `${app}apps/mobile/assets/fish/`;
const bytes = readFileSync(MANIFEST);
const manifest = JSON.parse(bytes.toString("utf8"));
const sha = createHash("sha256").update(bytes).digest("hex");
const tips = fishTips(readFileSync(TIPS_SOURCE, "utf8"));

describe("emit-fish: the core manifest", () => {
  it("holds every coded species, sorted, salmon included", () => {
    expect(Object.keys(FISH)).toEqual([
      "BB", "BCB", "CH", "CM", "CO", "CT", "CULTUS_LAKE_SCULPIN", "DV", "EB", "GB", "GE",
      "GR", "GREEN_STURGEON", "IN", "KO", "LMB", "LT", "LW", "MW", "NOOKSACK_DACE", "NP",
      "PK", "RB", "SALISH_SUCKER", "SK", "SMB", "ST", "WP", "WSG", "YP",
    ]);
    expect(Object.keys(FISH)).not.toContain("null");
  });

  it("keeps manifest order within a code: the first image is the one a row shows", () => {
    expect(FISH.DV.map((i) => i.caption)).toEqual(["Bull trout", "Dolly Varden"]);
    expect(FISH.CT.map((i) => i.caption)).toEqual(["Coastal cutthroat trout", "Westslope cutthroat trout"]);
    expect(FISH.GR[0].credit).toBe("The Fishes of Alaska (1907), A. Hoen & Co.");
  });

  it("shows the four recoded salmon (bb47bde5), ocean phase first", () => {
    const salmon = ["CM", "CO", "PK", "SK"].map((c) =>
      FISH[c].map((i) => `${c}: ${i.caption} (${i.phase}) — ${i.credit}, ${i.file}`));
    expect(salmon).toEqual([
      ["CM: Chum salmon (ocean) — WDFW, chum-salmon-ocean-phase.webp",
       "CM: Chum salmon (spawning) — WDFW, chum-salmon-spawning-phase.webp"],
      ["CO: Coho salmon (ocean) — WDFW, coho-salmon-ocean-phase.webp",
       "CO: Coho salmon (spawning) — WDFW, coho-salmon-spawning-phase.webp"],
      ["PK: Pink salmon (ocean) — WDFW, pink-salmon-ocean-phase.webp",
       "PK: Pink salmon (spawning) — WDFW, pink-salmon-spawning-phase.webp"],
      ["SK: Sockeye salmon (ocean) — WDFW, sockeye-salmon-ocean-phase.webp",
       "SK: Sockeye salmon (spawning) — WDFW, sockeye-salmon-spawning-phase.webp"],
    ]);
  });

  it("carries a stand-in's real species and the whole image record", () => {
    expect(FISH.NOOKSACK_DACE).toEqual([{
      file: "longnose-dace-nooksack-dace-stand-in.webp", caption: "Longnose dace",
      credit: "Jordan & Evermann (1900)", type: "B&W engraving", source: "Commons",
      phase: null, standInFor: "Nooksack dace", width: 800, height: 400,
    }]);
  });

  it("stamps the manifest's sha256", () => {
    expect(FISH_MANIFEST_SHA256).toBe(sha);
  });
});

describe("emit-fish: the credit lines (T43 FISH_CREDITS / FISH_CREDIT_OF)", () => {
  it("one line per OUTSIDE picture, keyed by file — every one credited, none invented; the app's own drawings are not credits", () => {
    const outside = manifest.images.filter((i: { source: string }) => i.source !== "this app");
    expect(Object.keys(FISH_CREDIT_OF).sort()).toEqual(outside.map((i: { file: string }) => i.file).sort());
    expect(manifest.images.length - outside.length).toBe(2);   // T38's adipose drawings
    expect(["adipose-wild.webp", "adipose-clipped.webp"].map((f) => f in FISH_CREDIT_OF)).toEqual([false, false]);
    expect(Object.keys(FISH_CREDIT_OF).length).toBe(39);
  });
  it("words a line \"<caption>[ · <phase>] — <credit> (<source>)\" (the pictured fish, a stand-in by its own caption)", () => {
    expect([FISH_CREDIT_OF["rainbow-trout.webp"], FISH_CREDIT_OF["steelhead-ocean-phase.webp"],
      FISH_CREDIT_OF["steelhead-spawning-phase.webp"], FISH_CREDIT_OF["longnose-dace-nooksack-dace-stand-in.webp"]]).toEqual([
      "Rainbow trout — Freshwater Fisheries Society of BC (GoFishBC)",
      "Steelhead · ocean — Freshwater Fisheries Society of BC (GoFishBC)",
      "Steelhead · spawning — WDFW (eRegulations WA)",
      "Longnose dace — Jordan & Evermann (1900) (Commons)"]);
  });
  it("FISH_CREDITS is every line, sorted and unique", () => {
    const all = [...new Set(Object.values(FISH_CREDIT_OF))].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    expect(FISH_CREDITS).toEqual(all);
    expect(FISH_CREDITS.length).toBe(39);
    expect(FISH_CREDITS.slice(0, 3)).toEqual(["Arctic grayling — The Fishes of Alaska (1907), A. Hoen & Co. (Commons)",
      "Black crappie — Duane Raver / USFWS (fws.gov)", "Brook trout — Freshwater Fisheries Society of BC (GoFishBC)"]);
  });
  it("de-duplicates two pictures that credit alike (mutation: a copied image adds no line)", () => {
    const twin = { ...manifest, images: [...manifest.images, { ...manifest.images[0], file: "twin.webp" }] };
    const { core } = emit(twin, sha, tips);
    const block = core.slice(core.indexOf("export const FISH_CREDITS"));
    expect(block.match(/"Arctic grayling — /g)?.length).toBe(1);
    expect(core).toContain('  "twin.webp": "Arctic grayling — The Fishes of Alaska (1907), A. Hoen & Co. (Commons)",\n');
  });
});

describe("emit-fish: the look-for tips (T36, contracts §3 FISH_TIPS)", () => {
  it("are page v36's FISHART.SPECIES[code].tips, every species it draws, sorted", () => {
    expect(Object.keys(FISH_TIPS)).toEqual(["BB", "BCB", "CT", "DV", "EB", "GB", "GE", "GR", "IN", "KO", "LMB", "LT", "LW", "MW", "NP",
      "RB", "SMB", "ST", "WP", "WSG", "YP"]);
    expect(FISH_TIPS.RB).toEqual(["Pink or red band along the side", "Small black spots, mostly above the middle of the side",
      "Rows of black spots on the tail"]);
    // page v36's own words (not app/fixtures/fishart.js, which an older copy says "…bright silver, 50 cm or longer")
    expect(FISH_TIPS.ST).toEqual(["A rainbow trout that went to sea: bright silver", "Faint spots, mostly on the back and tail",
      "Hatchery steelhead are missing this small fin (the adipose fin)"]);
  });
  it("read the source's SPECIES table, not a copy: an edited tip changes the output", () => {
    const src = readFileSync(TIPS_SOURCE, "utf8").replace("'Pink or red band along the side'", "'Mutated band'");
    expect(fishTips(src).RB![0]).toBe("Mutated band");
  });
  it("every golden sp: sheet's \"What to look for\" list equals FISH_TIPS[code]", () => {
    const g = JSON.parse(readFileSync(`${app}fixtures/golden/golden.json`, "utf8")) as { sheets: Record<string, { lines: string[] }> };
    const sp = Object.entries(g.sheets).filter(([k]) => k.startsWith("sp:"));
    expect(sp.length).toBe(19);
    const bad = sp.filter(([k, v]) => { const i = v.lines.indexOf("What to look for");
      return JSON.stringify(i < 0 ? [] : v.lines.slice(i + 1)) !== JSON.stringify(FISH_TIPS[k.slice(3)] ?? []); }).map(([k]) => k);
    expect(bad).toEqual([]);
  });
});

describe("emit-fish: the mobile require map", () => {
  const { mobile } = emit(manifest, sha, tips);
  const required = [...mobile.matchAll(/require\("\.\.\/assets\/fish\/([^"]+)"\)/g)].map((m) => m[1]);

  it("requires every coded .webp in the folder exactly once, each one present (and T38's two hatch drawings)", () => {
    const onDisk = readdirSync(assets).filter((f) => f.endsWith(".webp"));
    const coded = manifest.images.filter((i: { code: string | null }) => i.code !== null)
      .map((i: { file: string }) => i.file);
    for (const f of required) expect(onDisk).toContain(f);
    expect([...required].sort()).toEqual([...coded, "adipose-wild.webp", "adipose-clipped.webp"].sort());
    expect(new Set(required).size).toBe(required.length);
  });

  it("emits literal paths in manifest order, one line per code", () => {
    expect(mobile).toContain(
      '  "DV": [require("../assets/fish/bull-trout.webp"), require("../assets/fish/dolly-varden.webp")],\n');
    expect(mobile).toContain('  "GR": [require("../assets/fish/arctic-grayling.webp")],\n');
    expect(mobile).not.toContain("`");
  });
});

describe("emit-fish: the committed files are current (--check)", () => {
  it("equal emit() byte for byte", () => {
    const { core, mobile } = emit(manifest, sha, tips);
    expect(readFileSync(CORE_OUT, "utf8")).toBe(core);
    expect(readFileSync(MOBILE_OUT, "utf8")).toBe(mobile);
  });

  it("--check exits 0 on the committed tree", () => {
    const out = execFileSync("node", [`${app}tools/emit-fish.mjs`, "--check"], { encoding: "utf8" });
    expect(out).toBe("✓ fish.generated.ts current\n");
  });
});

describe("emit-fish: golden cross-check", () => {
  type Pic = { code?: string | null; caption?: string };
  const golden = JSON.parse(readFileSync(`${app}fixtures/golden/golden.json`, "utf8"));
  const pics: Pic[] = [];
  const walk = (o: unknown): void => {
    if (Array.isArray(o)) { o.forEach(walk); return; }
    if (o && typeof o === "object") {
      const rec = o as Record<string, unknown>;
      if (Array.isArray(rec.pics)) pics.push(...(rec.pics as Pic[]));
      Object.values(rec).forEach(walk);
    }
  };
  for (const s of Object.values(golden.states)) walk((s as { keep: unknown }).keep);
  const norm = (s: string) => s.replace(/\s+/g, " ").trim();

  it("every pictured code in golden has FISH[code], captioned by its first image + \" ›\"", () => {
    const coded = pics.filter((p) => p.code);
    expect(coded.length).toBeGreaterThan(0);
    const bad = coded.filter((p) => !FISH[p.code!] ||
      norm(p.caption ?? "") !== norm(`${FISH[p.code!][0].caption} ›`));
    expect(bad).toEqual([]);
  });
});
