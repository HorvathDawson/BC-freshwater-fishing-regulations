/**
 * T83 left: build-style's three `outline.drawAfter` refusals, each run on a copy of the style folder
 * (`STYLE_SRC_DIR`) with one mutation — the real source is never written. A refusal exits 1 and names the fault;
 * the unmutated copy builds.
 */
import { execFileSync } from "node:child_process";
import { cpSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it } from "vitest";

const STYLE = new URL("../packages/map/style/", import.meta.url).pathname;
const SCRIPT = new URL("./build-style.mjs", import.meta.url).pathname;
const made: string[] = [];

type Layer = { id: string; outline?: Record<string, unknown> };

function run(mutate?: (layers: Layer[]) => void,
             imagery?: (values: Record<string, unknown>) => void): { code: number; out: string } {
  const d = mkdtempSync(join(tmpdir(), "style-"));
  made.push(d);
  cpSync(STYLE, d, { recursive: true });
  const src = JSON.parse(readFileSync(join(d, "layers.source.json"), "utf8"));
  mutate?.(src.layers);
  writeFileSync(join(d, "layers.source.json"), JSON.stringify(src));
  const im = JSON.parse(readFileSync(join(d, "imagery.json"), "utf8"));
  imagery?.(im.values);
  writeFileSync(join(d, "imagery.json"), JSON.stringify(im));
  try {
    const out = execFileSync(process.execPath, [SCRIPT], { env: { ...process.env, STYLE_SRC_DIR: d + "/" },
                                                          stdio: "pipe", encoding: "utf8" });
    return { code: 0, out };
  } catch (e) {
    const x = e as { status: number; stderr: string; stdout: string };
    return { code: x.status, out: `${x.stdout}${x.stderr}` };
  }
}

const lake = (ls: Layer[]) => ls.find((l) => l.outline?.drawAfter !== undefined)!;

afterAll(() => { for (const d of made) rmSync(d, { recursive: true, force: true }); });

describe("outline.drawAfter refusals", () => {
  it("the copy as it is builds", () => {
    expect(run().code).toBe(0);
  }, 60_000);

  it("refuses an outline both under its fill and drawn after a later layer", () => {
    const r = run((ls) => { lake(ls).outline!.under = true; });
    expect(r.code).toBe(1);
    expect(r.out).toContain('cannot be both "under" its fill and "drawAfter" a later layer');
  }, 60_000);

  it("refuses a drawAfter that names no layer", () => {
    const r = run((ls) => { lake(ls).outline!.drawAfter = "no_such_layer"; });
    expect(r.code).toBe(1);
    expect(r.out).toContain('outline.drawAfter names "no_such_layer", which is not a layer');
  }, 60_000);

  it("refuses a drawAfter that comes before the fill it outlines", () => {
    const r = run((ls) => { lake(ls).outline!.drawAfter = ls[0]!.id; });   // the first layer: before the lake
    expect(r.code).toBe(1);
    expect(r.out).toContain("must come later in the source than the fill it outlines");
  }, 60_000);
});

/*
 * (T940) THE SATELLITE SET's refusals (`imagery.json`): it holds values for real, themeable tokens only, and an "@" may
 * name a token in the set or one already the same in every theme, never a themed one, which would bring the theme back.
 */
describe("the satellite set's refusals (T940)", () => {
  it("refuses a value for a token that does not exist", () => {
    const r = run(undefined, (v) => { v["color.status.nope"] = v["color.status.open"]; });
    expect(r.code).toBe(1);
    expect(r.out).toContain('imagery.json: "color.status.nope" is not a token');
  }, 60_000);

  it("refuses a name that points at a token which differs by theme and is not in the set", () => {
    const r = run(undefined, (v) => { v["color.status.open"] = "@color.ui.ink"; });
    expect(r.code).toBe(1);
    expect(r.out).toContain('imagery.json: color.status.open names "@color.ui.ink", which is neither in the set nor the same in every theme');
  }, 60_000);
});
