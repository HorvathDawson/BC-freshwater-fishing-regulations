/**
 * (T816) NO TEXT IN A LINE BOX SHORTER THAN ITS LETTERS. iOS cuts what a `lineHeight` lacks off the top of the glyphs
 * (`type.ts` FACE_METRICS); Android and the web draw the overflow, so a too-short box shows only on an iPhone: the
 * owner's build 36 had "18:00" on "When were you there?" (JetBrains Mono 30 px in a 23 px box) and the spot card's
 * "8°C" (32 px in 23) cut to fragments.
 *
 * Checked: the metrics against the font files themselves; every TYPE entry; and every Text style in the app's source that
 * starts from a TYPE entry (`...TYPE.x, …`, `[TYPE.x, {…}]`, `typeAt("x", n)`) with the size and line height it ends up with.
 * Before the fix the source scan named the three oversized sites; the near-miss below must still fail.
 */
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { FACE, FACE_METRICS, lineBoxFloor, TYPE, typeAt, type TextStyle } from "../packages/ui-native/src/type";

const HERE = dirname(fileURLToPath(import.meta.url));
const APP = join(HERE, "..");   // app/ (this test lives in app/tools: it reads the source and the font files)
/** a sub-pixel cut of the headroom (the month initials: 10 px in 10, 0.3 px short) is not a cut letter */
const SLACK = 0.5;

/** hhea ascent / descent and OS/2 cap height, per em, read from a TrueType file */
function fileMetrics(path: string): { ascent: number; descent: number; cap: number } {
  const b = readFileSync(path);
  const tables = new Map<string, number>();
  for (let i = 0, n = b.readUInt16BE(4); i < n; i++) tables.set(b.toString("latin1", 12 + i * 16, 16 + i * 16), b.readUInt32BE(20 + i * 16));
  const head = tables.get("head")!, hhea = tables.get("hhea")!, os2 = tables.get("OS/2")!;
  const upm = b.readUInt16BE(head + 18);
  return { ascent: b.readInt16BE(hhea + 4) / upm, descent: -b.readInt16BE(hhea + 6) / upm, cap: b.readInt16BE(os2 + 88) / upm };
}

const FONT_DIR: Record<keyof typeof FACE_METRICS, string> = {
  BricolageGrotesque: "bricolage-grotesque", Archivo: "archivo", JetBrainsMono: "jetbrains-mono",
};

describe("the faces' metrics are the font files'", () => {
  for (const face of Object.values(FACE)) {
    it(face, () => {
      const family = face.split("_")[0] as keyof typeof FACE_METRICS;
      const weight = face.slice(family.length + 1);
      const file = join(APP, "node_modules", "@expo-google-fonts", FONT_DIR[family], weight, `${face}.ttf`);
      const got = fileMetrics(file);
      for (const k of ["ascent", "descent", "cap"] as const) expect(FACE_METRICS[family][k]).toBeCloseTo(got[k], 3);
    });
  }
});

function short(s: TextStyle): number | null {
  if (s.lineHeight === undefined) return null;   // no line height: the face's own, nothing cut
  const need = lineBoxFloor(s.fontFamily, s.fontSize);
  return s.lineHeight + SLACK < need ? need - s.lineHeight : null;
}

describe("every TYPE entry's line box holds its letters", () => {
  for (const [k, s] of Object.entries(TYPE)) it(k, () => expect(short(s)).toBeNull());
  it("typeAt scales the box with the size", () => {
    expect(typeAt("figureBig", 30)).toMatchObject({ fontSize: 30, lineHeight: 36.32 });
    expect(short(typeAt("figureBig", 32))).toBeNull();
  });
});

/** the object literal's text from `from` to the brace closing the object it is in */
function restOfObject(src: string, from: number): string {
  let depth = 0;
  for (let i = from; i < src.length; i++) {
    const c = src[i];
    if (c === "{" || c === "(" || c === "[") depth++;
    else if (c === "}" || c === ")" || c === "]") { if (depth === 0) return src.slice(from, i); depth--; }
  }
  return src.slice(from);
}
const num = (rest: string, prop: string) => {
  const m = new RegExp(`(?:^|[,{\\s])${prop}:\\s*([\\d.]+)`).exec(rest);
  return m ? Number(m[1]) : undefined;
};

/** every Text style in `src` built on a TYPE entry, as the size and line height it ends with; each one too short */
function tooShort(src: string): string[] {
  const out: string[] = [];
  const check = (at: number, base: TextStyle, rest: string) => {
    const fontSize = num(rest, "fontSize") ?? base.fontSize;
    const own = num(rest, "lineHeight");
    const s: TextStyle = { ...base, fontSize, ...(own !== undefined ? { lineHeight: own }
      : base.lineHeight !== undefined ? { lineHeight: base.lineHeight } : {}) };
    const by = short(s);
    if (by !== null) out.push(`line ${src.slice(0, at).split("\n").length}: ${fontSize} px in a ${s.lineHeight} px box `
      + `(${by.toFixed(1)} px of the letters cut on iOS; use typeAt or a lineHeight)`);
  };
  const entry = (k: string) => (TYPE as Record<string, TextStyle>)[k];
  for (const m of src.matchAll(/\.\.\.TYPE\.(\w+)/g)) if (entry(m[1]!)) check(m.index, entry(m[1]!)!, restOfObject(src, m.index + m[0].length));
  for (const m of src.matchAll(/\[\s*TYPE\.(\w+)\s*,\s*\{/g)) if (entry(m[1]!)) check(m.index, entry(m[1]!)!, restOfObject(src, m.index + m[0].length));
  for (const m of src.matchAll(/\.\.\.typeAt\("(\w+)",\s*([\d.]+)\)/g))
    if (entry(m[1]!)) check(m.index, typeAt(m[1] as keyof typeof TYPE, Number(m[2])), restOfObject(src, m.index + m[0].length));
  return out;
}

function sources(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((e) => {
    const p = join(dir, e.name);
    if (e.isDirectory()) return e.name === "node_modules" || e.name === "ios" || e.name === "android" ? [] : sources(p);
    return /\.tsx?$/.test(e.name) && !/\.test\.tsx?$/.test(e.name) ? [p] : [];
  });
}

describe("every Text style in the app's source holds its letters", () => {
  it("none is shorter than its face needs", () => {
    const bad = [join(APP, "packages", "ui-native", "src"), join(APP, "apps", "mobile", "src")].flatMap(sources)
      .flatMap((f) => tooShort(readFileSync(f, "utf8")).map((l) => `${relative(APP, f)} ${l}`));
    expect(bad).toEqual([]);
  });
  it("the near-misses fail: build 36's hour, a size spread past its box, an array override", () => {
    expect(tooShort(`<Text style={{ ...TYPE.figureBig, fontSize: 30, flex: 1 }}>`)).toHaveLength(1);
    expect(tooShort(`<Text style={{ ...typeAt("figureBig", 30), lineHeight: 23 }}>`)).toHaveLength(1);
    expect(tooShort(`<Text style={[TYPE.body, { fontSize: 26, color: c }]}>`)).toHaveLength(1);
    expect(tooShort(`<Text style={{ ...typeAt("figureBig", 30), flex: 1 }}>`)).toEqual([]);
    expect(tooShort(`<Text style={{ ...TYPE.section, fontSize: 10.5, letterSpacing: 1.5 }}>`)).toEqual([]);
  });
});
