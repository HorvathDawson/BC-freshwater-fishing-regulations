/**
 * THE COMPOSED STYLES, HELD BYTE FOR BYTE (T1154 L1).
 *
 * The phone's style (`nativeStyle`) and the web's (`runtimeStyle`) for a fixed set of looks, serialised the way the
 * phone's binding is handed it (`JSON.stringify`), are held equal to `fixtures/map-style/composed.json`. The file was
 * recorded from the code before the map engine seam moved any of it, so a refactor of the composition must leave every
 * byte. A change that MEANS to move the look re-records the file in the same commit, and the file's diff is the review.
 *
 * THE LOOKS: on the phone every view × theme × ground, on the web every view × theme (and each theme with no modes
 * given), taken from `STYLE_META` (the last test holds the list whole, so a new view or theme is a new look and a
 * re-record); plus the paths only some looks take: the field's values (the area tables), the day's profiles, the Runs
 * field, the route map's pins and lines (the pin image), focus and the overlay groups, and a keyed imagery template.
 * 44 styles, about 8 MB of text; 1.6 MB on disk with each shared piece stored once.
 *
 * THE FILE: each style is cut one level down (its sources by name, its layers by place) and each distinct piece is stored
 * once, one per line, so a moved paint shows as the lines that moved. The test compares piece by piece (naming the source
 * or layer that moved) and then the whole text.
 *
 * Re-record (from app/): `STYLE_GOLDEN=write pnpm exec vitest run tools/style-golden.test.ts`. It writes the file and
 * FAILS, so a recording run is never a pass.
 */
import { describe, expect, it } from "vitest";
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { STYLE_META, runtimeStyle } from "@app/map";
import { nativeStyle, type NativeStyleInput } from "../packages/map/src/native-style";
import { NO_VALUES } from "../packages/map/src/style";

const GOLDEN = fileURLToPath(new URL("../fixtures/map-style/composed.json", import.meta.url));
const WRITE = process.env.STYLE_GOLDEN === "write";

const GAUGES = '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[-122,49]},'
  + '"properties":{"label":"15.7 m³/s · p4th"}}]}';
/** A phone's endpoints: the installed files (`releaseTiles` on a phone). */
const PHONE = { atlas: "file:///phone/release/tiles/atlas.pmtiles", basemap: "file:///phone/basemap.pmtiles",
                outside: "file:///phone/release/tiles/bc_outside.geojson", assets: "file:///phone/release/map-assets/" };
/** The web's: the archives over https. */
const WEB = { atlas: "https://tiles.example.invalid/atlas.pmtiles", basemap: "https://tiles.example.invalid/basemap.pmtiles",
              outside: "https://tiles.example.invalid/bc_outside.geojson", assets: "https://tiles.example.invalid/map-assets/",
              gauges: GAUGES };

const view = (id: string) => {
  const v = STYLE_META.views.find((x) => x.id === id);
  if (!v) throw new Error(`no view "${id}" in STYLE_META`);
  return { view: v.id, modes: { ...v.modes }, hide: [...(v.hide ?? [])], show: [...(v.show ?? [])] };
};
type Ground = NativeStyleInput["basemap"];
const phone = (id: string, theme: string, basemap: Ground, more: Partial<NativeStyleInput> = {}): NativeStyleInput =>
  ({ at: PHONE, theme, basemap, groups: {}, focus: false, gauges: GAUGES, ...view(id), ...more });

const PINS = [{ lat: 49.1, lon: -121.9, kind: "here" as const, tone: "tone-ink", label: "You are here", rank: 0 },
              { lat: 49.2, lon: -122.1, kind: "gauge" as const, tone: "tone-blue", label: "62%", rank: 1 }];
const LINES = { type: "FeatureCollection" as const, features: [{ type: "Feature" as const,
  properties: { role: "down" as const, tone: "tone-blue", kind: "line" as const, order: 1, on: false },
  geometry: { type: "MultiLineString" as const, coordinates: [[[-122, 49], [-121.9, 49.1]] as [number, number][]] } }] };
const STATUS = { closed: ["p0"], own: ["p1"], base: ["b"], no_access: [], none: [] };

const THEMES = Object.keys(STYLE_META.themes);
const VIEWS = STYLE_META.views.map((v) => v.id);
const GROUNDS = ["map", "satellite"] as const;

/** Each look by name: every view × theme × ground on the phone, then the paths only some looks take. */
const NATIVE: Record<string, NativeStyleInput> = {
  ...Object.fromEntries(VIEWS.flatMap((v) => THEMES.flatMap((t) => GROUNDS.map((g) =>
    [`phone · ${v} · ${t} · ${g}`, phone(v, t, g)])))),
  "phone · plain · dark · satellite · route pins and lines, focus, obstacles and depth": phone("plain", "dark", "satellite",
    { tracePins: PINS, traceLines: LINES, focus: true, groups: { obstacles: true, depth: true } }),
  "phone · conditions · cvd · map · the field's values (area tables)": phone("conditions", "cvd", "map", {
    areaData: { basin: { "BX-1": { standing: 42 }, "7": { standing: -100 } } },
    values: { ...NO_VALUES, standing: { section: () => NaN, basins: { "BX-1": 30, "C-2": 55 } } } as never }),
  "phone · plain · light · map · the day's profiles": phone("plain", "light", "map", {
    modes: { ...view("plain").modes, stream: "profile", lake: "profile" }, values: { ...NO_VALUES, status: STATUS } as never }),
  "phone · plain · dark · map · the Runs field": phone("plain", "dark", "map", {
    modes: { ...view("plain").modes, stream: "runs", basin: "runs" },
    values: { ...NO_VALUES, runs: { key: "6:44", basins: { LFRA: "pass", CHWK: "spawn" } } } as never }),
};
/** The web's: every theme with no modes given (the default view's), every view × theme, and a keyed imagery template. */
const RUNTIME: Record<string, () => unknown> = {
  ...Object.fromEntries(THEMES.map((t) => [`web · ${t} · no modes`, () => runtimeStyle(WEB, t)])),
  ...Object.fromEntries(VIEWS.flatMap((v) => THEMES.map((t) =>
    [`web · ${v} · ${t}`, () => runtimeStyle(WEB, t, view(v).modes)]))),
  "web · light · keyed imagery template": () =>
    runtimeStyle({ ...WEB, imagery: "https://imagery.example.invalid/tile/{z}/{y}/{x}?token=k" }, "light"),
};
const compose = (name: string): Record<string, unknown> => {
  const s = NATIVE[name] ? nativeStyle(NATIVE[name]).style : RUNTIME[name]!();
  // the serialised text, parsed back: what is held is exactly what `JSON.stringify` hands on
  return JSON.parse(JSON.stringify(s)) as Record<string, unknown>;
};
const NAMES = [...Object.keys(NATIVE), ...Object.keys(RUNTIME)];

/** A style one level down: each top-level value, each source by name, each layer by place, as a piece number. */
type Node = number | { a: number[] } | { o: [string, number][] };
type Skeleton = [string, Node][];
interface Golden { $about: string; looks: Record<string, Skeleton>; parts: unknown[] }

const cut = (style: Record<string, unknown>, intern: (text: string) => number): Skeleton =>
  Object.entries(style).map(([k, v]): [string, Node] => [k,
    Array.isArray(v) ? { a: v.map((x) => intern(JSON.stringify(x))) }
      : v !== null && typeof v === "object"
        ? { o: Object.entries(v).map(([n, x]): [string, number] => [n, intern(JSON.stringify(x))]) }
        : intern(JSON.stringify(v))]);

const rebuild = (sk: Skeleton, text: (i: number) => string): string => {
  const node = (n: Node) => (typeof n === "number" ? text(n)
    : "a" in n ? `[${n.a.map(text).join(",")}]`
      : `{${n.o.map(([k, i]) => `${JSON.stringify(k)}:${text(i)}`).join(",")}}`);
  return `{${sk.map(([k, n]) => `${JSON.stringify(k)}:${node(n)}`).join(",")}}`;
};

/** Where `style` differs from the held skeleton: a key, a source by name, a layer by place and id. */
function moved(style: Record<string, unknown>, sk: Skeleton, text: (i: number) => string): string[] {
  const out: string[] = [];
  if (Object.keys(style).join() !== sk.map(([k]) => k).join())
    out.push(`top-level keys: ${sk.map(([k]) => k).join()} -> ${Object.keys(style).join()}`);
  for (const [k, n] of sk) {
    const v = style[k];
    if (typeof n === "number") { if (JSON.stringify(v) !== text(n)) out.push(k); continue; }
    if ("a" in n) {
      const items = (Array.isArray(v) ? v : []) as { id?: string }[];
      if (items.length !== n.a.length) out.push(`${k}: ${n.a.length} -> ${items.length} items`);
      n.a.forEach((i, j) => { if (JSON.stringify(items[j]) !== text(i)) out.push(`${k}[${j}] ${items[j]?.id ?? ""}`); });
      continue;
    }
    const obj = (v ?? {}) as Record<string, unknown>;
    if (Object.keys(obj).join() !== n.o.map(([x]) => x).join())
      out.push(`${k}: ${n.o.map(([x]) => x).join()} -> ${Object.keys(obj).join()}`);
    for (const [name, i] of n.o) if (JSON.stringify(obj[name]) !== text(i)) out.push(`${k}.${name}`);
  }
  return out;
}

if (WRITE) {
  it("records fixtures/map-style/composed.json (a recording run never passes)", () => {
    const parts: string[] = [];
    const ids = new Map<string, number>();
    const intern = (t: string) => {
      if (!ids.has(t)) { ids.set(t, parts.length); parts.push(t); }
      return ids.get(t)!;
    };
    const looks = NAMES.map((name) => [name, cut(compose(name), intern)] as const);
    const about = "T1154 L1: the phone's and the web's composed styles, byte for byte (tools/style-golden.test.ts says "
      + "how it is read and re-recorded). Each look is a skeleton of piece numbers; each piece is one line of `parts`.";
    writeFileSync(GOLDEN, `{"$about":${JSON.stringify(about)},\n"looks":{\n`
      + looks.map(([n, sk]) => `${JSON.stringify(n)}:${JSON.stringify(sk)}`).join(",\n")
      + `\n},\n"parts":[\n${parts.join(",\n")}\n]}\n`);
    throw new Error(`written: ${GOLDEN} (${looks.length} looks, ${parts.length} pieces); run again without STYLE_GOLDEN`);
  });
} else {
  const golden = JSON.parse(readFileSync(GOLDEN, "utf8")) as Golden;
  const text = (i: number) => {
    if (i >= golden.parts.length) throw new Error(`piece ${i} is not in the golden file`);
    return JSON.stringify(golden.parts[i]);
  };

  describe("the composed styles are the ones recorded, byte for byte", () => {
    for (const name of NAMES) {
      it(name, () => {
        const sk = golden.looks[name];
        expect(sk, `no look "${name}" in the golden file`).toBeDefined();
        const style = compose(name);
        const m = moved(style, sk!, text);
        expect(m.length, `${m.length} piece(s) moved:\n${m.slice(0, 20).join("\n")}`).toBe(0);
        expect(JSON.stringify(style)).toBe(rebuild(sk!, text));
      });
    }

    it("holds exactly these looks", () => {
      expect(Object.keys(golden.looks)).toEqual(NAMES);
    });

    it("covers every view × theme × ground on the phone, every view × theme on the web", () => {
      const native = Object.values(NATIVE).map((l) => `${l.view} ${l.theme} ${l.basemap}`);
      const web = Object.keys(RUNTIME);
      for (const v of VIEWS) for (const t of THEMES) {
        for (const g of GROUNDS) expect(native, `phone ${v} ${t} ${g}`).toContain(`${v} ${t} ${g}`);
        expect(web, `web ${v} ${t}`).toContain(`web · ${v} · ${t}`);
      }
      expect([VIEWS.length, THEMES.length]).toEqual([4, 3]);   // what the counts in the header say
    });
  });
}
