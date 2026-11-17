/**
 * (T741) THE APP SHIPS ONLY THE TYPE FACES IT DRAWS. A family's index (`@expo-google-fonts/archivo`) `require`s every
 * face it has, and Metro puts every required asset in the app: importing 8 faces from three indexes shipped 41 (4.78 MB
 * in the AAB and in the iOS app, 33 of them never drawn). So:
 *   1. no app source imports a family's index, only a weight's folder (`@expo-google-fonts/archivo/400Regular`);
 *   2. every face a style names (`fontFamily: "Archivo_600SemiBold"`, in any app source) is imported and loaded by the
 *      app (App.tsx's `useFonts`), so no text falls back to the system face — the set named and the set loaded are equal;
 *   3. each imported weight folder exists and exports exactly that face.
 */
import { describe, expect, it } from "vitest";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const APP = fileURLToPath(new URL("..", import.meta.url));
const ROOTS = ["packages/core/src", "packages/data/src", "packages/ui/src", "packages/ui-native/src",
  "packages/map/src", "apps/mobile/src"];
const APP_TSX = "apps/mobile/src/App.tsx";
const FACE = /\b(Archivo|BricolageGrotesque|JetBrainsMono)_\d{3}[A-Za-z]+(?:_Italic)?\b/g;
const IMPORT = /import\s*\{([^}]*)\}\s*from\s*"(@expo-google-fonts\/[^"]+)"/g;

function sources(dir: string, out: string[] = []): string[] {
  for (const f of readdirSync(dir)) {
    if (f === "node_modules" || f === "dist" || f.startsWith(".")) continue;
    const p = join(dir, f);
    if (statSync(p).isDirectory()) sources(p, out);
    else if (/\.(ts|tsx)$/.test(f) && !/\.test\.tsx?$/.test(f)) out.push(p);
  }
  return out;
}
const files = ROOTS.flatMap((r) => sources(join(APP, r)));
const rel = (p: string) => relative(APP, p);

describe("(T741) type faces: only those drawn are shipped", () => {
  it("no app source imports a font family's index (it would ship every face of the family)", () => {
    const barrels = files.flatMap((p) => [...readFileSync(p, "utf8").matchAll(IMPORT)]
      .filter((m) => !/^@expo-google-fonts\/[a-z-]+\/\d{3}[A-Za-z_]+$/.test(m[2]!))
      .map((m) => `${rel(p)} imports from "${m[2]}"`));
    expect(barrels).toEqual([]);
  });

  it("every face a style names is the set App.tsx imports and loads", () => {
    const named = new Set<string>();
    for (const p of files) {
      if (rel(p) === APP_TSX) continue;
      for (const m of readFileSync(p, "utf8").matchAll(FACE)) named.add(m[0]);
    }
    const app = readFileSync(join(APP, APP_TSX), "utf8");
    const imported = new Set([...app.matchAll(IMPORT)].flatMap((m) => m[1]!.split(",").map((s) => s.trim()).filter(Boolean)));
    const loads = app.match(/useFonts\(\{([^}]*)\}\)/);
    expect(loads, "App.tsx loads its faces with useFonts({ … })").not.toBeNull();
    const loaded = new Set(loads![1]!.split(",").map((s) => s.trim().split(":")[0]!.trim()).filter(Boolean));
    expect([...named].sort()).toEqual([...imported].sort());
    expect([...loaded].sort()).toEqual([...imported].sort());
  });

  it("each imported weight folder exists and exports exactly its face", () => {
    const app = readFileSync(join(APP, APP_TSX), "utf8");
    const bad: string[] = [];
    for (const m of app.matchAll(IMPORT)) {
      const index = join(APP, "node_modules", m[2]!, "index.js");
      if (!existsSync(index)) { bad.push(`${m[2]}: no index.js`); continue; }
      const exported = [...readFileSync(index, "utf8").matchAll(/export const (\w+)/g)].map((e) => e[1]);
      const asked = m[1]!.split(",").map((s) => s.trim()).filter(Boolean);
      if (JSON.stringify(exported) !== JSON.stringify(asked)) bad.push(`${m[2]} exports ${exported.join(", ")}, App.tsx asks ${asked.join(", ")}`);
    }
    expect(bad).toEqual([]);
  });
});
