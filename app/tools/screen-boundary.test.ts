/**
 * (T1164) EVERY SCREEN IS MOUNTED THROUGH AN ERROR BOUNDARY. The shell (`Shell.tsx`) is the one place screens are mounted:
 * the four hosts (a spot, the add-a-spot flow, a water, the browse tabs) go through its `screen(name, key, <Host/>)`
 * helper, which wraps them in `ScreenBoundary`. Every screen below them (the Map, Conditions, Spots, More, the water's
 * views) is drawn by one of those hosts, so a fault in it ends that screen only, and is reported.
 *
 * A NEW SCREEN MOUNTED IN THE SHELL WITHOUT THE HELPER FAILS HERE: a component whose name ends in `Screen` or `Host` (or the
 * spot capture) drawn in `Shell.tsx` must be one of the four, on the helper's line. The scan is a net: it reads the one
 * file where screens are mounted; a screen mounted from some other file is a new mount point, and this test says so.
 */
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const APP = fileURLToPath(new URL("..", import.meta.url));
const SHELL = readFileSync(join(APP, "packages/ui-native/src/Shell.tsx"), "utf8");
const code = SHELL.replace(/\/\*[\s\S]*?\*\//g, (c) => c.replace(/[^\n]/g, "")).replace(/(^|[\s;{}),])\/\/.*$/gm, "$1");

/** Each component the shell draws as a screen: `<NameScreen`, `<NameHost`, `<SpotCapture`. */
const mounts = (src: string): { name: string; line: string }[] =>
  [...src.matchAll(/<((?:[A-Z]\w*)?(?:Screen|Host)|SpotCapture)\b/g)].map((m) => ({
    name: m[1]!, line: src.slice(src.lastIndexOf("\n", m.index) + 1, src.indexOf("\n", m.index)) }));

describe("every screen is mounted through a ScreenBoundary", () => {
  it("the shell mounts exactly its four screen hosts, each on the line of `screen(…)`", () => {
    const found = mounts(code);
    expect(found.map((m) => m.name).sort()).toEqual(["BrowseHost", "SpotCapture", "SpotHost", "WaterModeHost"]);
    for (const m of found) expect(m.line, `${m.name} is mounted outside screen(…)`).toMatch(new RegExp(`screen\\([^<]*<${m.name}\\b`));
  });

  it("the helper wraps its screen in ScreenBoundary, which resets on the key and goes back on goBack", () => {
    expect(code).toMatch(/const screen = \(name: ScreenName, key: string, node: React\.ReactNode, goBack\?: \(\) => void\) => \(\s*<ScreenBoundary screen=\{name\} resetKey=\{key\} palette=\{palette\} goBack=\{goBack\}>\{node\}<\/ScreenBoundary>\);/);
  });

  it("the scan catches a screen mounted bare (a fixture)", () => {
    const bare = `return <View><NewThingScreen a={1} /></View>;`;
    const [m] = mounts(bare);
    expect(m!.name).toBe("NewThingScreen");
    expect(m!.line).not.toMatch(/screen\([^<]*<NewThingScreen\b/);
  });
});
