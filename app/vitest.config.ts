import { defineConfig } from "vitest/config";

/**
 * Two projects, because the packages have different needs and one global environment
 * would be wrong for both.
 *
 * The alias is what makes `@app/ui-native` testable at all: its components import
 * `react-native`, which has no DOM. react-native-web IS the mobile-web target, so pointing
 * at it here exercises the tree a browser actually runs — not a stub of it. Native-only
 * behaviour remains unproven by tests, which is a known gap rather than a covered one.
 */
import { fileURLToPath } from "node:url";

const pkg = (name: string, entry = "src/index.ts") =>
  fileURLToPath(new URL(`packages/${name}/${entry}`, import.meta.url));

/**
 * Workspace packages are resolved to SOURCE, not through the node_modules symlink.
 *
 * `main` is `./src/index.ts`, and vitest treats anything reached through node_modules as
 * external — so Node got raw TypeScript and choked on the first `typeof` in a type
 * position. Pointing at the file directly puts them through the same transform as our own
 * code, and has the side benefit that a stale `dist/` can never be picked up instead.
 */
const alias = [
  { find: /^react-native$/, replacement: "react-native-web" },
  // react-native-svg's default entry is the NATIVE implementation; the browser one is the
  // `.web.js` sibling. Metro and webpack pick it by platform extension, so the package
  // relies on that and never declares a `browser` field. Name the entry here, and let
  // `resolve.extensions` below do the same job for everything it goes on to import.
  { find: /^react-native-svg$/,
    replacement: fileURLToPath(new URL(
      "node_modules/react-native-svg/lib/module/ReactNativeSVG.web.js", import.meta.url)) },
  { find: "@app/core", replacement: pkg("core") },
  { find: "@app/data/fixture", replacement: pkg("data", "src/fixture.ts") },
  // Subpath exports need their own alias: vitest resolves through these, not through the
  // package's `exports` map, so a new subpath is invisible until it is named here.
  { find: "@app/data/spots", replacement: pkg("data", "src/spots/index.ts") },
  { find: "@app/data/bundle/node", replacement: pkg("data", "src/bundle/drivers/node.ts") },
  { find: "@app/data/bundle", replacement: pkg("data", "src/bundle/source.ts") },
  { find: "@app/data", replacement: pkg("data") },
  { find: "@app/ui", replacement: pkg("ui") },
  { find: "@app/map", replacement: pkg("map") },
  { find: "@app/ui-native", replacement: pkg("ui-native") },
];

/**
 * Workspace packages resolve through node_modules symlinks to raw TypeScript (`main` is
 * `./src/index.ts`). Vitest treats anything under node_modules as external and hands it
 * to Node unprocessed, which chokes on the first `typeof` in a type position. Inlining
 * them means the same transform pipeline that handles our own files handles theirs.
 */
const inline = [
  /^@app\//,
  // react-native-svg ships untranspiled sources for the native entry point, so Node sees
  // TypeScript when vitest treats it as external. It is our only third-party renderer
  // dependency, so inlining it is cheap and precise.
  "react-native-web",
  "react-native-svg",
];

/**
 * `.web.js` FIRST — this is platform-extension resolution, the thing every react-native-web
 * bundler does and vite does not.
 *
 * react-native-svg ships `elements.js` (native) beside `elements.web.js` (DOM) and imports
 * `'./elements'` with no extension, trusting the bundler to pick. Without this, vite takes
 * the native file, which imports react-native's Flow-typed internals and fails to parse —
 * an error a hundred modules away from the actual cause.
 */
const extensions = [".web.tsx", ".web.ts", ".web.jsx", ".web.js",
                    ".mjs", ".js", ".mts", ".ts", ".jsx", ".tsx", ".json"];

export default defineConfig({
  test: {
    projects: [
      {
        test: { name: "logic", environment: "node", server: { deps: { inline } },
                include: ["packages/core/**/*.test.ts", "packages/data/**/*.test.ts",
                          "packages/ui/**/*.test.ts", "packages/map/**/*.test.ts",
                          "conformance/**/*.test.ts", "tools/**/*.test.ts"] },
        resolve: { alias, extensions },
      },
      {
        test: { name: "render", environment: "jsdom", server: { deps: { inline } },
                setupFiles: ["./tools/render-setup.ts"],
                // @app/ui is arithmetic and runs in node, EXCEPT its hooks: a hook has to be
                // mounted to be proved, and the one bug that reached a browser was a render
                // loop no unit test could see.
                include: ["packages/ui-native/**/*.test.{ts,tsx}",
                          "packages/ui/**/*.render.test.tsx"] },
        resolve: { alias, extensions },
      },
    ],
  },
});
