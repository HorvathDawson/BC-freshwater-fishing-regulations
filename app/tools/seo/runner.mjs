/**
 * T193/T269/T279 — THE LOADER the SEO build runs its TypeScript through, for the main thread (tools/build-seo.mjs) and each
 * render thread (tools/seo/pane-worker.mjs): vite's transform with vitest.config.ts's aliases, run by vite-node (vitest's
 * own module runner; its CommonJS interop is what react-native-web's dependencies need). react-native resolves to
 * react-native-web, `.web.*` files first, and the app's `require("<picture>")` (Metro's static asset form) becomes a call the
 * build answers with the URL the web export serves it at. Neither `vite` nor `vite-node` is a new dependency (both are vitest's).
 */
import { fileURLToPath } from "node:url";
import { createServer } from "vite";
import { ViteNodeServer } from "vite-node/server";
import { ViteNodeRunner } from "vite-node/client";
import { readFileSync } from "node:fs";

/** The workspace packages the app's code imports as `@app/<name>[/<subpath>]`. */
const PACKAGES = ["core", "data", "ui", "ui-native", "map"];

/**
 * Each `@app/<name>[/<subpath>]` the packages export, read from their own package.json `exports` (a package without one
 * exports `src/index.ts`), as an EXACT-match alias. Read, not listed by hand: a hand list drifted twice (T369's
 * `@app/data/regs/place` and `@app/ui/place` broke the 0.5.0 staging build), and a prefix match sent a subpath to its
 * parent's index (`@app/data/regs` + `/place`).
 */
function workspaceAliases(app) {
  return PACKAGES.flatMap((name) => {
    const exp = JSON.parse(readFileSync(`${app}packages/${name}/package.json`, "utf8")).exports ?? { ".": "./src/index.ts" };
    return Object.entries(exp).map(([sub, file]) => ({
      find: new RegExp(`^@app/${name}${sub === "." ? "" : sub.slice(1).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`),
      replacement: `${app}packages/${name}/${file.replace(/^\.\//, "")}`,
    }));
  });
}

/** A runner (`executeFile(<absolute path>)`) and its close. */
export async function makeRunner() {
  process.env.NODE_ENV ??= "production";   // react-native-web's production class names, as the exported app draws them
  const app = fileURLToPath(new URL("../..", import.meta.url));
  const pkg = (name, entry = "src/index.ts") => `${app}packages/${name}/${entry}`;
  // releaseDir.ts (a node-only driver) reads `__dirname`, which vitest provides and an ESM module runner does not
  globalThis.__dirname = `${app}packages/data/src/sql/drivers`;
  // Metro defines it for every bundle; react-native-gesture-handler reads it at module load (as tools/render-setup.ts)
  globalThis.__DEV__ = false;

  /** Metro's `require("../assets/x.webp")` in the app's generated picture tables, as a call the build answers (appExport.ts). */
  const assets = {
    name: "cift-assets",
    transform(code, id) {
      if (!/fishImages\.generated\.ts$/.test(id)) return null;
      const dir = id.slice(0, id.lastIndexOf("/") + 1);
      return code.replace(/require\("([^"]+)"\)/g, (_, p) => `globalThis.__ciftAsset(${JSON.stringify(new URL(p, `file://${dir}`).pathname)})`);
    },
  };

  const server = await createServer({
    root: app, configFile: false, logLevel: "error", appType: "custom", server: { middlewareMode: true, hmr: false, ws: false, watch: null },
    plugins: [assets],
    esbuild: { jsx: "automatic" },
    resolve: {
      extensions: [".web.tsx", ".web.ts", ".web.jsx", ".web.js", ".mjs", ".js", ".mts", ".ts", ".jsx", ".tsx", ".json"],
      alias: [
        { find: /^react-native$/, replacement: "react-native-web" },
        { find: /^react-native-svg$/, replacement: `${app}node_modules/react-native-svg/lib/module/ReactNativeSVG.web.js` },
        { find: /^react-native-gesture-handler$/, replacement: `${app}node_modules/react-native-gesture-handler/lib/module/index.js` },
        ...workspaceAliases(app),
      ],
    },
  });
  await server.pluginContainer.buildStart({});
  const node = new ViteNodeServer(server, { deps: { inline: [/^@app\//, "react-native-web", "react-native-svg", "react-native-gesture-handler"] } });
  const runner = new ViteNodeRunner({
    root: app, base: server.config.base, interopDefault: true,
    fetchModule: (id) => node.fetchModule(id), resolveId: (id, importer) => node.resolveId(id, importer),
  });


  return { app, run: (file) => runner.executeFile(file), close: () => server.close() };
}
