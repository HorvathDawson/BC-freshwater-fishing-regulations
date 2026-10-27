#!/usr/bin/env node
/**
 * T193 - the SEO static-site build. The page logic is TypeScript over the workspace packages (tools/seo/), which plain Node
 * cannot load (extension-less imports), so this loads it through vite's SSR module loader with the same aliases as
 * vitest.config.ts. `vite` is vitest's own engine, not a new dependency.
 *
 *   node tools/build-seo.mjs --fixture                       (the committed 43-water fixture release)
 *   node tools/build-seo.mjs --release-dir <release folder>  (a cut release; run after the bundle is cut)
 */
import { fileURLToPath } from "node:url";
import { createServer } from "vite";

const app = fileURLToPath(new URL("..", import.meta.url));
const pkg = (name, entry = "src/index.ts") => `${app}packages/${name}/${entry}`;
// releaseDir.ts (a node-only driver) reads `__dirname`, which vitest provides and an ESM module runner does not
globalThis.__dirname = `${app}packages/data/src/sql/drivers`;
const server = await createServer({
  root: app, configFile: false, logLevel: "error", appType: "custom", server: { middlewareMode: true, hmr: false, watch: null },
  resolve: { alias: [
    { find: "@app/core", replacement: pkg("core") },
    { find: "@app/data/release/node", replacement: pkg("data", "src/sql/drivers/releaseDir.ts") },
    { find: "@app/data/regs", replacement: pkg("data", "src/regs/index.ts") },
  ] },
  ssr: { noExternal: [/^@app\//] },
});
try {
  const { main } = await server.ssrLoadModule(`${app}tools/seo/build.ts`);
  await main(process.argv.slice(2));
} finally {
  await server.close();
}
