// T225: run a parity/timing entry (.ts) under node — bundled by app/node_modules' esbuild into a temp .mjs, then
// imported. `node app/workers/feeds/parity/gauges/run.mjs parity/gauges/parity.ts [args...]`
import { createRequire } from "node:module";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { pathToFileURL, fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const feeds = resolve(here, "../..");
const require = createRequire(join(feeds, "../../package.json"));
const esbuild = require("esbuild");
const entry = resolve(feeds, process.argv[2]);
const dir = mkdtempSync(join(tmpdir(), "feeds-parity-"));
const out = join(dir, "entry.mjs");
await esbuild.build({
  entryPoints: [entry], bundle: true, platform: "node", format: "esm", target: "node22", outfile: out, logLevel: "warning",
  banner: { js: `import { createRequire as __cr } from "node:module"; const require = __cr(import.meta.url);` },
  define: { "import.meta.url": JSON.stringify(pathToFileURL(entry).href) },
});
try {
  process.argv.splice(2, 1);
  await import(pathToFileURL(out).href);
} finally {
  rmSync(dir, { recursive: true, force: true });
}
