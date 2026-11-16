// (T757 item 7) THE FEED SHAPE THE FEEDS WORKER ABOUT TO BE DEPLOYED SERVES: `DFO_SHAPE` exported by
// app/workers/feeds/src/status.ts (what its /status.json and the feed index say as `dfo`), read by EVALUATING that
// module (bundled with the app's esbuild), never by grepping its text.
//
//   node deploy/lib/worker-shape.mjs [status.ts]
//
// Prints one line of JSON: the shape (`{"pieces":1}`), or `null` when the module exports no DFO_SHAPE (a worker from
// before T757). Exit 1, saying why, when the module cannot be bundled or evaluated: the caller refuses then
// (`pipeline.deliver.release.pieces_gate --worker-deploy`).
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import path from "node:path";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const entry = path.resolve(process.argv[2] ?? path.join(root, "app/workers/feeds/src/status.ts"));
try {
  const esbuild = createRequire(path.join(root, "app/package.json"))("esbuild");
  const out = esbuild.buildSync({ entryPoints: [entry], bundle: true, write: false, format: "esm", platform: "neutral",
                                  target: "es2022", logLevel: "silent" });
  const mod = await import(`data:text/javascript;base64,${Buffer.from(out.outputFiles[0].text).toString("base64")}`);
  const shape = mod.DFO_SHAPE;
  if (shape !== undefined && (!shape || typeof shape !== "object" || Array.isArray(shape)))
    throw new Error(`DFO_SHAPE is ${JSON.stringify(shape)}, not an object`);
  console.log(JSON.stringify(shape ?? null));
} catch (e) {
  console.error(`worker-shape: ${entry} could not be read: ${e instanceof Error ? e.message : String(e)}`);
  process.exit(1);
}
