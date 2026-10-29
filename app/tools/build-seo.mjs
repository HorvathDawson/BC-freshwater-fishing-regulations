#!/usr/bin/env node
/**
 * T193 - the SEO static-site build. The page logic is TypeScript over the workspace packages (tools/seo/), loaded through
 * tools/seo/runner.mjs (vite-node, as vitest loads it). (T279) The water pages' renders run on render threads
 * (tools/seo/pane-worker.mjs, `--threads`, at most 4), each with its own runner.
 *
 *   node tools/build-seo.mjs --fixture                                          (the committed 43-water fixture release)
 *   node tools/build-seo.mjs --release-dir <release folder> --app <web export>  (a cut release and the app built for it)
 */
import { makeRunner } from "./seo/runner.mjs";

const r = await makeRunner();
try {
  const { main } = await r.run(`${r.app}tools/seo/build.ts`);
  await main(process.argv.slice(2));
} finally {
  await r.close();
}
