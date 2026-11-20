/**
 * T279 — A RENDER THREAD's work: the water pages the main thread asks for, one at a time — the pane rendered (pane.tsx),
 * the app's document around it (prerender.ts), the page checked (site.ts `checkWaterPage`). The main thread gets the page's
 * HTML, the facts the budget reads of it and its problems back, and at the end this thread's part of the shared stylesheet.
 * (T917) The page is compressed and written by a job thread (jobs.ts), not here: that was 31 % of this thread's time.
 */
import type { MessagePort } from "node:worker_threads";
import type { ItemId } from "@app/data";
import { makeReleaseSource } from "@app/data/release";
import { openReleaseDir } from "@app/data/release/node";
import type { FishImages } from "@app/ui-native";
import { readAppExport } from "./appExport";
import { pageFacts } from "./compressed";
import { PaneRenderer } from "./pane";
import { appDocument, type AppDocument } from "./prerender";
import { checkWaterPage } from "./site";

export interface ThreadData {
  readonly releaseDir: string; readonly appDir: string; readonly out: string; readonly origin: string;
  readonly doc: Pick<AppDocument, "critical" | "cssHref" | "preload">;
}
export type ToThread = { t: "page"; n: number; id: string; region: string | null; head: string; file: string } | { t: "css" } | { t: "stop" };

/** @public — loaded by tools/seo/pane-worker.mjs through the module runner (a dynamic load knip cannot follow). */
export async function serve(port: MessagePort, d: ThreadData): Promise<void> {
  const loader = openReleaseDir(d.releaseDir), app = readAppExport(d.appDir);
  // (T405) a picture the app requires but its export does not serve is an error, never a pane drawn without it
  (globalThis as { __ciftAsset?: (p: string) => unknown }).__ciftAsset = (p: string) => {
    const a = app.asset(p);
    if (!a) throw new Error(`build-seo: the app export ${d.appDir} serves no ${p} (rebuild the export)`);
    return a;
  };
  const images = await import("../../apps/mobile/src/fishImages.generated");
  const panes = new PaneRenderer({ source: makeReleaseSource(loader), loader,
                                   fishImages: images.FISH_IMAGES as FishImages });
  let chain = Promise.resolve();
  port.on("message", (m: ToThread) => {
    chain = chain.then(async () => {
      if (m.t === "stop") { port.close(); return; }
      if (m.t === "css") { port.postMessage({ t: "css", parts: panes.parts() }); return; }
      try {
        const html = appDocument(app.template, { ...d.doc, head: m.head, item: m.id, region: m.region, pane: await panes.water(m.id as ItemId) });
        port.postMessage({ t: "done", n: m.n, html, facts: pageFacts(m.file, html), problems: checkWaterPage(html, d.origin) });
      } catch (e) {
        port.postMessage({ t: "done", n: m.n, error: e instanceof Error ? e.stack ?? e.message : String(e) });
      }
    });
  });
  port.postMessage({ t: "ready" });
}
