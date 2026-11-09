/**
 * (T466) THE WALKTHROUGH'S CLOSED EXAMPLE, FOUND IN THE RELEASE THE APP HOLDS: `GUIDE_CLOSED` is a water's name and the
 * stream key + start metre of one of its part's runs (`part_line`), never a raw id (rule 46). The part that run belongs
 * to is the example only when the release says it is closed all year (`part.closed_all_year`); anything else — the water
 * not in this release, no `part_line` in it, the run moved, the part open some of the year — is NO example (null), and
 * the walk says so instead of pointing at a part that is not closed.
 */
import { useEffect, useState } from "react";
import { GUIDE_CLOSED, reportFailure, spanName } from "@app/core";
import type { ItemId, WaterSource } from "@app/data";
import type { RegsLoader, RegsReads } from "@app/data/regs";
import { loadRegsStore } from "@app/ui";

export interface GuideClosed {
  /** the water (its durable item id) and its name */
  readonly item: string;
  readonly name: string;
  /** the part's index in the water (what the frame and the water map name a part by) */
  readonly part: number;
  /** the part's short name, as the water map's bar shows it */
  readonly label: string;
}

export async function findGuideClosed(source: Pick<WaterSource, "searchNames">,
                                      store: Pick<RegsReads, "itemOrd" | "parts" | "partLines" | "statics">): Promise<GuideClosed | null> {
  const hits = await source.searchNames(GUIDE_CLOSED.name, 10);
  const hit = hits.find((h) => h.name === GUIDE_CLOSED.name && h.kind === GUIDE_CLOSED.kind);
  if (!hit) return null;
  const ord = await store.itemOrd(hit.item as ItemId);
  if (ord === null) return null;
  const runs = await store.partLines(ord);
  const run = runs.find((r) => r.blk === GUIDE_CLOSED.blk && r.m0 === GUIDE_CLOSED.from);
  if (!run) return null;
  const facts = (await store.parts(ord))[run.partIx]?.facts;
  if (!facts?.closed_all_year) return null;
  // (T517) the part's name, composed by the one name function from its end tokens (as the map's bar says it)
  return { item: hit.item, name: hit.name, part: run.partIx, label: spanName((await store.statics()).ends, hit.name, facts) };
}

/**
 * The closed example of the release the app holds, read once the walkthrough is due or replayed (`wanted`: the regs store
 * is not opened on a first paint for it). undefined: still reading; null: none (or no release, or the read failed —
 * reported, and the walk says it has no example).
 */
export function useGuideClosed(source: Pick<WaterSource, "searchNames">, loader: RegsLoader | null,
                               wanted: boolean): GuideClosed | null | undefined {
  const [found, setFound] = useState<GuideClosed | null | undefined>(undefined);
  useEffect(() => {
    if (!wanted || !loader) return;
    let live = true;
    void loadRegsStore(loader).then((store) => findGuideClosed(source, store))
      .then((c) => { if (live) setFound(c); })
      .catch((e: unknown) => { reportFailure(e); if (live) setFound(null); });
    return () => { live = false; };
  }, [source, loader, wanted]);
  return loader ? found : null;
}
