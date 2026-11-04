/**
 * (T369, owner 2026-11-03) ONE LOAD PATH PER SCREEN: what a water frame SHOWS is derived from the frame (the route) and
 * the release by the functions here, and by nothing a way in carries beside them. A tap, a search Open, a step, a
 * parts-list pick, a flow, back, a reload of the address — each ends in a frame, and the same frame draws the same:
 *
 *   framePart     the part the frame is on: its `part`, else the part the Regulations view opens for its tap
 *                 (`targetFor`: `part_section`, else the section's key, else the first part with one)
 *   frameStretch  the RELEASE's stretch of that part (`part_stretch.stretch_ix`): the frame's, else the tap's, else 0
 *   frameSection  the section the Conditions view is about: the tap (it lies in the part: the route drops a tap the
 *                 moment the part moves), else the stretch's midpoint section, else the part's (`mid_sid`), else the
 *                 part's first section — never the water's FIRST section (`sections[0]`, the old answer)
 *   frameHere     the Conditions view's "you are here": the tap's point, else (owner 2026-11-04) the place a link or a
 *                 non-map open names — the part's / stretch's midpoint (`part_geo` / `part_stretch` `mid`), which lies
 *                 on that midpoint section
 *
 * `useFrameView` reads the release for them (the store's `targetFor` and `navRows`, both asked once per store), and
 * the three views of the water — Regulations (`WaterScreen`), Conditions (`WaterConditions`) and Map (`useWaterMap`)
 * — read their part / section / point from it alone. `regsPartOf` is the same answer for the opener: a tapped water
 * opens AT its part (T369 D3), so no view ever has to adopt one later.
 */
import type { RegsTarget } from "@app/core";
import type { ItemId, SectionId } from "@app/data";
import { targetFor, type NavRows, type RegsLoader, type RegsStore } from "@app/data/regs";
import { useAsync, type Async } from "./async";
import { loadRegsStore, useRegsTarget } from "./regs";
import type { LatLon, WaterFrame } from "./route";

const ids = new WeakMap<object, number>();
let nextId = 0;
const idOf = (o: object): number => { let i = ids.get(o); if (i === undefined) ids.set(o, i = ++nextId); return i; };

/** What of a water's `navRows` the frame's section and point are read from. */
export type FrameNav = Pick<NavRows, "sections" | "geo" | "stretches">;
/** The frame's fields that decide what it shows (`view` decides only which view is in front). */
export type FrameKey = Pick<WaterFrame, "item" | "part" | "tapSid" | "at"> & { readonly stretch?: number };

/** The release's stretch of `part` the frame is on: the frame's own, else the one holding its tap, else the first. */
export function frameStretch(f: FrameKey, part: number | null, nav: FrameNav | null): number {
  if (f.stretch) return f.stretch;
  if (part === null || f.tapSid === null || !nav) return 0;
  const tap = Number(f.tapSid);
  return nav.stretches.find((s) => s.partIx === part && s.sids.includes(tap))?.stretch ?? 0;
}

/** The middle of the frame's part / stretch: its midpoint section and the point on it (null: the release has none). */
function middleOf(part: number, stretch: number, nav: FrameNav): { sid: number; at: LatLon | null } | null {
  const st = nav.stretches.filter((s) => s.partIx === part);
  const s = st.length > 1 ? st.find((x) => x.stretch === stretch) : undefined;
  const g = s ?? nav.geo.find((x) => x.partIx === part);
  if (g) return { sid: g.midSid, at: g.mid ? { lon: g.mid[0], lat: g.mid[1] } : null };
  const first = nav.sections.find((x) => x.partIx === part);
  return first ? { sid: first.sid, at: null } : null;
}

/** THE SECTION A FRAME IS ABOUT (the Conditions view's reach). Null: not known yet (no release read). */
export function frameSection(f: FrameKey, part: number | null, nav: FrameNav | null): SectionId | null {
  if (f.item === null) return f.tapSid;
  if (f.tapSid !== null && (part === null || !nav || nav.sections.some((s) => s.sid === Number(f.tapSid) && s.partIx === part)))
    return f.tapSid;
  if (part === null || !nav) return null;
  const m = middleOf(part, frameStretch(f, part, nav), nav);
  return m ? (m.sid as SectionId) : null;
}

/** THE "YOU ARE HERE" OF A FRAME: the tap's point on the tapped section, else the part's / stretch's middle. */
export function frameHere(f: FrameKey, part: number | null, nav: FrameNav | null): LatLon | null {
  const section = frameSection(f, part, nav);
  if (section === null) return null;
  if (f.tapSid !== null && section === f.tapSid) return f.at;
  if (part === null || !nav) return null;
  const m = middleOf(part, frameStretch(f, part, nav), nav);
  return m && m.sid === Number(section) ? m.at : null;
}

/** What a frame shows, read once from the release. */
export interface FrameView {
  /** The Regulations view's answer: the part (named water) or the section (unnamed reach). */
  readonly target: RegsTarget | null;
  readonly part: number | null;
  readonly stretch: number;
  readonly section: SectionId | null;
  readonly here: LatLon | null;
}

/**
 * THE ONE FUNCTION over the release (the hook form): the target exactly as the Regulations view asks it (`useRegsTarget`,
 * answered once per store), then the water's `navRows` for the section and the point. `store` null (no release): ready
 * with what the frame alone says (its tap and point; no part's middle to fall back on).
 */
export function useFrameView(store: RegsStore | null, f: FrameKey): Async<FrameView> {
  const found = useRegsTarget(f.part === null ? store : null, f.item, f.tapSid);
  const target: RegsTarget | null = f.part !== null && f.item !== null ? { kind: "part", item: f.item, partIx: f.part }
    : found.value ?? null;
  const part = target?.kind === "part" ? target.partIx : null;
  const navKey = `frame-nav:${store ? idOf(store) : ""}:${f.item ?? ""}`;
  const nav = useAsync(async () => {
    const ord = await store!.itemOrd(f.item!);
    return ord === null ? null : store!.navRows(ord);
  }, navKey, !!store && f.item !== null);
  if (store && f.part === null && f.item !== null && found.state !== "ready")
    return found.state === "failed" ? { state: "failed", value: null, error: found.error } : { state: "loading", value: null, error: null };
  if (store && f.item !== null && nav.state !== "ready")
    return nav.state === "failed" ? { state: "failed", value: null, error: nav.error } : { state: "loading", value: null, error: null };
  const n = nav.value ?? null;
  const unnamed: RegsTarget | null = f.item === null && f.tapSid !== null ? { kind: "section", handle: f.tapSid } : null;
  return { state: "ready", error: null,
           value: { target: f.item === null ? unnamed : target, part, stretch: frameStretch(f, part, n),
                    section: frameSection(f, part, n), here: frameHere(f, part, n) } };
}

/**
 * (T369 D3) THE PART A TAPPED SECTION OPENS, for the opener (`OpenerOptions.partOf`): the Regulations view's own answer
 * (`targetFor`) and the release's stretch holding the section — so a tapped frame carries its part from the moment it
 * opens, and the Regulations, Conditions and Map views (and the address) never see a part-less tapped frame.
 */
export function regsPartOf(loader: RegsLoader): (item: ItemId, sid: SectionId) => Promise<{ part: number; stretch?: number } | null> {
  return async (item, sid) => {
    const store = await loadRegsStore(loader);
    return partOfTap(store, item, sid);
  };
}

/** `regsPartOf` over an open store. */
export async function partOfTap(store: RegsStore, item: ItemId,
                                sid: SectionId): Promise<{ part: number; stretch?: number } | null> {
  const t = await targetFor(store, item, Number(sid));
  if (t.kind !== "part") return null;
  const ord = await store.itemOrd(item);
  const nav = ord === null ? null : await store.navRows(ord);
  const stretch = frameStretch({ item, part: t.partIx, tapSid: sid, at: null }, t.partIx, nav);
  return { part: t.partIx, ...(stretch ? { stretch } : {}) };
}
