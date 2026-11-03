/**
 * (T369, owner 2026-11-03) A LINK ADDRESSES A PLACE ON THE WATER, NOT A PART NUMBER. `part=N` is an index into one
 * release's parts, and a re-split moves it; a place — the FWA stream key and a distance along it (`@app/data`
 * `StreamPlace`) — is FWA's and does not. So a link carries the place the frame is on, made here from the release the
 * app holds, and a link opened is resolved here, in the release the app holds then: the section whose span holds the
 * distance → its water (named or not) → its part and stretch. Pure and offline: release reads only, no tiles, no
 * geometry. Inside the app the frame keeps its part index (one release); only the address carries the place.
 *
 * UNIVERSAL: a named stream's part or stretch (its midpoint section's middle), an UNNAMED reach (the tapped section's
 * middle; no item), a stream water of one part (no place: the item says it all), a lake and a split lake's closure
 * area (no place: the area's item id is the address — lakes have no route measure).
 *
 * THE PLACE OF A PART is the middle of its midpoint section (`part_geo.mid_sid`; a stretch's, `part_stretch.mid_sid`):
 * strictly inside the part, so the same release resolves it back to the same part and stretch, and a later release to
 * whichever part now holds that point.
 */
import type { ItemId, SectionId, WaterSource } from "@app/data";
import { lineMid, type RegsReads, type StreamPlace } from "@app/data/regs";
import { waterOfSection } from "./openWater";
import type { WaterFrame } from "./route";

export type { StreamPlace };
export type PlaceReads = Pick<RegsReads, "itemOrd" | "parts" | "navRows" | "sectionPart" | "sectionLine" | "sectionAt" | "hasLines">;
/** What a section's water is asked of (`openWater.ts` `waterOfSection`, the map tap's own resolver: one rule for both). */
export type ItemFor = Pick<WaterSource, "itemForSection">;

/** The middle of the first of `sids` that is a stream section (a lake's handle has no line); null: none is. Throws when
 *  the release has no lines at all: the frame needs a place the release cannot give (said, never hidden). */
async function midOf(store: PlaceReads, sids: readonly number[]): Promise<StreamPlace | null> {
  if (!(await store.hasLines()))
    throw new Error("place: this release has no section_line, so the part on screen cannot be put in its address");
  for (const s of sids) {
    const l = await store.sectionLine(s);
    if (l) return lineMid(l);
  }
  return null;
}

/**
 * THE PLACE A FRAME IS ON, for its address; null: the frame needs none (browsing, a whole water, a water of one part
 * and one stretch, a lake) or the release cannot say (cut before `section_line`).
 */
export async function placeOf(store: PlaceReads, f: WaterFrame | null): Promise<StreamPlace | null> {
  if (!f) return null;
  if (f.item === null || f.part === null) return f.tapSid !== null ? midOf(store, [f.tapSid]) : null;
  const ord = await store.itemOrd(f.item);
  if (ord === null) return null;
  const nav = await store.navRows(ord);
  const stretches = nav.stretches.filter((s) => s.partIx === f.part);
  if (nav.geo.length <= 1 && stretches.length <= 1) return null;          // one part, one stretch: the item is enough
  const st = stretches.length > 1 ? stretches.find((s) => s.stretch === (f.stretch ?? 0)) : undefined;
  const mid = st ? st.midSid : nav.geo.find((g) => g.partIx === f.part)?.midSid;
  const rest = st ? st.sids : nav.sections.filter((s) => s.partIx === f.part).map((s) => s.sid);
  return midOf(store, mid === undefined ? rest : [mid, ...rest]);
}

/** Where a place falls in this release: the section, its water (null: an unnamed reach) and that water's part / stretch. */
export interface PlaceAt { sid: SectionId; item: ItemId | null; part: number | null; stretch?: number }

/** THE PLACE RESOLVED in the release held; null: no section of this release spans it (or the release has no lines). */
export async function resolvePlace(store: PlaceReads, itemFor: ItemFor, p: StreamPlace): Promise<PlaceAt | null> {
  const h = await store.sectionAt(p.blk, p.m);
  if (h === null) return null;
  const sid = h as SectionId;
  const item = await waterOfSection(itemFor, sid);
  if (item === null) return { sid, item: null, part: null };
  const ord = await store.itemOrd(item);
  const part = ord === null ? null : await store.sectionPart(ord, h);
  if (ord === null || part === null) return { sid, item, part: null };
  const st = (await store.navRows(ord)).stretches.find((s) => s.partIx === part && s.sids.includes(h));
  return { sid, item, part, ...(st && st.stretch ? { stretch: st.stretch } : {}) };
}
