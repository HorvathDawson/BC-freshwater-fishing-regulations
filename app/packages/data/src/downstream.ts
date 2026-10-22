/**
 * (T53) Where an unnamed reach flows: the first NAMED water down its flow pointers. Geography only — the Regulations
 * header's "Tributary of …" line (core `downstreamLabel` words it); no regulation is read from it.
 */
import type { ItemId, RegsSource, SectionId } from "./index";

/** How many sections down an unnamed reach's flow pointers are walked looking for a named water. */
export const DOWNSTREAM_WALK = 50;

/**
 * Down `section`'s flow pointers (`traceToGauge`: the bundle's `section_down`), past unnamed sections, to the first
 * section that belongs to a water (`waterFor`). Null when the pointers end (they exist only inside a gauge's watershed)
 * or nothing named lies within `bound` sections. The tapped section itself is never asked.
 */
export async function downstreamWater(source: Pick<RegsSource, "traceToGauge" | "waterFor">, section: SectionId,
                                      bound: number = DOWNSTREAM_WALK): Promise<{ item: ItemId; name: string; kind: string } | null> {
  const path = await source.traceToGauge(section);
  for (const s of path.slice(1, 1 + bound)) {
    const w = await source.waterFor(s);
    if (w) return w;
  }
  return null;
}
