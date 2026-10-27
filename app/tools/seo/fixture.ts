/** The SEO tests' shared fixture: the committed regs fixture release (43 waters, not the province) on a fixed day. */
import { openRegsStore, type RegsStore } from "@app/data/regs";
import { openRegsFixture } from "@app/data/release/node";
import { buildDayOf } from "./model";

/** Tuesday 2026-10-27, noon local. */
export const TODAY_DATE = new Date(2026, 9, 27, 12);
export const TODAY = buildDayOf(TODAY_DATE);

export async function openFixture() {
  const loader = openRegsFixture();
  const store: RegsStore = await openRegsStore(loader);
  return { loader, store };
}
