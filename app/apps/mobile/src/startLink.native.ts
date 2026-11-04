/** (T41) No start link on native: navigation stays in the Shell (D1), no route ships. */
import type { WaterSource } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import type { RegsStore } from "@app/data/regs";
import type { StartLink } from "@app/ui";

export function startLink(): StartLink | null { return null; }

/** (T369) Links are web-only: a phone has no start link to resolve, so being asked is a bug, said. */
export function resolveStart(_link: StartLink, _store: RegsStore, _loader: ReleaseLoader,
                             _itemFor: Pick<WaterSource, "itemForSection">): Promise<StartLink> {
  return Promise.reject(new Error("start link: native has no start link to resolve (links are web-only)"));
}
