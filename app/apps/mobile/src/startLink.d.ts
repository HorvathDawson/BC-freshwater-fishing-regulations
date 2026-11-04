/**
 * The shared declaration for `./startLink` (T41): the web's start link — `?regs=<item>&blk=&m=&date=&tab=&sheet=&age=&res=
 * &theme=&scroll=` (T369: `blk` + `m`, a place, resolved in the release once open), a TEST AFFORDANCE that opens one water's Regulations page straight away (`@app/ui` `parseStartLink`).
 * `startLink.web.ts` reads the page's query string once; `startLink.native.ts` has none (D1: no route ships on native).
 * Bundlers pick by platform extension, TypeScript does not.
 */
import type { WaterSource } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import type { RegsStore } from "@app/data/regs";
import type { StartLink } from "@app/ui";

export declare function startLink(): StartLink | null;

/**
 * (T369) The start link resolved in the open release (`@app/ui/place` `resolveStartLink`, web-only: a place → its
 * section → its water and part here; a water of one part named alone → that part). Native: never asked (it has no start
 * link), and refuses.
 */
export declare function resolveStart(link: StartLink, store: RegsStore, loader: ReleaseLoader,
                                     itemFor: Pick<WaterSource, "itemForSection">): Promise<StartLink>;
