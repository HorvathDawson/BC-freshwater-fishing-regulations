/**
 * The shared declaration for `./startLink` (T41): the web's start link — `?regs=<item>&part=&date=&tab=&sheet=&age=&res=
 * &theme=&scroll=`, a TEST AFFORDANCE that opens one water's Regulations page straight away (`@app/ui` `parseStartLink`).
 * `startLink.web.ts` reads the page's query string once; `startLink.native.ts` has none (D1: no route ships on native).
 * Bundlers pick by platform extension, TypeScript does not.
 */
import type { StartLink } from "@app/ui";

export declare function startLink(): StartLink | null;
