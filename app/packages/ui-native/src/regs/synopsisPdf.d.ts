/** T215: open our copy of the synopsis with `boxes` highlighted; throws when it cannot (the caller falls back). `.native.ts` is the phone's, `.web.ts` the browser's. */
import type { SourceBox } from "@app/core";
import type { SynopsisSource } from "./synopsisSource";
import type { LoadHooks } from "./synopsisLoad";
export declare function loadSynopsis(s: SynopsisSource, fetchImpl?: typeof fetch, hooks?: Partial<LoadHooks>): Promise<Uint8Array>;
export declare function openHighlighted(s: SynopsisSource, pdf: number, boxes: readonly SourceBox[], hooks?: Partial<LoadHooks>): Promise<void>;
