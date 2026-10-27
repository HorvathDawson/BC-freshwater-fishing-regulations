/** T215: open our copy of the synopsis with `boxes` highlighted; throws when it cannot (the caller falls back). `.native.ts` is the phone's, `.web.ts` the browser's. */
import type { SourceBox } from "@app/core";
import type { SynopsisSource } from "./synopsisSource";
export declare function loadSynopsis(s: SynopsisSource, fetchImpl?: typeof fetch): Promise<Uint8Array>;
export declare function openHighlighted(s: SynopsisSource, pdf: number, boxes: readonly SourceBox[]): Promise<void>;
