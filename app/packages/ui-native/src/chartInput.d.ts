/**
 * The shared declaration for `./chartInput`: `chartInput.web.ts` listens for the browser's wheel and asks whether the
 * pointer is a finger; `chartInput.native.ts` has neither. Bundlers pick by platform extension; TypeScript does not,
 * so both are checked against this one signature.
 */
import type { RefObject } from "react";

export declare function useChartWheel(ref: RefObject<unknown>, onWheel: (x: number, deltaY: number) => boolean,
                                      enabled: boolean): void;
export declare function useChartHover(ref: RefObject<unknown>, onHover: (x: number | null) => void,
                                      enabled: boolean): void;
export declare function chartPointer(): "touch" | "pointer";
