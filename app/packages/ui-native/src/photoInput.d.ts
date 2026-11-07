/**
 * The shared declaration for `./photoInput`: `photoInput.web.ts` listens for the browser's wheel (a mouse wheel and a
 * trackpad pinch, which arrives as ctrl+wheel); `photoInput.native.ts` has neither. T454.
 */
import type { RefObject } from "react";

export declare function usePhotoWheel(ref: RefObject<unknown>, onWheel: (x: number, y: number, deltaY: number) => void,
                                      enabled: boolean): void;
