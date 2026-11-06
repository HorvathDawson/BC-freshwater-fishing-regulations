/** Escape closes a surface on the web (escapeCloses.web.ts); a no-op on a phone (escapeCloses.native.ts). */
export declare function useEscapeCloses(active: boolean, onClose: () => void): void;
/** Every Escape calls `onEscape` while `active`, on the web (escapeCloses.web.ts); a no-op on a phone. */
export declare function useEscapeKey(active: boolean, onEscape: () => void): void;
