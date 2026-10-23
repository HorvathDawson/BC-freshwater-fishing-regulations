/** Closing search keeps the focus in its field on the web (searchFocus.web.ts); a phone drops it (searchFocus.native.ts). */
export declare const CLOSE_KEEPS_FOCUS: boolean;
/** The props that let a click on a focused-but-closed field open search (web: `onClick`; a phone: none). */
export declare function closedFieldClick(open: () => void): object;
