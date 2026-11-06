/**
 * (T282) How a part's code is asked for. The web (`partAsk.web.ts`): when drawn or idle, and with Metro's `ErrorUtils`
 * out of the way (`unguarded`, T391). A phone (`partAsk.native.ts`): the bundle is one file, so at once and plainly.
 */
export declare const ASK_AT_ONCE: boolean;
export declare function askPart<T>(load: () => Promise<T>): Promise<T>;
