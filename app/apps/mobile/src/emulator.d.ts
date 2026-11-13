/**
 * (T603 item 5) Is this run on an Android EMULATOR? `emulator.native.ts` reads the build's own facts; `emulator.web.ts`
 * is false (a browser is not one). Both are checked against this one signature.
 */
export declare function onEmulator(): boolean;
