/**
 * (T436) THE PLATFORM LINE a report, a crash header and the version headers carry: "web" on the web, the phone's OS and
 * its version on a phone ("ios 18.1", "android 34"). One function, so the five places that sent it cannot spell it
 * differently (`platformLine.web.ts` / `platformLine.native.ts`).
 */
export declare function platformLine(): string;
