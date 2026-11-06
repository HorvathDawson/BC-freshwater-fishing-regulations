/**
 * Say `text` aloud through the phone's screen reader (TalkBack / VoiceOver), `announce.native.ts`. The web has nothing
 * to call: a live region on the page says it (`announce.web.ts` is a no-op).
 */
export declare function announce(text: string): void;
