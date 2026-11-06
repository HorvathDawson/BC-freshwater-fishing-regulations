/**
 * The browser window's own size, read directly (`window.innerWidth` / `innerHeight`) — for the moments React Native's
 * `useWindowDimensions` has nothing yet (a document that reports no size, a test's). `windowSize.web.ts` reads it;
 * `windowSize.native.ts` has no browser window and answers null.
 */
export declare function windowSize(): { w: number; h: number } | null;
