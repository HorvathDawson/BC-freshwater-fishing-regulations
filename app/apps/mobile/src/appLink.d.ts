/**
 * The shared declaration for `./appLink` (T625): a tapped share link, handed to the app. `appLink.native.ts` listens for
 * the launch URL and `url` events (Android App Links); `appLink.web.ts` has none (the browser is the page). Returns the stop.
 */
import type { View } from "@app/ui";

export declare function onAppLink(open: (r: { item: string; view: View }) => void): () => void;
