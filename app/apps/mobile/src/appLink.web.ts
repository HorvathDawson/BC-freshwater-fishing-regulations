/** (T625) The web has no app links: the browser already is the page a link names (`startLink.web.ts` reads it). */
import type { View } from "@app/ui";

export function onAppLink(_open: (r: { item: string; view: View }) => void): () => void { return () => {}; }
