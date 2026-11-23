/** (T625) The web has no app links: the browser already is the page a link names (`startLink.web.ts` reads it). */
import type { AppLinkOpen } from "@app/ui";

export function onAppLink(_open: (r: AppLinkOpen) => void): () => void { return () => {}; }
