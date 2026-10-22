/**
 * (T41) The web's start link: the page's query string, read once at start. A bad value is a console error (the browser
 * walk fails on any) and the app opens as usual — never on some other page.
 */
import { parseStartLink, type StartLink } from "@app/ui";

export function startLink(): StartLink | null {
  try { return parseStartLink(window.location.search); }
  catch (e) { console.error(e instanceof Error ? e.message : String(e)); return null; }
}
