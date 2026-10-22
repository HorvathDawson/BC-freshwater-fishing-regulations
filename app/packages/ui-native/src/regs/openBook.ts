/**
 * T43 — THE one way a "Rule book p.N ↗" link opens (decisions D6 a), for every caller: the regs page's lines, its sheets,
 * the menu's glossary. On web the synopsis PDF at its PDF page (`#page=<pdf>`); on iOS / Android the PDF URL with NO
 * fragment (a native viewer ignores it) — the link's label already names the PRINTED page, so the reader knows where to
 * look. The URL is core's `bookUrl`; this file only adds the platform and the open. If D6 becomes (b) (an in-app viewer
 * honouring the page), only this file changes: every caller already passes the PDF page.
 */
import { Linking, Platform } from "react-native";
import { bookUrl } from "@app/core";

/** Open the synopsis for a book run's PDF page. A refused open (no PDF handler) is logged, never thrown or alerted. */
export function openBook(pdf: number, os: string = Platform.OS): Promise<void> {
  return Linking.openURL(bookUrl(pdf, os)).then(() => undefined, (e: unknown) => {
    console.warn(`openBook: could not open the synopsis (PDF page ${pdf})`, e);
  });
}
