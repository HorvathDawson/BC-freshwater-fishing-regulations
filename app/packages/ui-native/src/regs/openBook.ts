/**
 * T43 — THE one way a "Rule book p.N ↗" link opens (decisions D6 a), for every caller: the regs page's lines, its sheets,
 * the menu's glossary. On web the synopsis PDF at its PDF page (`#page=<pdf>`); on iOS / Android the PDF URL with NO
 * fragment (a native viewer ignores it) — the link's label already names the PRINTED page. The URL is core's `bookUrl`.
 *
 * T215 (owner 2026-10-27, option B — the same on web and phone): a link that carries the entry's `boxes` and a release that
 * ships our copy of the PDF (`setSynopsisSource`) open THAT copy with a highlight annotation on the passage
 * (`synopsisPdf` / `synopsisPdf.web`). No boxes, no copy, or any failure: the plain link above, as before.
 */
import { Linking, Platform } from "react-native";
import { bookUrl, type SourceBox } from "@app/core";
import { openHighlighted } from "./synopsisPdf";
import { getSynopsisSource } from "./synopsisSource";

/** Open the synopsis for a book run's PDF page. A refused open (no PDF handler) is logged, never thrown or alerted. */
export function openBook(pdf: number, boxes?: readonly SourceBox[], os: string = Platform.OS,
                         open: typeof openHighlighted = openHighlighted): Promise<void> {
  const src = getSynopsisSource();
  const plain = () => Linking.openURL(bookUrl(pdf, os)).then(() => undefined, (e: unknown) => {
    console.warn(`openBook: could not open the synopsis (PDF page ${pdf})`, e);
  });
  if (!src || !boxes?.length) return plain();
  return open(src, pdf, boxes).catch((e: unknown) => {
    console.warn(`openBook: could not highlight (PDF page ${pdf}); opening the plain link`, e);
    return plain();
  });
}
