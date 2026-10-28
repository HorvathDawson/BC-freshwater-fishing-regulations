/**
 * T43 — THE one way a "Rule book p.N ↗" link opens (decisions D6 a), for every caller: the regs page's lines, its sheets,
 * the menu's glossary. On web the synopsis PDF at its PDF page (`#page=<pdf>`); on iOS / Android the PDF URL with NO
 * fragment (a native viewer ignores it) — the link's label already names the PRINTED page. The URL is core's `bookUrl`.
 *
 * T215 (owner 2026-10-27, option B — the same on web and phone): a link that carries the entry's `boxes` and a release that
 * ships our copy of the PDF (`setSynopsisSource`) open THAT copy with a highlight annotation on the passage
 * (`synopsisPdf` / `synopsisPdf.web`). No copy in the release: the plain link above (a link with no box opens our copy at its page, unmarked). A failure shows Retry + the plain link on the loading card.
 */
import { Linking, Platform } from "react-native";
import { bookUrl, type SourceBox } from "@app/core";
import { openHighlighted } from "./synopsisPdf";
import { getSynopsisSource } from "./synopsisSource";
import { runSynopsisLoad } from "./synopsisLoad";

/** Open the synopsis for a book run's PDF page. A refused open (no PDF handler) is logged, never thrown or alerted. */
export function openBook(pdf: number, boxes?: readonly SourceBox[], os: string = Platform.OS,
                         open: typeof openHighlighted = openHighlighted): Promise<void> {
  const src = getSynopsisSource();
  const plain = () => Linking.openURL(bookUrl(pdf, os)).then(() => undefined, (e: unknown) => {
    console.warn(`openBook: could not open the synopsis (PDF page ${pdf})`, e);
  });
  // (owner 2026-10-28) OUR copy from the release, highlighted or not; gov.bc.ca only when the release names none
  if (!src) return plain();
  // (owner 2026-10-28) one download at a time, with progress + cancel (`synopsisLoad`); a failure shows Retry and the plain
  // link on the loading card rather than silently opening gov.bc.ca. A press while one is loading is ignored.
  const bx = boxes ?? [];
  const run = runSynopsisLoad(pdf, bx, (h) => open(src, pdf, bx, h));
  if (!run) return Promise.resolve();
  return run.catch((e: unknown) => {
    console.warn(`openBook: could not open the highlighted synopsis (PDF page ${pdf})`, e);
  });
}
