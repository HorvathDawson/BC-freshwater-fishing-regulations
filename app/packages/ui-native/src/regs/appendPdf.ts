/**
 * T354 — the row highlighted IN PLACE, without reading the book: a PDF incremental update. A PDF may be extended by
 * appending objects and a new cross-reference section that points back (/Prev) at the old one; a viewer reads the last
 * section first, so the appended objects win. The append here is a few hundred bytes:
 *   - one /Highlight per box, as `highlightPdf` draws them;
 *   - each marked page's /Annots array again (the shipped copy makes every page's an indirect array), its own refs
 *     followed by the new ones; the page itself is never rewritten;
 *   - an xref STREAM (the book's own xref is one) whose dictionary is the new trailer.
 * Everything it needs from the file is the release's index (`synopsis.append`, pipeline/deliver/release/synopsis.py),
 * so the phone copies the held file natively and writes these bytes on the end: no 24 MB on the JS heap (T351/T360).
 * The in-app viewer (`PdfViewer.native.tsx`) opens it at the page. Pure: index in, bytes out.
 */
import { highlightGeometry, type SourceBox } from "@app/core";
import type { SynopsisAppend } from "@app/data";

/** The index format this append reads; any other is refused (thrown). */
export const APPEND_V = 2;
/** As `highlightPdf`: yellow, multiplied without an appearance stream by pdfium and PDFKit. */
const COLOUR = "[1 0.9 0.2]";

const num = (x: number) => String(Math.round(x * 100) / 100);
const nums = (xs: readonly number[]) => `[${xs.map(num).join(" ")}]`;
const ref = ([n, g]: readonly [number, number]) => `${n} ${g} R`;

/**
 * The bytes to write after the shipped synopsis (`bytes` long, the size the index was cut from) so every box shows
 * highlighted on its own page. Null only when no box fits a page (nothing to mark). An index this does not read, or a
 * file it was not cut from, THROWS: never a book quietly unmarked.
 */
export function appendHighlight(ix: SynopsisAppend, bytes: number, boxes: readonly SourceBox[]): Uint8Array | null {
  if (ix.v !== APPEND_V) throw new Error(`synopsis: page index v${ix.v}, this app reads v${APPEND_V}`);
  if (!(bytes > ix.startxref)) throw new Error(`synopsis: ${bytes} bytes cannot hold the index's xref at ${ix.startxref}`);
  const n = ix.page.length;
  let next = ix.size;
  const objs: { at: [number, number]; body: string }[] = [];
  const added = new Map<number, string[]>();                      // page (1-based) -> its new annotations' refs
  for (const b of boxes) {
    const g = highlightGeometry(b);
    if (!g || !(b.pdf >= 1 && b.pdf <= n)) continue;
    const [pn, pg] = ix.page[b.pdf - 1]!;
    const a = next++;
    objs.push({ at: [a, 0], body: `<</Type/Annot/Subtype/Highlight/Rect ${nums(g.rect)}/QuadPoints ${nums(g.quadPoints)}`
      + `/C ${COLOUR}/CA 1/F 4/Border [0 0 0]/T (Source)/P ${pn} ${pg} R>>` });
    added.set(b.pdf, [...(added.get(b.pdf) ?? []), `${a} 0 R`]);
  }
  if (!added.size) return null;
  for (const [p, refs] of added) {
    const [, , an, ag, had] = ix.page[p - 1]!;
    objs.push({ at: [an, ag], body: `[${[had, ...refs].filter(Boolean).join(" ")}]` });
  }

  // the objects, each at its offset in the whole file (the book's bytes come first)
  let text = "\n";
  const offsets = new Map<number, [number, number]>();          // object -> [offset, generation]
  for (const o of objs) {
    offsets.set(o.at[0], [bytes + text.length, o.at[1]]);
    text += `${o.at[0]} ${o.at[1]} obj\n${o.body}\nendobj\n`;
  }
  const xref = next++;
  const xrefAt = bytes + text.length;
  offsets.set(xref, [xrefAt, 0]);

  // /W [1 4 2]: type 1 (in use, at an offset), a 4-byte offset, a 2-byte generation; /Index runs of consecutive numbers
  const order = [...offsets.keys()].sort((a, b) => a - b);
  const runs: [number, number][] = [];
  for (const k of order) {
    const last = runs[runs.length - 1];
    if (last && last[0] + last[1] === k) last[1]++; else runs.push([k, 1]);
  }
  const data = new Uint8Array(order.length * 7);
  order.forEach((k, i) => {
    const [off, gen] = offsets.get(k)!;
    data.set([1, (off >>> 24) & 255, (off >>> 16) & 255, (off >>> 8) & 255, off & 255, (gen >>> 8) & 255, gen & 255], i * 7);
  });
  const dict = `<</Type/XRef/Size ${next}/W [1 4 2]/Index [${runs.map(([a, c]) => `${a} ${c}`).join(" ")}]`
    + `/Root ${ref(ix.root)}${ix.info ? `/Info ${ref(ix.info)}` : ""}${ix.id ? `/ID [<${ix.id[0]}><${ix.id[1]}>]` : ""}`
    + `/Prev ${ix.startxref}/Length ${data.length}>>`;
  const head = `${text}${xref} 0 obj\n${dict}\nstream\n`;
  const tail = `\nendstream\nendobj\nstartxref\n${xrefAt}\n%%EOF\n`;
  const out = new Uint8Array(head.length + data.length + tail.length);
  out.set(ascii(head), 0);
  out.set(data, head.length);
  out.set(ascii(tail), head.length + data.length);
  return out;
}

/** The append is PDF syntax and refs only: every char is one byte. */
function ascii(s: string): Uint8Array {
  const b = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) b[i] = s.charCodeAt(i) & 255;
  return b;
}
