import { useState } from "react";

/** What `/api/synopsis/ref` returns: where a printed page (and a named row on it) is in the repo PDF. */
interface Ref {
  printed: number; pdf_page: number; pdf_url: string; page_url: string;
  row_image: string | null; row_page_url: string | null;
}

/** The printed page a source line cites ("… (p.18)", "synopsis 2025-27 p.21 (PDF p.23) …"); null when none. */
export function printedPageOf(source: string): number | null {
  const m = /\bp\.(\d+)/.exec(source);
  return m ? Number(m[1]) : null;
}

/**
 * "Open PDF ›" for a synopsis row: the repo PDF at the row's PDF page (printed page mapped to the
 * PDF's, which differs by 2-6), plus the page rendered with the row boxed and the row's own crop
 * inline. The review app has no pdf-lib, so the boxed render is the highlight (T215's boxes, the
 * row_boxes.json sidecar).
 */
export function SynopsisLink({ printed, water, testid = "synopsis-link" }: {
  printed: number; water?: string; testid?: string;
}) {
  const [ref, setRef] = useState<Ref | null>(null);
  const [err, setErr] = useState("");
  const [open, setOpen] = useState(false);
  const q = `printed=${printed}${water ? `&water=${encodeURIComponent(water)}` : ""}`;
  async function load() {
    if (ref) return ref;
    try {
      const res = await fetch(`/api/synopsis/ref?${q}`);
      if (!res.ok) throw new Error(`${res.status}`);
      const r = (await res.json()) as Ref;
      setRef(r);
      return r;
    } catch (e) {
      setErr(`page ${printed} not found in the repo PDF`);
      return null;
    }
  }
  async function openPdf(e: React.MouseEvent) {
    e.preventDefault();
    const r = await load();
    if (r) window.open(r.row_page_url ?? r.pdf_url, "_blank", "noopener");
  }
  async function toggle(e: React.MouseEvent) {
    e.preventDefault();
    if (!open) await load();
    setOpen(!open);
  }
  return (
    <span className="syn-link" data-testid={testid}>
      <a href="#" onClick={(e) => void openPdf(e)}
        title={`printed p.${printed}: the page with the row boxed (the PDF itself: link below)`}>Open PDF ›</a>
      {" · "}
      <a href="#" onClick={(e) => void toggle(e)} aria-expanded={open}>{open ? "hide row" : "show row"}</a>
      {err && <span className="warn-text"> {err}</span>}
      {open && ref && (
        <span className="syn-open">
          {ref.row_image
            ? <img src={ref.row_image} alt="the synopsis row as extracted" className="evidence-crop" />
            : <span className="dim">no row crop matched this water on p.{printed}</span>}
          <a href={ref.pdf_url} target="_blank" rel="noreferrer">the PDF at page {ref.pdf_page}</a>
          {" · "}<a href={ref.page_url} target="_blank" rel="noreferrer">plain page image</a>
        </span>
      )}
    </span>
  );
}
