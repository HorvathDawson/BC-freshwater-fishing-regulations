/**
 * T354 — what the in-app rule-book viewer shows: a local PDF, the PDF page to open at, and why it could not draw it
 * (the viewer then says so, with "Open in another app": `openOutside`). Data only. Set by the phone's open
 * (`synopsisPdf.native.ts`), drawn by `PdfViewer` (mounted once by the Shell). The web never sets it: its browser tab
 * is the viewer.
 */
import { useSyncExternalStore } from "react";

export interface PdfView { readonly uri: string; readonly page: number; readonly failed: string | null;
  /** when it was asked for (epoch ms): the viewer logs how long the page took to draw */
  readonly at: number }

let view: PdfView | null = null;
const subs = new Set<() => void>();
const set = (v: PdfView | null) => { view = v; subs.forEach((f) => f()); };

export const showPdf = (uri: string, page: number): void => set({ uri, page, failed: null, at: Date.now() });
/** the viewer could not draw the file: said on its face, never a silent hand-off */
export const failPdf = (why: string): void => { if (view) set({ ...view, failed: why }); };
export const closePdf = (): void => set(null);
export const getPdfView = (): PdfView | null => view;
const subscribe = (f: () => void) => { subs.add(f); return () => { subs.delete(f); }; };
export const usePdfView = (): PdfView | null => useSyncExternalStore(subscribe, getPdfView, getPdfView);
