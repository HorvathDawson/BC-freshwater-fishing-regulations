import React, { useEffect, useRef, useState } from 'react';
import * as pdfjsLib from 'pdfjs-dist';
import type { PDFDocumentProxy, RenderTask } from 'pdfjs-dist';
import workerUrl from 'pdfjs-dist/build/pdf.worker.min.mjs?url';
import { ZoomIn, ZoomOut, Maximize2 } from 'lucide-react';
import FishLoader from './FishLoader';

// Render the PDF with pdf.js so it always displays inline, regardless of the
// user's browser PDF settings (e.g. Chrome's "download PDFs instead of opening
// them", which disables the native viewer and would otherwise turn our <iframe>
// into a download). Pages are rasterised to <canvas> elements at a zoom level
// the user controls.
pdfjsLib.GlobalWorkerOptions.workerSrc = workerUrl;

const MIN_ZOOM = 0.5;
const MAX_ZOOM = 4;
const ZOOM_STEP = 0.25;

interface PdfViewerProps {
    url: string;
    title?: string;
}

type Status = 'loading' | 'ready' | 'error';

export default function PdfViewer({ url, title }: PdfViewerProps) {
    const pagesRef = useRef<HTMLDivElement>(null);
    const viewerRef = useRef<HTMLDivElement>(null);
    const dragRef = useRef<{ x: number; y: number; left: number; top: number } | null>(null);
    // When zooming via wheel, keep the point under the cursor stationary. Captured
    // before the zoom change and re-applied once the pages have re-rendered.
    const anchorRef = useRef<{ fx: number; fy: number; ox: number; oy: number } | null>(null);
    const [doc, setDoc] = useState<PDFDocumentProxy | null>(null);
    const [status, setStatus] = useState<Status>('loading');
    const [zoom, setZoom] = useState(1);
    const [dragging, setDragging] = useState(false);

    // Load the document once per url.
    useEffect(() => {
        let cancelled = false;
        setStatus('loading');
        setDoc(null);
        setZoom(1);

        const loadingTask = pdfjsLib.getDocument({ url });
        loadingTask.promise
            .then((pdf) => {
                if (cancelled) { pdf.destroy(); return; }
                setDoc(pdf);
            })
            .catch((err) => {
                if (cancelled) return;
                console.error('[PdfViewer] failed to load PDF', err);
                setStatus('error');
            });

        return () => {
            cancelled = true;
            loadingTask.destroy().catch(() => { /* already destroyed */ });
        };
    }, [url]);

    // Free the document when it changes / on unmount.
    useEffect(() => {
        if (!doc) return;
        return () => { doc.destroy().catch(() => { /* already destroyed */ }); };
    }, [doc]);

    // (Re)render every page whenever the document or zoom changes.
    useEffect(() => {
        const host = pagesRef.current;
        if (!doc || !host) return;

        let cancelled = false;
        const tasks: RenderTask[] = [];

        (async () => {
            host.replaceChildren();
            // Cap DPR so large surveys at high zoom don't exhaust canvas memory.
            const dpr = Math.min(window.devicePixelRatio || 1, 2);
            const baseWidth = host.clientWidth || 600;

            for (let pageNum = 1; pageNum <= doc.numPages; pageNum++) {
                if (cancelled) return;
                const page = await doc.getPage(pageNum);
                const base = page.getViewport({ scale: 1 });
                const cssWidth = baseWidth * zoom;
                const viewport = page.getViewport({ scale: (cssWidth / base.width) * dpr });

                const canvas = document.createElement('canvas');
                canvas.className = 'pdf-viewer-page';
                canvas.width = Math.floor(viewport.width);
                canvas.height = Math.floor(viewport.height);
                canvas.style.width = Math.floor(cssWidth) + 'px';
                canvas.style.height = 'auto';

                const ctx = canvas.getContext('2d');
                if (!ctx) continue;
                host.appendChild(canvas);

                const task = page.render({ canvasContext: ctx, viewport });
                tasks.push(task);
                try {
                    await task.promise;
                } catch {
                    /* render cancelled by zoom change / unmount */
                }
            }

            if (!cancelled) setStatus('ready');

            // Re-anchor the scroll position under the cursor after a wheel zoom.
            const anchor = anchorRef.current;
            if (!cancelled && anchor && viewerRef.current) {
                const v = viewerRef.current;
                v.scrollLeft = anchor.fx * v.scrollWidth - anchor.ox;
                v.scrollTop = anchor.fy * v.scrollHeight - anchor.oy;
                anchorRef.current = null;
            }
        })();

        return () => {
            cancelled = true;
            tasks.forEach((t) => t.cancel());
        };
    }, [doc, zoom]);

    const zoomOut = () => setZoom((z) => Math.max(MIN_ZOOM, +(z - ZOOM_STEP).toFixed(2)));
    const zoomIn = () => setZoom((z) => Math.min(MAX_ZOOM, +(z + ZOOM_STEP).toFixed(2)));
    const fitWidth = () => setZoom(1);

    // Ctrl/Cmd + wheel (and trackpad pinch, which reports ctrlKey) zooms, anchored
    // on the cursor; a plain wheel scrolls the page normally. Fast scrolls are
    // coalesced to at most one zoom step per animation frame so rapid input stays
    // smooth instead of thrashing the canvas re-render. Registered natively so we
    // can preventDefault (React's onWheel is passive).
    useEffect(() => {
        const viewer = viewerRef.current;
        if (!viewer) return;

        let accum = 0;
        let raf: number | null = null;

        const apply = () => {
            raf = null;
            const dir = accum > 0 ? 1 : -1;
            accum = 0;
            setZoom((z) => {
                const next = Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, +(z + dir * ZOOM_STEP).toFixed(2)));
                if (next === z) anchorRef.current = null;
                return next;
            });
        };

        const onWheel = (e: WheelEvent) => {
            if (!e.ctrlKey && !e.metaKey) return; // plain wheel = native scroll
            e.preventDefault();
            const rect = viewer.getBoundingClientRect();
            const ox = e.clientX - rect.left;
            const oy = e.clientY - rect.top;
            anchorRef.current = {
                fx: viewer.scrollWidth ? (viewer.scrollLeft + ox) / viewer.scrollWidth : 0.5,
                fy: viewer.scrollHeight ? (viewer.scrollTop + oy) / viewer.scrollHeight : 0.5,
                ox,
                oy,
            };
            accum += -e.deltaY;
            if (raf == null) raf = requestAnimationFrame(apply);
        };

        viewer.addEventListener('wheel', onWheel, { passive: false });
        return () => {
            viewer.removeEventListener('wheel', onWheel);
            if (raf != null) cancelAnimationFrame(raf);
        };
    }, []);

    // Drag-to-pan the scroll container when the page is larger than the viewport.
    // Mouse only — touch devices pan/scroll natively (and pinch-zoom the page).
    const onPointerDown = (e: React.PointerEvent<HTMLDivElement>) => {
        if (e.pointerType !== 'mouse') return;
        const viewer = viewerRef.current;
        if (!viewer) return;
        // Don't hijack clicks on the zoom controls.
        if ((e.target as HTMLElement).closest('.pdf-viewer-controls')) return;
        const canPan =
            viewer.scrollWidth > viewer.clientWidth || viewer.scrollHeight > viewer.clientHeight;
        if (!canPan) return;
        dragRef.current = { x: e.clientX, y: e.clientY, left: viewer.scrollLeft, top: viewer.scrollTop };
        setDragging(true);
        viewer.setPointerCapture(e.pointerId);
    };

    const onPointerMove = (e: React.PointerEvent<HTMLDivElement>) => {
        const drag = dragRef.current;
        const viewer = viewerRef.current;
        if (!drag || !viewer) return;
        viewer.scrollLeft = drag.left - (e.clientX - drag.x);
        viewer.scrollTop = drag.top - (e.clientY - drag.y);
    };

    const endDrag = (e: React.PointerEvent<HTMLDivElement>) => {
        if (!dragRef.current) return;
        dragRef.current = null;
        setDragging(false);
        try { viewerRef.current?.releasePointerCapture(e.pointerId); } catch { /* not captured */ }
    };

    const viewerClass =
        'pdf-viewer' + (zoom > 1 ? ' is-pannable' : '') + (dragging ? ' is-dragging' : '');

    return (
        <div className={viewerClass}>
            <div
                className="pdf-viewer-scroll"
                ref={viewerRef}
                onPointerDown={onPointerDown}
                onPointerMove={onPointerMove}
                onPointerUp={endDrag}
                onPointerCancel={endDrag}
            >
                <div className="pdf-viewer-pages" ref={pagesRef} aria-label={title} />

                {status === 'loading' && (
                    <div className="pdf-viewer-status" style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 4 }}>
                        <FishLoader size={180} />
                        <span>Loading depth map…</span>
                    </div>
                )}
                {status === 'error' && (
                    <div className="pdf-viewer-status pdf-viewer-error">
                        Couldn’t display the PDF here.{' '}
                        <a href={url} target="_blank" rel="noopener noreferrer">
                            Open it in a new tab
                        </a>
                        .
                    </div>
                )}
            </div>

            {status === 'ready' && (
                <div className="pdf-viewer-controls" role="toolbar" aria-label="Zoom controls">
                    <button
                        type="button"
                        onClick={zoomOut}
                        disabled={zoom <= MIN_ZOOM}
                        aria-label="Zoom out"
                        title="Zoom out"
                    >
                        <ZoomOut size={16} strokeWidth={2} />
                    </button>
                    <span className="pdf-viewer-zoom" aria-live="polite">
                        {Math.round(zoom * 100)}%
                    </span>
                    <button
                        type="button"
                        onClick={zoomIn}
                        disabled={zoom >= MAX_ZOOM}
                        aria-label="Zoom in"
                        title="Zoom in"
                    >
                        <ZoomIn size={16} strokeWidth={2} />
                    </button>
                    <span className="pdf-viewer-controls-divider" aria-hidden="true" />
                    <button
                        type="button"
                        onClick={fitWidth}
                        disabled={zoom === 1}
                        aria-label="Fit to width"
                        title="Fit to width"
                    >
                        <Maximize2 size={16} strokeWidth={2} />
                    </button>
                </div>
            )}
        </div>
    );
}
