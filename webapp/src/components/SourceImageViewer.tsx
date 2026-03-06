import React, { useRef, useState, useEffect, useCallback } from 'react';
import { X, ZoomIn, ZoomOut, RotateCcw } from 'lucide-react';
import './SourceImageViewer.css';

interface SourceImageViewerProps {
    src: string;
    name: string;
    onClose: () => void;
}

/**
 * Full-screen image viewer with pinch-zoom & pan.
 *
 * Uses CSS transform (not native scroll/zoom) so closing the modal
 * never leaves the page in a zoomed state — fixing the map gesture
 * conflict on mobile.
 *
 * On mobile the image auto-fits to screen width so wide screenshots
 * are readable immediately; the user can pan vertically to see tall
 * content.
 */
const SourceImageViewer: React.FC<SourceImageViewerProps> = ({ src, name, onClose }) => {
    const containerRef = useRef<HTMLDivElement>(null);
    const imgRef = useRef<HTMLImageElement>(null);

    // Transform state
    const [scale, setScale] = useState(1);
    const [translate, setTranslate] = useState({ x: 0, y: 0 });
    const [imgLoaded, setImgLoaded] = useState(false);

    // Pointer tracking for pan
    const pointers = useRef<Map<number, { x: number; y: number }>>(new Map());
    const lastPinchDist = useRef<number | null>(null);
    const lastPanPos = useRef<{ x: number; y: number } | null>(null);
    const isDragging = useRef(false);

    // Min / max scale
    const MIN_SCALE = 0.5;
    const MAX_SCALE = 6;

    // --- Auto-fit on load (mobile: fill width so text is readable) ---
    const autoFit = useCallback(() => {
        const img = imgRef.current;
        const container = containerRef.current;
        if (!img || !container) return;

        const cw = container.clientWidth;
        const ch = container.clientHeight;
        const nw = img.naturalWidth;
        const nh = img.naturalHeight;
        if (!nw || !nh) return;

        const isMobile = window.innerWidth <= 768;

        if (isMobile) {
            // Fill the container width so text in wide screenshots is readable.
            // Allow vertical panning for tall content.
            const fitScale = cw / nw;
            const displayH = nh * fitScale;
            setScale(fitScale);
            // Center vertically if image is shorter than viewport, else top-align
            const yOffset = displayH < ch ? 0 : (displayH - ch) / 2;
            setTranslate({ x: 0, y: -yOffset });
        } else {
            // Desktop: fit entire image (contain)
            const fitScale = Math.min(cw / nw, ch / nh, 1);
            setScale(fitScale);
            setTranslate({ x: 0, y: 0 });
        }
    }, []);

    useEffect(() => {
        if (imgLoaded) autoFit();
    }, [imgLoaded, autoFit]);

    // Escape key closes
    useEffect(() => {
        const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
        window.addEventListener('keydown', onKey);
        return () => window.removeEventListener('keydown', onKey);
    }, [onClose]);

    // --- Pointer handlers (unified touch + mouse) ---
    const onPointerDown = useCallback((e: React.PointerEvent) => {
        // Don't capture events on the header buttons
        if ((e.target as HTMLElement).closest('.siv-header')) return;
        (e.target as HTMLElement).setPointerCapture?.(e.pointerId);
        pointers.current.set(e.pointerId, { x: e.clientX, y: e.clientY });
        isDragging.current = true;
        lastPanPos.current = { x: e.clientX, y: e.clientY };
        lastPinchDist.current = null;
    }, []);

    const onPointerMove = useCallback((e: React.PointerEvent) => {
        if (!isDragging.current) return;
        pointers.current.set(e.pointerId, { x: e.clientX, y: e.clientY });

        const pts = Array.from(pointers.current.values());

        if (pts.length >= 2) {
            // Pinch zoom
            const dist = Math.hypot(pts[0].x - pts[1].x, pts[0].y - pts[1].y);
            if (lastPinchDist.current !== null) {
                const delta = dist / lastPinchDist.current;
                setScale(s => Math.min(MAX_SCALE, Math.max(MIN_SCALE, s * delta)));
            }
            lastPinchDist.current = dist;
            // Also pan with pinch midpoint
            const mx = (pts[0].x + pts[1].x) / 2;
            const my = (pts[0].y + pts[1].y) / 2;
            if (lastPanPos.current) {
                const dx = mx - lastPanPos.current.x;
                const dy = my - lastPanPos.current.y;
                setTranslate(t => ({ x: t.x + dx, y: t.y + dy }));
            }
            lastPanPos.current = { x: mx, y: my };
        } else if (pts.length === 1 && lastPanPos.current) {
            // Single-finger / mouse drag → pan
            const dx = e.clientX - lastPanPos.current.x;
            const dy = e.clientY - lastPanPos.current.y;
            setTranslate(t => ({ x: t.x + dx, y: t.y + dy }));
            lastPanPos.current = { x: e.clientX, y: e.clientY };
        }
    }, []);

    const onPointerUp = useCallback((e: React.PointerEvent) => {
        pointers.current.delete(e.pointerId);
        if (pointers.current.size === 0) {
            isDragging.current = false;
            lastPanPos.current = null;
            lastPinchDist.current = null;
        } else if (pointers.current.size === 1) {
            // Switch from pinch to single-finger pan seamlessly
            const remaining = Array.from(pointers.current.values())[0];
            lastPanPos.current = remaining;
            lastPinchDist.current = null;
        }
    }, []);

    // Mouse wheel zoom (desktop) — must use native listener with { passive: false }
    // so preventDefault() actually suppresses page scroll.  React registers wheel
    // handlers as passive, which causes "Unable to preventDefault inside passive
    // event listener" warnings.
    useEffect(() => {
        const el = containerRef.current;
        if (!el) return;
        const handleWheel = (e: WheelEvent) => {
            e.preventDefault();
            const factor = e.deltaY > 0 ? 0.9 : 1.1;
            setScale(s => Math.min(MAX_SCALE, Math.max(MIN_SCALE, s * factor)));
        };
        el.addEventListener('wheel', handleWheel, { passive: false });
        return () => el.removeEventListener('wheel', handleWheel);
    }, []);

    // Zoom controls
    const zoomIn = () => setScale(s => Math.min(MAX_SCALE, s * 1.4));
    const zoomOut = () => setScale(s => Math.max(MIN_SCALE, s / 1.4));
    const resetZoom = () => autoFit();

    const pct = Math.round(scale * 100 / (imgRef.current ? Math.min(
        (containerRef.current?.clientWidth ?? 1) / (imgRef.current.naturalWidth || 1),
        (containerRef.current?.clientHeight ?? 1) / (imgRef.current.naturalHeight || 1),
        1
    ) : 1));

    return (
        <div
            className="siv-overlay"
            role="dialog"
            aria-modal="true"
            aria-label={`Synopsis source image for ${name}`}
        >
            {/* Header */}
            <div className="siv-header">
                <span className="siv-title">{name}</span>
                <div className="siv-controls">
                    <button onClick={zoomOut} className="siv-btn" aria-label="Zoom out" title="Zoom out">
                        <ZoomOut size={16} />
                    </button>
                    <span className="siv-zoom-pct">{pct}%</span>
                    <button onClick={zoomIn} className="siv-btn" aria-label="Zoom in" title="Zoom in">
                        <ZoomIn size={16} />
                    </button>
                    <button onClick={resetZoom} className="siv-btn" aria-label="Reset zoom" title="Fit to screen">
                        <RotateCcw size={16} />
                    </button>
                    <button onClick={onClose} className="siv-btn siv-close" aria-label="Close">
                        <X size={18} />
                    </button>
                </div>
            </div>

            {/* Image viewport */}
            <div
                ref={containerRef}
                className="siv-viewport"
                onPointerDown={onPointerDown}
                onPointerMove={onPointerMove}
                onPointerUp={onPointerUp}
                onPointerCancel={onPointerUp}
            >
                <img
                    ref={imgRef}
                    src={src}
                    alt={`Synopsis source for ${name}`}
                    className="siv-img"
                    draggable={false}
                    onLoad={() => setImgLoaded(true)}
                    style={{
                        transform: `translate(${translate.x}px, ${translate.y}px) scale(${scale})`,
                        transformOrigin: 'center center',
                        opacity: imgLoaded ? 1 : 0,
                    }}
                />
                {!imgLoaded && <div className="siv-loading">Loading…</div>}
            </div>
        </div>
    );
};

export default SourceImageViewer;
