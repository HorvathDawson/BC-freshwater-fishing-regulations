import { useEffect, useRef } from "react";

/**
 * ESCAPE CLOSES IT, wherever the focus is (owner 2026-10-22 late, desktop search). A dialog over the page (a Modal:
 * the Layers sheet, a menu page) hears its own Escape first and is left to it. Web only: a phone has no Escape key
 * (escapeCloses.native.ts is the no-op twin).
 */
export function useEscapeCloses(active: boolean, onClose: () => void): void {
  const ref = useRef(onClose);
  ref.current = onClose;
  useEffect(() => {
    if (!active || typeof window === "undefined") return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== "Escape" || e.defaultPrevented) return;
      if (typeof document !== "undefined" && document.querySelector('[aria-modal="true"]')) return;
      ref.current();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [active]);
}
