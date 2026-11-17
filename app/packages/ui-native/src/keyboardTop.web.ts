import { useEffect, useState } from "react";

/** the visual viewport, as far as this reads it (absent in an old browser or a test's document) */
interface Viewport extends EventTarget { readonly height: number; readonly offsetTop: number; readonly scale: number }

/**
 * A phone browser's keyboard shrinks the VISUAL viewport, not the page (iOS Safari): its foot, in the page's own
 * coordinates (`offsetTop + height`, what `measureInWindow` measures against), is the keyboard's top. Pinch zoom shrinks
 * it too, so a zoomed page (scale ≠ 1) reads as no keyboard. A desktop browser has no on-screen keyboard: never smaller.
 */
export function useKeyboardTop(): number | null {
  const [top, setTop] = useState<number | null>(null);
  useEffect(() => {
    if (typeof window === "undefined") return;
    const vv = (window as unknown as { visualViewport?: Viewport }).visualViewport;
    if (!vv) return;
    const read = () => {
      const foot = vv.offsetTop + vv.height;
      setTop(Math.abs(vv.scale - 1) < 0.01 && window.innerHeight - foot > 40 ? foot : null);
    };
    read();
    vv.addEventListener("resize", read);
    vv.addEventListener("scroll", read);
    return () => { vv.removeEventListener("resize", read); vv.removeEventListener("scroll", read); };
  }, []);
  return top;
}
