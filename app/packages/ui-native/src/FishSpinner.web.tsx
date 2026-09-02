/**
 * The loading fish, on the web. `FishSpinner.tsx` is the native one.
 *
 * TWO CSS ANIMATIONS AND NO JAVASCRIPT AT ALL — which is what the native version has always
 * been trying to be. `useNativeDriver` is honoured on iOS and Android; react-native-web has
 * no native animated module, so it warns on every mount and drives the loop from JS. That
 * is the exact failure the whole design exists to avoid: a loader that stutters when the
 * thread is busy, which is precisely when it is on screen.
 *
 * The CSS here is the same composite `design/fish-sprite.html` renders — a strip paged by
 * `steps()` inside an element the orbit rotates. Keeping both is not duplication of LOGIC:
 * there is none. The frames, the timings and the ring geometry all come from the one
 * generated file, so the two renderers cannot disagree about what is drawn.
 */
import { useEffect, useState } from "react";
import { FISH_SPRITE } from "./fish-sprite.generated";
import type { Palette } from "./theme";

const STYLE_ID = "fish-spinner-keyframes";

/** Injected once. Two keyframes, no per-instance cost. */
function useKeyframes() {
  useEffect(() => {
    if (document.getElementById(STYLE_ID)) return;
    const el = document.createElement("style");
    el.id = STYLE_ID;
    el.textContent =
      `@keyframes fish-orbit { to { transform: rotate(360deg); } }\n` +
      `@keyframes fish-page { to { transform: translateX(var(--fish-strip)); } }`;
    document.head.appendChild(el);
  }, []);
}

export function FishSpinner({ palette, size = 96, label = "Loading", colour }:
  { palette: Palette; size?: number; label?: string; colour?: string }) {
  useKeyframes();
  const [still, setStill] = useState(false);

  useEffect(() => {
    // A spinner is the one thing on screen that always moves, so it is the one thing
    // "reduce motion" is most likely to have been turned on for.
    const q = window.matchMedia?.("(prefers-reduced-motion: reduce)");
    if (!q) return;
    setStill(q.matches);
    const on = (e: MediaQueryListEvent) => setStill(e.matches);
    q.addEventListener("change", on);
    return () => q.removeEventListener("change", on);
  }, []);

  const ring = size * FISH_SPRITE.ring.radius * 2;
  const play = still ? "paused" : "running";

  return (
    <div role="progressbar" aria-label={label} aria-busy={!still}
         style={{ position: "relative", width: size, height: size }}>
      {/* the orbit, in its own colour — a circle looks the same at every angle, so it is
          not rotated */}
      <div data-testid="fish-ring"
           style={{ position: "absolute", left: "50%", top: "50%", width: ring, height: ring,
                    marginLeft: -ring / 2, marginTop: -ring / 2, borderRadius: "50%",
                    border: `${Math.max(size * FISH_SPRITE.ring.width, 1)}px solid ${palette.line}` }} />
      <div style={{ position: "absolute", inset: 0, overflow: "hidden",
                    animation: `fish-orbit ${FISH_SPRITE.orbitMs}ms linear infinite`,
                    animationPlayState: play }}>
        <div data-testid="fish-sprite" style={{
          width: size * FISH_SPRITE.frames, height: size,
          backgroundColor: colour ?? palette.live,
          WebkitMaskImage: `url("${FISH_SPRITE.uri}")`,
          maskImage: `url("${FISH_SPRITE.uri}")`,
          WebkitMaskSize: "100% 100%", maskSize: "100% 100%",
          WebkitMaskRepeat: "no-repeat", maskRepeat: "no-repeat",
          // Pixels, not a percentage: a percentage mask/transform on a strip N times wider
          // than its box does not step by one frame.
          ["--fish-strip" as string]: `-${size * FISH_SPRITE.frames}px`,
          animation: `fish-page ${FISH_SPRITE.cycleMs}ms steps(${FISH_SPRITE.frames}) infinite`,
          animationPlayState: play,
        }} />
      </div>
    </div>
  );
}
