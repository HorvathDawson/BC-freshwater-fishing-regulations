/**
 * Paging a baked sprite strip with one animated value.
 *
 * Lives here, not in the component, because it is arithmetic (rule 25) — and because
 * desktop draws the same strip with a CSS `steps()` keyframe over the same numbers.
 */

/**
 * A staircase: hold each frame, then jump to the next.
 *
 * The obvious `[0, 1] -> [0, -(frames - 1) * width]` is a RAMP, and a ramp slides the strip
 * continuously so two half-frames show at once. A staircase needs a plateau per frame, and
 * `interpolate` requires a strictly increasing input range — hence the epsilon before each
 * step rather than a repeated value.
 *
 * The final point returns to frame 0 at input 1, so the value can loop 0 -> 1 forever
 * without a seam.
 */
export function spriteStaircase(frames: number, width: number):
  { inputRange: number[]; outputRange: number[] } {
  if (frames < 1) throw new Error(`a sprite strip needs at least one frame, got ${frames}`);
  const inputRange: number[] = [];
  const outputRange: number[] = [];
  for (let i = 0; i < frames; i++) {
    inputRange.push(i / frames, (i + 1) / frames - EPS);
    const at = i === 0 ? 0 : -i * width;   // not `-0 * width`, which is -0
    outputRange.push(at, at);
  }
  inputRange.push(1);
  outputRange.push(0);
  return { inputRange, outputRange };
}

/** Small enough to be invisible at any frame rate, large enough to keep the range strict. */
const EPS = 1e-6;
