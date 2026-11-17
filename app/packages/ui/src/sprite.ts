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

/**
 * (T814) A SHEET, not a strip: `frames` laid row-major in `columns` (frame i at column i % columns, row
 * floor(i / columns)), each `width` x `height`. Two staircases off ONE value, so the column and the row can never
 * disagree about the frame: `x` steps every frame and wraps each row, `y` steps once a row. A strip of the coho loader's
 * 80 frames would be 80 frames wide, past a phone's texture limit; a grid keeps the sheet near square.
 */
export function spriteGridStaircase(frames: number, columns: number, width: number, height: number): {
  x: { inputRange: number[]; outputRange: number[] }; y: { inputRange: number[]; outputRange: number[] };
} {
  if (frames < 1) throw new Error(`a sprite sheet needs at least one frame, got ${frames}`);
  if (columns < 1) throw new Error(`a sprite sheet needs at least one column, got ${columns}`);
  const inputRange: number[] = [];
  const xs: number[] = [];
  const ys: number[] = [];
  const at = (n: number) => (n === 0 ? 0 : -n);   // not -0
  for (let i = 0; i < frames; i++) {
    inputRange.push(i / frames, (i + 1) / frames - EPS);
    const x = at((i % columns) * width), y = at(Math.floor(i / columns) * height);
    xs.push(x, x);
    ys.push(y, y);
  }
  inputRange.push(1);
  xs.push(0);
  ys.push(0);
  return { x: { inputRange, outputRange: xs }, y: { inputRange: [...inputRange], outputRange: ys } };
}

/** Small enough to be invisible at any frame rate, large enough to keep the range strict. */
const EPS = 1e-6;
