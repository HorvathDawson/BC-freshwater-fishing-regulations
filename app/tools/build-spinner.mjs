/**
 * Bakes the loading fish into a sprite strip.  Run by `pnpm spinner`.
 *
 * WHY THIS IS A BUILD STEP
 * ------------------------
 * `archive/webapp/src/components/FishLoader.tsx` drew a 30-segment spine, a body wave, a
 * dorsal and pectoral fin, a quadratic tail and a bubble system from `requestAnimationFrame`.
 * Every one of those is a SHAPE change, and a shape change is JS work on the same thread
 * that is busy doing the thing you are waiting for — so the loader stuttered exactly when
 * there was something to wait for, which reads as a hung app.
 *
 * `transform` and `opacity` are the only things a platform animation driver can run without
 * JS. So the split is: whatever `transform` can express stays live (the orbit is a rotation),
 * and everything it cannot is rendered here, ahead of time, into frames.
 *
 * WHAT MAKES THE LOOP SHORT
 * -------------------------
 * In the archive the body wave ran at 5× the orbit (`sin(time * 5 - i * 0.3)` against
 * `baseAngle = time`). Five whole wave cycles per orbit means the fish's shape *in its own
 * rotating frame* repeats five times per lap — so one baked cycle is a fifth of a lap, not a
 * whole one. Fifteen frames instead of seventy-seven.
 *
 * The bubbles come along for free. In the rotating frame the fish is still and the world
 * turns backwards under it, so a bubble shed at the tail sweeps back along the arc on its
 * own. Give each bubble a life of exactly one wave cycle and the strip loops seamlessly:
 * the bubble leaving the last frame is the one entering the first.
 *
 * OUTPUT
 * ------
 * A grayscale+alpha PNG, as a data URI in a generated TS module rather than a `.png` on
 * disk. An asset file needs Metro's asset pipeline, webpack's loader and a vitest plugin to
 * agree; a data URI is just a string and every bundler already handles it identically.
 *
 * Grayscale+alpha, not colour, because the runtime tints it. One strip serves the light,
 * dark and colour-blind palettes — and cannot fall out of step with them.
 */
import { deflateSync } from "node:zlib";
import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

// ── the drawing ──────────────────────────────────────────────────────────────────────
// Archive numbers scaled by 0.8, which is what fits a 70px orbit and a 100px fish into a
// square frame instead of the 400x300 canvas they were drawn for.
const S = 192;             // frame, device px (96 CSS px at 2x)
const N = 15;              // frames per wave cycle — 24fps at the 0.64s cycle below
const C = S / 2;
const ORBIT = 58;
const SEGMENTS = 30;
// Proportions follow the archive (100 long, 26 wide, on a 70 orbit) scaled to ours. The
// earlier 40-long fish with a floored body width read as a tadpole: the floor kept the
// rear thick, so the taper that makes a fish look like a fish never happened.
// WIGGLE was 2.6 and every baked frame looked identical — on a 40px fish that is a two
// pixel deflection, which is nothing. The whole reason to bake is motion transform cannot
// express, so if the frames do not differ the strip is 17 KB of the same picture.
// EXACTLY the archive's proportions, scaled by ORBIT/70. Every guess I made at these
// looked worse than the thing being replaced, so they are now derived rather than picked:
// fishLength 100, fishWidth 26, tail 30, dorsal 18, pectoral 24, wiggle 6, on a 70 orbit.
const K = ORBIT / 70;
const FISH_LEN = 100 * K, FISH_W = 26 * K, TAIL = 30 * K;
const DORSAL = 18 * K, PECTORAL = 24 * K, WIGGLE = 6 * K;
const TAIL_AT = SEGMENTS - 1;
const EYE_AT = 3, EYE_R = 2.2, EYE_OUT = 2.6;
const BUBBLES = 5, BUBBLE_MIN = 2.0, BUBBLE_MAX = 4.5;
const CYCLE = (Math.PI * 2) / 5;   // one wave cycle, in the archive's `time` units

const ang = (a, b) => Math.atan2(b.y - a.y, b.x - a.x);

/**
 * The fish's spine at wave phase `t`, in the frame that rotates with the orbit.
 *
 * `baseAngle` drops the archive's `time` term: that term IS the orbit, and the orbit is a
 * rotation the runtime applies to the whole strip. What is left is the fish's own shape.
 */
function spine(t) {
  const pts = [];
  for (let i = 0; i < SEGMENTS; i++) {
    const dist = i * (FISH_LEN / SEGMENTS);
    const a = -dist / ORBIT;
    const nx = Math.cos(a), ny = Math.sin(a);
    const wave = Math.sin(t * 5 - i * 0.3) * WIGGLE * (i / SEGMENTS);
    pts.push({ x: C + (ORBIT + wave) * nx, y: C + (ORBIT + wave) * ny });
  }
  return pts;
}

/** Flatten one quadratic Bézier into points (the tail's two curves). */
function quad(p0, cp, p1, steps = 12) {
  const out = [];
  for (let i = 1; i <= steps; i++) {
    const u = i / steps, v = 1 - u;
    out.push({ x: v * v * p0.x + 2 * v * u * cp.x + u * u * p1.x,
               y: v * v * p0.y + 2 * v * u * cp.y + u * u * p1.y });
  }
  return out;
}

function circle(cx, cy, r, steps = 40, reverse = false) {
  const out = [];
  for (let i = 0; i < steps; i++) {
    const a = ((reverse ? -i : i) / steps) * Math.PI * 2;
    out.push({ x: cx + r * Math.cos(a), y: cy + r * Math.sin(a) });
  }
  return out;
}

/** Everything drawn at wave phase `t`, as `{ rings, alpha }` shapes filled even-odd. */
function shapes(t) {
  const sp = spine(t);
  // NO RING HERE. The strip is tinted with a single colour, so anything baked into it can
  // only ever be the fish's colour — and the archive's loader was a steel-blue fish on a
  // PALE GREY orbit, which is most of why it looked better. The ring is drawn by the
  // component as a bordered view instead, in its own token.
  const out = [];

  // Bubbles shed from the tail. Each is one wave cycle old at most, so the set is periodic
  // and the strip loops. In this frame the world turns backwards under a still fish, which
  // is what carries them off along the arc — no per-bubble path to store.
  for (let k = 0; k < BUBBLES; k++) {
    const age = ((t / CYCLE) + k / BUBBLES) % 1;          // 0 = just shed, 1 = gone
    const born = spine(t - age * CYCLE);
    const tail = born[SEGMENTS - 1], prev = born[SEGMENTS - 5];
    const a = ang(prev, tail);
    const bx = tail.x - Math.cos(a) * 8, by = tail.y - Math.sin(a) * 8;
    // sweep back along the orbit by the angle the fish covered since it was shed
    const swept = -age * CYCLE;
    const dx = bx - C, dy = by - C;
    const r = Math.hypot(dx, dy) + age * 5.5;              // and wander outward as they age
    const th = Math.atan2(dy, dx) + swept;
    const size = BUBBLE_MIN + ((k * 2.7) % 1) * (BUBBLE_MAX - BUBBLE_MIN) + age * 1.2;
    const px = C + r * Math.cos(th), py = C + r * Math.sin(th);
    out.push({ rings: [circle(px, py, size), circle(px, py, Math.max(size - 1.3, 0.2))],
               alpha: (1 - age) * 0.85 });
  }

  // dorsal, pectoral — triangles hung off the spine, exactly as the archive hung them
  const dA = ang(sp[9], sp[17]);
  out.push({ rings: [[sp[9],
                      { x: sp[13].x + Math.cos(dA - 1.2) * DORSAL,
                        y: sp[13].y + Math.sin(dA - 1.2) * DORSAL },
                      sp[17]]], alpha: 1 });
  const pA = ang(sp[5], sp[12]);
  out.push({ rings: [[sp[5],
                      { x: sp[8].x + Math.cos(pA + 1.35) * PECTORAL,
                        y: sp[8].y + Math.sin(pA + 1.35) * PECTORAL },
                      sp[12]]], alpha: 1 });

  // body: the spine offset either side by a width that tapers to nothing at head and tail
  const half = (i) => (FISH_W / 2) * Math.sin(Math.pow(i / (SEGMENTS - 1), 0.6) * Math.PI);
  const side = (sign) => sp.map((p, i) => {
    const a = ang(sp[Math.max(i - 1, 0)], sp[Math.min(i + 1, SEGMENTS - 1)]) + sign * Math.PI / 2;
    return { x: p.x + Math.cos(a) * half(i), y: p.y + Math.sin(a) * half(i) };
  });
  const eA = ang(sp[EYE_AT - 1], sp[EYE_AT + 1]) - Math.PI / 2;
  const eye = circle(sp[EYE_AT].x + Math.cos(eA) * EYE_OUT,
                     sp[EYE_AT].y + Math.sin(eA) * EYE_OUT, EYE_R, 20, true);
  out.push({ rings: [[...side(1), ...side(-1).reverse()], eye], alpha: 1 });

  // tail: two quadratics meeting at a notch
  // The tail's angle comes from the spine and nothing else, as in the archive. An extra
  // flick term was added when the body wave was too small to see; with the wave at its
  // proper amplitude it over-drives the fin and tears it off the peduncle, which meets the
  // body at a single point.
  const tp = sp[TAIL_AT], ta = ang(sp[TAIL_AT - 4], tp);
  const tip = { x: tp.x + Math.cos(ta) * TAIL * 0.4, y: tp.y + Math.sin(ta) * TAIL * 0.4 };
  out.push({ rings: [[tp,
    ...quad(tp, { x: tp.x + Math.cos(ta - 0.9) * TAIL, y: tp.y + Math.sin(ta - 0.9) * TAIL }, tip),
    ...quad(tip, { x: tp.x + Math.cos(ta + 0.9) * TAIL, y: tp.y + Math.sin(ta + 0.9) * TAIL }, tp),
  ]], alpha: 1 });

  return out;
}

// ── rasterising ──────────────────────────────────────────────────────────────────────
const SS = 4;  // vertical supersampling; horizontal coverage is computed analytically

/** Even-odd scanline fill of one shape, composited into `alpha` by max. */
function fill(alpha, width, height, rings, shapeAlpha) {
  const edges = [];
  for (const ring of rings)
    for (let i = 0; i < ring.length; i++) {
      const a = ring[i], b = ring[(i + 1) % ring.length];
      if (a.y !== b.y) edges.push([a, b]);
    }
  const cov = new Float64Array(width);
  for (let oy = 0; oy < height; oy++) {
    cov.fill(0);
    for (let s = 0; s < SS; s++) {
      const y = oy + (s + 0.5) / SS;
      const xs = [];
      for (const [a, b] of edges) {
        const lo = Math.min(a.y, b.y), hi = Math.max(a.y, b.y);
        if (y < lo || y >= hi) continue;
        xs.push(a.x + ((y - a.y) / (b.y - a.y)) * (b.x - a.x));
      }
      if (xs.length < 2) continue;
      xs.sort((p, q) => p - q);
      for (let i = 0; i + 1 < xs.length; i += 2) {
        const xa = Math.max(xs[i], 0), xb = Math.min(xs[i + 1], width);
        for (let px = Math.floor(xa); px < xb; px++) {
          if (px < 0) continue;
          cov[px] += Math.min(xb, px + 1) - Math.max(xa, px);
        }
      }
    }
    for (let px = 0; px < width; px++) {
      if (cov[px] === 0) continue;
      const v = Math.min(cov[px] / SS, 1) * shapeAlpha;
      const at = oy * width + px;
      if (v > alpha[at]) alpha[at] = v;
    }
  }
}

// ── PNG ──────────────────────────────────────────────────────────────────────────────
const CRC = (() => {
  const t = new Int32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    t[n] = c;
  }
  return t;
})();
const crc32 = (buf) => {
  let c = -1;
  for (const b of buf) c = CRC[(c ^ b) & 0xff] ^ (c >>> 8);
  return (c ^ -1) >>> 0;
};
function chunk(type, data) {
  const len = Buffer.alloc(4); len.writeUInt32BE(data.length);
  const body = Buffer.concat([Buffer.from(type, "latin1"), data]);
  const crc = Buffer.alloc(4); crc.writeUInt32BE(crc32(body));
  return Buffer.concat([len, body, crc]);
}
/** RGB (colour type 2). Only used for the human-readable contact sheet. */
function pngRGB(width, height, rgb) {
  const raw = Buffer.alloc(height * (1 + width * 3));
  for (let y = 0; y < height; y++) {
    const row = y * (1 + width * 3);
    raw[row] = 0;
    rgb.copy(raw, row + 1, y * width * 3, (y + 1) * width * 3);
  }
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(width, 0); ihdr.writeUInt32BE(height, 4);
  ihdr[8] = 8; ihdr[9] = 2;
  return Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    chunk("IHDR", ihdr),
    chunk("IDAT", deflateSync(raw, { level: 9 })),
    chunk("IEND", Buffer.alloc(0)),
  ]);
}

/**
 * A CONTACT SHEET, not the strip.
 *
 * The strip itself is white pixels with the drawing in the alpha channel, so saved as a
 * file it looks like an empty white rectangle in every viewer — which is exactly as useful
 * as not writing it. This composites the frames onto the dark palette's ground in the dark
 * palette's accent, laid out in a grid, so the artwork can be reviewed by looking at it.
 */
function contactSheet(alpha, stripW, frame, frames, cols = 5) {
  const rows = Math.ceil(frames / cols);
  const W = frame * cols, H = frame * rows;
  const bg = [0x10, 0x12, 0x15], fg = [0x37, 0xd6, 0xea];   // theme.ts DARK: wash, live
  const px = Buffer.alloc(W * H * 3);
  for (let i = 0; i < W * H; i++) { px[i * 3] = bg[0]; px[i * 3 + 1] = bg[1]; px[i * 3 + 2] = bg[2]; }
  for (let f = 0; f < frames; f++) {
    const cx = (f % cols) * frame, cy = Math.floor(f / cols) * frame;
    for (let y = 0; y < frame; y++)
      for (let x = 0; x < frame; x++) {
        const a = alpha[y * stripW + f * frame + x];
        if (!a) continue;
        const d = ((cy + y) * W + cx + x) * 3;
        for (let c = 0; c < 3; c++) px[d + c] = Math.round(bg[c] * (1 - a) + fg[c] * a);
      }
  }
  return pngRGB(W, H, px);
}

/** Grayscale+alpha (colour type 4): white pixels, the alpha channel carries the drawing. */
function png(width, height, alpha) {
  const raw = Buffer.alloc(height * (1 + width * 2));
  for (let y = 0; y < height; y++) {
    const row = y * (1 + width * 2);
    raw[row] = 0;                                    // filter: none
    for (let x = 0; x < width; x++) {
      raw[row + 1 + x * 2] = 255;
      raw[row + 2 + x * 2] = Math.round(alpha[y * width + x] * 255);
    }
  }
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(width, 0); ihdr.writeUInt32BE(height, 4);
  ihdr[8] = 8; ihdr[9] = 4; ihdr[10] = 0; ihdr[11] = 0; ihdr[12] = 0;
  return Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    chunk("IHDR", ihdr),
    chunk("IDAT", deflateSync(raw, { level: 9 })),
    chunk("IEND", Buffer.alloc(0)),
  ]);
}


/**
 * A page that PLAYS it.
 *
 * A contact sheet answers "is the fish drawn right"; it cannot answer "does it swim", and
 * that is the question fifteen baked frames exist to answer. This runs the real composite —
 * the strip paged by `steps()` inside an element the orbit rotates — at four sizes, so the
 * loop, the seam and the frame rate are all visible.
 *
 * It doubles as the reference for `FishSpinner.web.tsx`: the CSS below IS what that
 * component should do, and no JavaScript runs the animation.
 */
function PAGE({ uri, frames, kb, cycle, orbit, figures, ring, ringW }) {
  return `<!doctype html>
<meta charset="utf8"><title>Fish spinner</title>
<style>
  :root { --bg:#101215; --fg:#F0F2F0; --sub:#98A0A7; --line:#252A2F;
          --accent:#A97CFF; --fish:#37D6EA; }   /* palette.live, as the app uses */
  :root[data-t="light"] { --bg:#F7F7F5; --fg:#15181C; --sub:#6C737A; --line:#E5E6E1;
                          --accent:#5F26E0; --fish:#04879B; }
  body { margin:0; min-height:100vh; background:var(--bg); color:var(--fg);
         font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
         display:grid; place-items:center; align-content:center; gap:26px; padding:48px 24px; }
  h1 { font-size:19px; font-weight:650; margin:0; letter-spacing:-.2px; }
  .sub { color:var(--sub); font-size:13px; margin:-20px 0 0; }
  .stage { display:flex; gap:44px; align-items:flex-end; flex-wrap:wrap;
           justify-content:center; min-height:140px; }

  /* ---- THE COMPONENT: two elements, two animations, zero JavaScript ----
     Clip a strip ${frames} frames wide and translate it a whole frame at a time. NOT
     percentage mask-position: a percentage there resolves against (box - image), so
     -100% on a strip ${frames}x wider than its box moves it the wrong way by the wrong
     amount. Pixels, through --s, or nothing steps correctly. This mirrors the React
     Native component exactly, which is the point. */
  /* the orbit ring: drawn, not baked, so it carries its own colour */
  .fish { position:relative; width:var(--s); height:var(--s); }
  .fish::before { content:""; position:absolute; left:50%; top:50%;
                  width:calc(var(--s) * ${(ring * 2).toFixed(4)});
                  height:calc(var(--s) * ${(ring * 2).toFixed(4)});
                  transform:translate(-50%,-50%); border-radius:50%;
                  border:max(1px, calc(var(--s) * ${ringW.toFixed(5)})) solid var(--line); }
  .spin { position:absolute; inset:0; overflow:hidden;
          animation:orbit ${orbit}ms linear infinite; }
  .spin > i { display:block; width:calc(${frames} * var(--s)); height:var(--s);
              background-color:var(--fish);
              -webkit-mask-image:url("${uri}"); mask-image:url("${uri}");
              -webkit-mask-repeat:no-repeat; mask-repeat:no-repeat;
              -webkit-mask-size:100% 100%; mask-size:100% 100%;
              animation:page ${cycle}ms steps(${frames}) infinite; }
  @keyframes orbit { to { transform:rotate(360deg); } }
  @keyframes page  { to { transform:translateX(calc(-${frames} * var(--s))); } }
  /* ---------------------------------------------------------------------- */

  .paused .spin, .paused .spin > i { animation-play-state:paused; }
  figure { margin:0; display:grid; gap:10px; justify-items:center; }
  figcaption { color:var(--sub); font-size:12px; }
  .bar { display:flex; gap:10px; flex-wrap:wrap; justify-content:center; }
  button { font:inherit; font-size:13px; padding:7px 14px; border-radius:999px; cursor:pointer;
           border:1px solid var(--line); background:transparent; color:var(--sub); }
  button[aria-pressed="true"] { background:var(--accent); color:var(--bg); border-color:transparent; }
  .strip { width:min(100%,1100px); height:64px; background-color:var(--fish);
           -webkit-mask-image:url("${uri}"); mask-image:url("${uri}");
           -webkit-mask-size:100% 100%; mask-size:100% 100%; }
  .note { color:var(--sub); font-size:12.5px; max-width:64ch; text-align:center; }
  code { font-family:ui-monospace,monospace; font-size:12px; color:var(--fg); }
</style>
<h1>Fish spinner</h1>
<p class="sub">${frames} baked frames &middot; ${kb} KB &middot; one wave cycle, five per orbit</p>
<div class="bar">
  <button data-t="dark" aria-pressed="true">dark</button>
  <button data-t="light" aria-pressed="false">light</button>
  <button id="pause" aria-pressed="false">pause</button>
</div>
<div class="stage">
  ${figures}
</div>
<div class="strip" title="the whole strip, left to right"></div>
<p class="note">Grayscale+alpha, coloured by <code>mask-image</code> here and
<code>tintColor</code> in the app &mdash; one file, every palette. Pause and reload to step
the loop; nothing here is driven by JavaScript.</p>
<script>
  const root = document.documentElement;
  for (const b of document.querySelectorAll("button[data-t]"))
    b.onclick = () => { root.dataset.t = b.dataset.t;
      document.querySelectorAll("button[data-t]").forEach(o =>
        o.setAttribute("aria-pressed", String(o === b))); };
  const p = document.getElementById("pause");
  p.onclick = () => { const on = document.body.classList.toggle("paused");
                      p.setAttribute("aria-pressed", String(on));
                      p.textContent = on ? "play" : "pause"; };
</script>
`;
}

// ── build ────────────────────────────────────────────────────────────────────────────
const W = S * N;
const alpha = new Float64Array(W * S);
for (let f = 0; f < N; f++) {
  const t = (f / N) * CYCLE;
  const frame = new Float64Array(S * S);
  for (const sh of shapes(t)) fill(frame, S, S, sh.rings, sh.alpha);
  for (let y = 0; y < S; y++)
    for (let x = 0; x < S; x++) alpha[y * W + f * S + x] = frame[y * S + x];
}
const bytes = png(W, S, alpha);
const uri = `data:image/png;base64,${bytes.toString("base64")}`;
const FISH_SPRITE_CYCLE = 640, FISH_SPRITE_ORBIT = 3200;

// A viewable copy beside the module, because a base64 data URI inside a TS file is not
// something anyone can look at — and the artwork is reviewed by looking at it.
const sheet = fileURLToPath(new URL("../design/fish-sprite.png", import.meta.url));
writeFileSync(sheet, contactSheet(alpha, W, S, N));

const out = fileURLToPath(new URL("../packages/ui-native/src/fish-sprite.generated.ts",
                                  import.meta.url));
writeFileSync(out, `/**
 * GENERATED by \`pnpm spinner\` (tools/build-spinner.mjs). Do not edit.
 *
 * ${N} frames of one body-wave cycle, ${S}x${S} device px each, laid out left to right.
 * Grayscale+alpha: every pixel is white and the alpha channel is the drawing, so the
 * runtime tints one strip for every palette.
 */
export const FISH_SPRITE = {
  /** Frames in the strip. */
  frames: ${N},
  /** One frame, in device px. The component scales to its own \`size\`. */
  frame: ${S},
  /**
   * The orbit the fish swims, as fractions of the frame. The component draws this as a
   * bordered view rather than baking it: the strip is tinted with ONE colour, and a ring
   * in the fish's colour is most of why the first version looked worse than the canvas
   * loader it replaced — that one was a steel-blue fish on a pale grey orbit.
   */
  ring: { radius: ${(ORBIT / S).toFixed(5)}, width: ${(2.4 / S).toFixed(5)} },
  /** Seconds for one full pass of the strip — a fifth of the orbit, five waves per lap. */
  cycleMs: ${FISH_SPRITE_CYCLE},
  /** Seconds for one orbit. cycleMs x 5, and they must stay in that ratio. */
  orbitMs: ${FISH_SPRITE_ORBIT},
  uri: "${uri}",
} as const;
`);
/**
 * A page that plays it.
 *
 * A contact sheet answers "is the fish drawn right"; it cannot answer "does it swim", and
 * that is the question the frames exist for. This runs the real composite — the strip
 * paged by `steps()` inside an element rotated by the orbit — so the loop, the seam and
 * the frame rate are all visible.
 *
 * It doubles as the reference for `FishSpinner.web.tsx`: the CSS below IS what that
 * component should do, and it needs no JS at all.
 */
const SIZES = [110, 72, 44, 28];
const figures = SIZES.map((px) =>
  `<figure><div class="fish" style="--s:${px}px"><div class="spin"><i></i></div></div>` +
  `<figcaption>${px}px</figcaption></figure>`).join("\n  ");

const html = fileURLToPath(new URL("../design/fish-sprite.html", import.meta.url));
writeFileSync(html, PAGE({ uri, frames: N, kb: (bytes.length / 1024).toFixed(1),
                           cycle: FISH_SPRITE_CYCLE, orbit: FISH_SPRITE_ORBIT, figures,
                           ring: ORBIT / S, ringW: 2.4 / S }));

console.log(`fish sprite  ${N} frames  ${W}x${S}  ${(bytes.length / 1024).toFixed(1)} KB`);
console.log(`  module  packages/ui-native/src/fish-sprite.generated.ts  (base64 data URI)`);
console.log(`  frames  design/fish-sprite.png   — contact sheet, one image per frame`);
console.log(`  MOVING  design/fish-sprite.html  — open this to watch it swim`);
