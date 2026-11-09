#!/usr/bin/env node
/**
 * ICON PROPOSALS (owner: "better icons"). PROPOSALS ONLY: nothing here is wired into the app.
 *
 *   node handoff/PLAN/app-integration/icon-proposals/build.mjs
 *     -> <dir>/<direction>/{icon,foreground,background,monochrome}.svg   five launcher directions
 *     -> <dir>/markers/{current,proposed}-<kind>.svg                     the fish-obstacle map marks
 *     -> <dir>/icon-proposals.html                                       the comparison page (inline SVG, self-contained)
 *
 * Launcher art is on Android's adaptive canvas: 108 x 108, the visible mask is the central 72, and the art stays in the
 * central 66-diameter safe circle (radius 33 around 54,54). Colours are the app's own theme tokens (light theme values,
 * copied here as literals because this is a proposal page, not app code).
 */
import { writeFileSync, mkdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const dir = fileURLToPath(new URL("./", import.meta.url));
const repo = fileURLToPath(new URL("../../../../", import.meta.url));

// App tokens (packages/map/style/themes/light.json + dark.json).
const T = {
  card: "#FFFFFF", ink: "#15181C", page: "#EFEFEC", accent: "#5F26E0", accentDk: "#A97CFF",
  water: "#4A90E2", waterDk: "#38B6DE", route: "#2E62A8", outside: "#F8F8F2", outsideDk: "#0A0B0D",
  open: "#016890", own: "#D08404", closed: "#8F0006", forest: "#2F5E2A", inkDk: "#F0F2F0", cardDk: "#15181B",
  line2: "#D3D5CF", lineDk: "#252A2F",
};

const svg = (vb, body, w, h) =>
  `<svg xmlns="http://www.w3.org/2000/svg" viewBox="${vb}"${w ? ` width="${w}" height="${h ?? w}"` : ""}>${body}</svg>`;

/* ------------------------------------------------------------------ launcher directions */
// The river every direction shares (an S bend inside the safe circle).
const RIVER = "M38 26 C72 30 70 48 54 54 S36 78 70 82";

const DIRECTIONS = [
  {
    id: "1-sections",
    name: "Sections",
    line: "One river bend in three status colours, open, own rules and closed, split by gaps the way the map cuts a stream into sections. The most 'this app' of the five.",
    bg: `<rect width="108" height="108" fill="${T.outside}"/>`
      + `<path d="M8 20 C30 14 40 30 60 22 S92 18 104 26 M4 92 C26 84 44 98 66 90 S94 86 106 94" fill="none" stroke="${T.line2}" stroke-width="1.6"/>`,
    fg: (c = {}) => [
      [c.a ?? T.open, 0], [c.b ?? T.own, -35], [c.c ?? T.closed, -70],
    ].map(([col, off]) => `<path d="${RIVER}" pathLength="100" fill="none" stroke="${col}" stroke-width="12" stroke-dasharray="30 100" stroke-dashoffset="${off}"/>`).join(""),
    s: 0.78,
    mono: (k) => [0, -35, -70].map((off) => `<path d="${RIVER}" pathLength="100" fill="none" stroke="${k}" stroke-width="12" stroke-dasharray="30 100" stroke-dashoffset="${off}"/>`).join(""),
  },
  {
    id: "2-hook-river",
    name: "Hook river",
    line: "A fishing hook whose shank is a meandering river, light blue on the app's ink, over faint topo contours. It says 'fishing' without drawing a fish.",
    bg: `<rect width="108" height="108" fill="${T.ink}"/>`
      + [40, 31, 22].map((r, i) => `<ellipse cx="${60 - i * 2}" cy="${50 + i}" rx="${r + 8}" ry="${r}" fill="none" stroke="#2A3037" stroke-width="1.4"/>`).join(""),
    fg: (c = {}) => `<g fill="none" stroke="${c.a ?? T.waterDk}" stroke-width="7" stroke-linecap="round" stroke-linejoin="round">`
      + `<circle cx="60" cy="27" r="5"/>`
      + `<path d="M60 32 C70 40 50 46 60 54 C66 59 62 64 62 68 A13 13 0 0 1 36 68 L36 58"/>`
      + `<path d="M36 58 L43 64" stroke-width="6"/></g>`,
    s: 0.88,
    mono: (k) => null,
  },
  {
    id: "3-rule-card",
    name: "Rule card",
    line: "A square regulation card with the app's hard offset shadow, a river and a section tick on it, on the accent violet. Matches the brutalist UI (square corners, hairline ink, slab shadow).",
    bg: `<rect width="108" height="108" fill="${T.accent}"/>`,
    fg: (c = {}) => `<rect x="39" y="35" width="36" height="44" fill="${c.shadow ?? T.ink}"/>`
      + `<rect x="34" y="30" width="36" height="44" fill="${c.card ?? T.card}" stroke="${c.shadow ?? T.ink}" stroke-width="2.5"/>`
      + `<clipPath id="k3${c.id ?? ""}"><rect x="35.25" y="31.25" width="33.5" height="41.5"/></clipPath>`
      + `<path d="M42 30 C42 44 62 42 60 54 S48 66 52 74" fill="none" stroke="${c.a ?? T.water}" stroke-width="6" clip-path="url(#k3${c.id ?? ""})"/>`
      + `<path d="M50 52 H66" stroke="${c.tick ?? T.closed}" stroke-width="3.5"/>`,
    mono: (k) => `<mask id="m3"><rect width="108" height="108" fill="#fff"/>`
      + `<path d="M42 30 C42 44 62 42 60 54 S48 66 52 74" fill="none" stroke="#000" stroke-width="6"/>`
      + `<path d="M50 52 H66" stroke="#000" stroke-width="3.5"/></mask>`
      + `<rect x="39" y="35" width="36" height="44" fill="${k}" opacity=".45"/>`
      + `<rect x="34" y="30" width="36" height="44" fill="${k}" mask="url(#m3)"/>`,
  },
  {
    id: "4-valley",
    name: "Valley",
    line: "Two Coast Mountain peaks and a river widening out of the valley toward you, on the watershed green. The most 'BC' and the most scenic; least about regulations.",
    bg: `<rect width="108" height="108" fill="${T.forest}"/>`,
    s: 0.9,
    fg: (c = {}) => `<path d="M22 64 L40 36 L50 50 L62 28 L86 64 Z" fill="${c.peak ?? T.inkDk}"/>`
      + `<path d="M40 36 L45.5 44.5 L41 44 L36.5 41.5 Z M62 28 L69 39 L63.5 37 L58 40 Z" fill="${c.snow ?? "#B9C7BD"}"/>`
      + `<path d="M52 62 C50 68 58 70 54 76 C51 80 44 82 44 86 L66 86 C66 80 72 78 68 72 C64 68 58 68 56 62 Z" fill="${c.a ?? T.waterDk}"/>`,
    mono: (k) => `<path d="M22 64 L40 36 L50 50 L62 28 L86 64 Z" fill="${k}"/>`
      + `<path d="M52 66 C50 70 58 72 54 77 C51 80 45 82 45 85 L65 85 C65 80 71 78 67 73 C64 70 58 70 56 66 Z" fill="${k}"/>`,
  },
  {
    id: "5-pin-river",
    name: "Pin river",
    line: "The current icon's idea, a location pin, redrawn as one bold violet pin with a white river running through it. The safest evolution, and it stays recognisable to current users.",
    bg: `<rect width="108" height="108" fill="${T.card}"/>`,
    fg: (c = {}) => `<clipPath id="p5${c.id ?? ""}"><path d="M54 86 C44 72 32 62 32 48 A22 22 0 1 1 76 48 C76 62 64 72 54 86 Z"/></clipPath>`
      + `<path d="M54 86 C44 72 32 62 32 48 A22 22 0 1 1 76 48 C76 62 64 72 54 86 Z" fill="${c.a ?? T.accent}"/>`
      + `<path d="M38 30 C64 34 40 52 58 58 S72 70 60 90" fill="none" stroke="${c.river ?? T.card}" stroke-width="6.5" clip-path="url(#p5${c.id ?? ""})"/>`,
    mono: (k) => `<mask id="m5"><rect width="108" height="108" fill="#fff"/><path d="M38 30 C64 34 40 52 58 58 S72 70 60 90" fill="none" stroke="#000" stroke-width="6.5"/></mask>`
      + `<path d="M54 86 C44 72 32 62 32 48 A22 22 0 1 1 76 48 C76 62 64 72 54 86 Z" fill="${k}" mask="url(#m5)"/>`,
  },
];
// Hook river's monochrome is its foreground in one colour.
DIRECTIONS[1].mono = (k) => DIRECTIONS[1].fg({ a: k });

/** A direction's foreground / monochrome, scaled about the canvas centre by its own `s`. */
const sc = (d, body) => d.s ? `<g transform="translate(54 54) scale(${d.s}) translate(-54 -54)">${body}</g>` : body;
const FG = (d, c) => sc(d, d.fg(c));
const MONO = (d, k) => sc(d, d.mono(k));

/* The current icon: favicon.svg's own drawing, placed as build-icons.mjs places it (72 % of a white square). */
const fav = readFileSync(repo + "app/apps/mobile/assets/icon/favicon.svg", "utf8");
const favInner = /<svg[^>]*>([\s\S]*)<\/svg>/.exec(fav)[1].replace(/id="[^"]*"/g, "");
const CURRENT = {
  id: "current", name: "Current",
  bg: `<rect width="108" height="108" fill="${T.card}"/>`,
  // build-icons: the adaptive foreground is the art at 50 % of the canvas, centred.
  fg: () => `<g transform="translate(54 54) scale(${54 / 22.7}) translate(-12.06 -11.85)">${favInner}</g>`,
  mono: (k) => `<g transform="translate(54 54) scale(${54 / 22.7}) translate(-12.06 -11.85)">${favInner.replaceAll("#0EA5E9", k)}</g>`,
};

/* ------------------------------------------------------------------ crops */
// A superellipse (n = 4), the squircle most launchers use, inscribed in the visible 72 (18..90).
function squircle(cx = 54, cy = 54, r = 36, n = 4) {
  const pts = [];
  for (let i = 0; i < 96; i++) {
    const t = (i / 96) * 2 * Math.PI, c = Math.cos(t), s = Math.sin(t);
    pts.push([cx + r * Math.sign(c) * Math.abs(c) ** (2 / n), cy + r * Math.sign(s) * Math.abs(s) ** (2 / n)]);
  }
  return "M" + pts.map(([x, y]) => `${x.toFixed(2)} ${y.toFixed(2)}`).join(" L") + " Z";
}
const SQUIRCLE = squircle();
let uid = 0;
/** The icon as a launcher shows it: background + foreground, cropped to the mask, in the visible 72. */
function launcher(d, px, shape = "squircle", extra = {}) {
  const id = `c${uid++}`;
  const clip = shape === "circle" ? `<circle cx="54" cy="54" r="36"/>` : shape === "square" ? `<rect x="18" y="18" width="72" height="72"/>` : `<path d="${SQUIRCLE}"/>`;
  return svg("18 18 72 72", `<defs><clipPath id="${id}">${clip}</clipPath></defs><g clip-path="url(#${id})">${d.bg}${FG(d, { id, ...extra })}</g>`, px);
}
/** The Android 13 themed icon: the monochrome layer tinted by the wallpaper (Material You), on its tonal ground. */
function themed(d, px, dark = false) {
  const id = `t${uid++}`;
  const [bg, fg] = dark ? ["#2B3442", "#C9D6EA"] : ["#D9E3F4", "#1F3552"];
  const m = MONO(d, fg).replaceAll(/id="(m\d)"/g, `id="$1${id}"`).replaceAll(/url\(#(m\d)\)/g, `url(#$1${id})`);
  return svg("18 18 72 72", `<defs><clipPath id="${id}"><circle cx="54" cy="54" r="36"/></clipPath></defs><g clip-path="url(#${id})"><rect width="108" height="108" fill="${bg}"/>${m}</g>`, px);
}
/** The safe-zone diagram: the full 108 canvas, the 72 mask and the 66 safe circle drawn over it. */
function safeZone(d, px) {
  return svg("0 0 108 108", `${d.bg}${FG(d, { id: `s${uid++}` })}`
    + `<rect x="18" y="18" width="72" height="72" fill="none" stroke="#E0218A" stroke-width=".8" stroke-dasharray="2 2"/>`
    + `<circle cx="54" cy="54" r="33" fill="none" stroke="#E0218A" stroke-width=".8"/>`, px);
}

/* ------------------------------------------------------------------ markers */
// CURRENT: @app/core obstacleIcons.ts OBSTACLE_ICON_SHAPES, verbatim (16 grid), drawn as obstacleIconSvg draws it.
const W = 1.7;
const CUR = {
  falls: [{ stroke: [[1.5, 3], [14.5, 3]], w: 2 }, { stroke: [[4.5, 6.5], [4.5, 9.5]], w: W }, { stroke: [[8, 6.5], [8, 13]], w: W }, { stroke: [[11.5, 6.5], [11.5, 10.5]], w: W }],
  cascade: [{ stroke: [[2, 3], [5.5, 3], [5.5, 7.5], [10, 7.5], [10, 12], [14, 12]], w: W }],
  canyon: [{ fill: [[1.5, 2], [5.8, 2], [4.8, 5.5], [6, 8], [4.6, 11], [5.4, 14], [1.5, 14]] }, { fill: [[10.2, 2], [14.5, 2], [14.5, 14], [10.6, 14], [11.4, 11], [10, 8], [11.2, 5.5]] }],
  velocity: [{ stroke: [[3, 3.5], [7, 8], [3, 12.5]], w: W }, { stroke: [[8.5, 3.5], [12.5, 8], [8.5, 12.5]], w: W }],
  dam: [{ fill: [[1.5, 3], [14.5, 3], [14.5, 8], [1.5, 8]] }, { stroke: [[1.5, 12.5], [4.2, 10.8], [6.9, 12.5], [9.6, 10.8], [12.3, 12.5], [14.5, 11.2]], w: 1.4 }],
  culvert: [{ stroke: [[2, 3.5], [10, 3.5]], w: W }, { stroke: [[2, 12.5], [10, 12.5]], w: W }, { ring: [10, 8, 3.3, 4.5], w: W }],
  other: [{ disc: [8, 8, 3.4] }],
};
const HALO = 1.4;
function currentMarker(kind, ink, halo, size = 16) {
  const els = (colour, grow) => CUR[kind].map((s) => {
    const pts = (p) => p.map(([x, y], i) => `${i ? "L" : "M"}${x} ${y}`).join(" ");
    if (s.stroke) return `<path d="${pts(s.stroke)}" fill="none" stroke="${colour}" stroke-width="${s.w + grow}" stroke-linecap="round" stroke-linejoin="round"/>`;
    if (s.fill) return `<path d="${pts(s.fill)} Z" fill="${colour}" stroke="${colour}" stroke-width="${grow}" stroke-linejoin="round"/>`;
    if (s.ring) return `<ellipse cx="${s.ring[0]}" cy="${s.ring[1]}" rx="${s.ring[2]}" ry="${s.ring[3]}" fill="none" stroke="${colour}" stroke-width="${s.w + grow}"/>`;
    return `<circle cx="${s.disc[0]}" cy="${s.disc[1]}" r="${s.disc[2] + grow / 2}" fill="${colour}"/>`;
  }).join("");
  return svg("0 0 16 16", els(halo, HALO * 2) + els(ink, 0), size);
}

/*
 * PROPOSED: one grid (16, live area 2..14), one stroke (2.0, square caps, miter joins: the app's square corners, and a
 * corner that stays a corner when a 3x raster is shown at 1x), one halo (1.5 paper). Two tiers by SHAPE, matching the
 * existing sortOrder: a full barrier (dam, falls) is a solid ink plate with the glyph cut out in paper; a partial or
 * possible barrier (canyon, cascade, velocity, culvert) is the ink glyph alone on the halo. Unknown is a small diamond,
 * so no obstacle is ever a dot (a dot is a gauge on this map).
 */
const PROP = {
  dam:      { plate: true, ink: `<path d="M3.5 11.5 L5.5 9.75 L8 11.5 L10.5 9.75 L12.5 11.5" stroke-width="1.6"/>`, fill: `<rect x="3" y="4" width="10" height="3"/>` },
  falls:    { plate: true, ink: `<path d="M2.5 5 H8.5 V12.5"/><path d="M11.5 7.5 V11"/>` },
  canyon:   { fill: `<path d="M2 2 H6.2 L5.2 5.5 L6.4 8 L5 11 L5.8 14 H2 Z"/><path d="M9.8 2 H14 V14 H10.2 L11 11 L9.6 8 L10.8 5.5 Z"/>` },
  cascade:  { ink: `<path d="M2 3.5 H6 V7.5 H10 V11.5 H14"/>` },
  velocity: { ink: `<path d="M3 3.5 L7 8 L3 12.5"/><path d="M8.5 3.5 L12.5 8 L8.5 12.5"/>` },
  culvert:  { ink: `<circle cx="8" cy="8" r="5"/><path d="M5 10.5 H11"/>` },
  other:    { fill: `<path d="M8 4.5 L11.5 8 L8 11.5 L4.5 8 Z"/>` },
};
const SW = 2, PH = 1.5;
function proposedMarker(kind, ink, halo, size = 16) {
  const p = PROP[kind];
  const strokes = (col, w) => p.ink ? `<g fill="none" stroke="${col}" stroke-width="${w}" stroke-linecap="square" stroke-linejoin="miter">${p.ink}</g>` : "";
  const fills = (col, grow) => p.fill ? `<g fill="${col}" stroke="${col}" stroke-width="${grow}" stroke-linejoin="miter">${p.fill}</g>` : "";
  let body;
  if (p.plate) {
    // halo frame, ink plate, glyph knocked out in the halo colour
    body = `<rect x=".5" y=".5" width="15" height="15" fill="${halo}"/><rect x="1.75" y="1.75" width="12.5" height="12.5" fill="${ink}"/>`
      + strokes(halo, SW) + fills(halo, 0.001);
  } else {
    body = strokes(halo, SW + PH * 2) + fills(halo, PH * 2) + strokes(ink, SW) + fills(ink, 0.001);
  }
  return svg("0 0 16 16", body, size);
}
const KINDS = ["dam", "falls", "canyon", "cascade", "velocity", "culvert", "other"];
const KIND_WORD = { dam: "Dam", falls: "Falls", canyon: "Canyon", cascade: "Cascade", velocity: "Velocity barrier", culvert: "Culvert", other: "Other / unknown" };

/* ------------------------------------------------------------------ write SVG files */
const bare = (s) => s.replace("<svg ", '<svg width="1024" height="1024" ');
for (const d of DIRECTIONS) {
  mkdirSync(dir + d.id, { recursive: true });
  writeFileSync(`${dir}${d.id}/background.svg`, bare(svg("0 0 108 108", d.bg)) + "\n");
  writeFileSync(`${dir}${d.id}/foreground.svg`, bare(svg("0 0 108 108", FG(d, { id: "f" }))) + "\n");
  writeFileSync(`${dir}${d.id}/monochrome.svg`, bare(svg("0 0 108 108", MONO(d, "#000000"))) + "\n");
  // the full-bleed square (iOS / Play 512 / icon-1024): the visible 72 of the adaptive canvas
  writeFileSync(`${dir}${d.id}/icon.svg`, bare(svg("18 18 72 72", d.bg + FG(d, { id: "i" }))) + "\n");
}
mkdirSync(dir + "markers", { recursive: true });
for (const k of KINDS) {
  writeFileSync(`${dir}markers/current-${k}.svg`, currentMarker(k, "#0A1116", "#F8F8F2") + "\n");
  writeFileSync(`${dir}markers/proposed-${k}.svg`, proposedMarker(k, "#0A1116", "#F8F8F2") + "\n");
}

/* ------------------------------------------------------------------ page */
const appGrid = (dark) => {
  const tones = dark ? ["#3A4250", "#4C3B52", "#2F4A44", "#51483A", "#394A5E", "#4A3A3A", "#3E4A33"] : ["#F2B8A0", "#A8D5E2", "#C9E4A6", "#F5D58A", "#C7B8EA", "#9FC9B9", "#E8A8C0"];
  return tones.map((t) => `<div class="app"><div class="ph" style="background:${t}"></div><span></span></div>`);
};
function homeScreen(d, dark) {
  const cells = appGrid(dark);
  cells.splice(5, 0, `<div class="app"><div class="ic">${launcher(d, 48)}</div><span>Can I Fish</span></div>`);
  return `<div class="phone ${dark ? "dk" : "lt"}"><div class="status">9:41</div><div class="grid">${cells.join("")}</div>`
    + `<div class="dock">${[0, 1, 2, 3].map(() => `<div class="ph"></div>`).join("")}</div></div>`;
}
function directionBlock(d, isCurrent = false) {
  return `<section class="dir${isCurrent ? " cur" : ""}">
  <header><h2>${isCurrent ? "Current" : d.name}</h2><p>${isCurrent ? "Sky-blue line fish with a location pin on white (favicon.svg via build-icons.mjs). Thin 2-unit lines in a 24 box: at 48 px the fish outline is ~1.5 px and the pin's hole closes up." : d.line}</p>
  ${isCurrent ? "" : `<code>icon-proposals/${d.id}/</code>`}</header>
  <div class="row">
    <figure class="big">${launcher(d, 512, "square")}<figcaption>512 (Play listing, square)</figcaption></figure>
    <div class="col">
      <div class="sizes">
        <figure>${launcher(d, 96)}<figcaption>96</figcaption></figure>
        <figure>${launcher(d, 48)}<figcaption>48</figcaption></figure>
        <figure>${launcher(d, 24)}<figcaption>24</figcaption></figure>
      </div>
      <div class="sizes">
        <figure>${launcher(d, 96, "circle")}<figcaption>circle mask</figcaption></figure>
        <figure>${launcher(d, 96, "squircle")}<figcaption>squircle mask</figcaption></figure>
        <figure>${safeZone(d, 96)}<figcaption>108 canvas · 72 mask · 66 safe</figcaption></figure>
      </div>
      <div class="sizes">
        <figure>${themed(d, 96)}<figcaption>monochrome, themed light</figcaption></figure>
        <figure>${themed(d, 96, true)}<figcaption>themed dark</figcaption></figure>
        <figure>${themed(d, 48, true)}<figcaption>48</figcaption></figure>
      </div>
      <div class="homes">${homeScreen(d, false)}${homeScreen(d, true)}</div>
    </div>
  </div>
</section>`;
}

// Map backgrounds for the markers: light basemap, dark basemap, an imagery-like ground (turbulence, no external image).
function mapScene(kindFn, theme, w = 360, h = 150, idp = "") {
  const lt = theme === "light", img = theme.startsWith("img");
  const ink = theme === "img-dark" || theme === "dark" ? "#F4F8F5" : "#0A1116";
  const halo = theme === "img-dark" || theme === "dark" ? "#0A0B0D" : "#F8F8F2";
  const fid = `turb${idp}${uid++}`;
  const ground = img
    ? `<filter id="${fid}"><feTurbulence type="fractalNoise" baseFrequency=".035" numOctaves="4" seed="7"/><feColorMatrix values="0 0 0 0 .18  0 0 0 0 .26  0 0 0 0 .14  1.1 0 0 0 0"/></filter>`
      + `<rect width="${w}" height="${h}" fill="#4E5A3A"/><rect width="${w}" height="${h}" filter="url(#${fid})"/>`
      + `<path d="M0 30 C80 60 140 20 220 70 S320 120 ${w} 100 L${w} ${h} L0 ${h} Z" fill="#6B6A4E" opacity=".35"/>`
    : `<rect width="${w}" height="${h}" fill="${lt ? "#F2F1EA" : "#121417"}"/>`
      + `<path d="M0 120 C60 100 90 140 160 128 S270 90 ${w} 110" fill="none" stroke="${lt ? "#E2E4DA" : "#1E2328"}" stroke-width="18"/>`;
  const river = `<path d="M-5 70 C60 50 100 95 170 78 S280 40 ${w + 5} 64" fill="none" stroke="${img ? "#2D4D5C" : lt ? T.open : "#A7FBB2"}" stroke-width="${img ? 6 : 3}"/>`
    + `<path d="M120 0 C130 30 150 50 160 80" fill="none" stroke="${img ? "#2D4D5C" : lt ? T.own : "#FFD307"}" stroke-width="${img ? 3 : 2}"/>`
    + `<path d="M250 150 C255 110 262 80 270 52" fill="none" stroke="${img ? "#2D4D5C" : lt ? T.closed : "#FF3B6D"}" stroke-width="${img ? 3 : 2}"/>`;
  const at = [[34, 62], [82, 64], [128, 76], [161, 77], [206, 70], [262, 64], [318, 60]];
  const marks = KINDS.map((k, i) => `<g transform="translate(${at[i][0] - 8} ${at[i][1] - 8})">${kindFn(k, ink, halo, 16)}</g>`).join("");
  return svg(`0 0 ${w} ${h}`, ground + river + marks, w, h);
}

const SCENES = [["light", "Light basemap"], ["dark", "Dark basemap"], ["img-light", "Imagery, light-theme ink"], ["img-dark", "Imagery, dark-theme ink"]];

const markerTable = KINDS.map((k) => `<tr><th>${KIND_WORD[k]}</th>
  <td>${currentMarker(k, "#0A1116", "#F8F8F2", 14)} ${currentMarker(k, "#0A1116", "#F8F8F2", 16)} ${currentMarker(k, "#0A1116", "#F8F8F2", 18)}<span class="zoom">${currentMarker(k, "#0A1116", "#F8F8F2", 48)}</span></td>
  <td>${proposedMarker(k, "#0A1116", "#F8F8F2", 14)} ${proposedMarker(k, "#0A1116", "#F8F8F2", 16)} ${proposedMarker(k, "#0A1116", "#F8F8F2", 18)}<span class="zoom">${proposedMarker(k, "#0A1116", "#F8F8F2", 48)}</span></td>
  <td class="note">${{
    dam: "Current: bar + wavy water. Proposed: solid plate (full barrier) with the wall across and water below cut out.",
    falls: "Current: ledge + three drips. Proposed: plate, ledge and drop, with spray.",
    canyon: "Two walls with a gap, kept. Proposed: slightly heavier walls, miter corners.",
    cascade: "Stepped run, kept. Proposed: 2.0 square-cap stroke, even steps.",
    velocity: "Double chevron, kept. Proposed: miter points stay sharp at 14 px.",
    culvert: "Current: pipe in perspective (ellipse + lines, reads as a 'D' at 14 px). Proposed: pipe end-on, water line inside.",
    other: "Current: a dot, which reads as a gauge. Proposed: a small diamond.",
  }[k]}</td></tr>`).join("");

const css = `
:root{--bg:#EFEFEC;--card:#FFFFFF;--ink:#15181C;--sub:#5B6168;--line:#D3D5CF;--accent:#5F26E0}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){--bg:#0A0B0D;--card:#15181B;--ink:#F0F2F0;--sub:#9AA1A8;--line:#343B42;--accent:#A97CFF}}
:root[data-theme="dark"]{--bg:#0A0B0D;--card:#15181B;--ink:#F0F2F0;--sub:#9AA1A8;--line:#343B42;--accent:#A97CFF}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.45 -apple-system,system-ui,"Segoe UI",Roboto,sans-serif}
main{max-width:1240px;margin:0 auto;padding:24px 16px 64px}
h1{font-size:28px;margin:0 0 4px}h2{font-size:20px;margin:0}p{margin:4px 0;color:var(--sub);max-width:820px}
code{font:12px ui-monospace,Menlo,monospace;color:var(--accent)}
.lede{margin-bottom:28px}
section{background:var(--card);border:1px solid var(--ink);box-shadow:5px 5px 0 var(--ink);padding:16px;margin:0 0 28px}
section.cur{box-shadow:none;border-style:dashed}
header{margin-bottom:12px}
.row{display:flex;gap:20px;flex-wrap:wrap}
.big svg{width:min(512px,100%);height:auto;border:1px solid var(--line);display:block}
.col{flex:1;min-width:280px;display:flex;flex-direction:column;gap:14px}
.sizes{display:flex;gap:18px;align-items:flex-end;flex-wrap:wrap}
figure{margin:0;display:flex;flex-direction:column;align-items:center;gap:4px}
figcaption{font-size:11px;color:var(--sub);text-align:center;max-width:120px}
.homes{display:flex;gap:14px;flex-wrap:wrap}
.phone{width:200px;height:300px;padding:10px 12px;position:relative;border:6px solid #111;display:flex;flex-direction:column}
.phone.lt{background:linear-gradient(160deg,#cfe3f1,#e9e2d0 60%,#d7e6c9)}
.phone.dk{background:linear-gradient(160deg,#1b2433,#232031 60%,#14201a)}
.status{font-size:10px;color:#222;margin-bottom:16px}.dk .status{color:#ddd}
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px 6px}
.app{display:flex;flex-direction:column;align-items:center;gap:3px}
.app .ph,.app .ic{width:36px;height:36px}.app .ph{border-radius:30%}
.app .ic svg{width:36px;height:36px}
.app span{font-size:8px;color:#222;height:10px;white-space:nowrap}.dk .app span{color:#eee}
.dock{position:absolute;left:12px;right:12px;bottom:12px;display:flex;justify-content:space-around}
.dock .ph{width:32px;height:32px;border-radius:30%;background:rgba(255,255,255,.55)}.dk .dock .ph{background:rgba(255,255,255,.14)}
table{border-collapse:collapse;width:100%}th,td{border-bottom:1px solid var(--line);padding:8px 6px;text-align:left;vertical-align:middle}
td svg{vertical-align:middle;margin-right:6px}.zoom{display:inline-block;margin-left:10px;padding:4px;background:#F2F1EA;border:1px solid var(--line)}
td.note{font-size:12px;color:var(--sub);max-width:340px}
.scenes{display:grid;grid-template-columns:repeat(auto-fit,minmax(380px,1fr));max-width:860px;gap:14px}
.scene svg{display:block;max-width:100%;height:auto;border:1px solid var(--line)}
.scene h3{font-size:13px;margin:0 0 4px;font-weight:600}
.scene .pair{display:flex;flex-direction:column;gap:6px}
.lbl{font-size:11px;color:var(--sub)}
ul{margin:6px 0 0 18px;padding:0;color:var(--sub)}li{margin:2px 0}
.scroll{overflow-x:auto}
`;

const page = `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Icon Proposals</title><style>${css}</style></head><body><main>
<h1>Icon proposals</h1>
<p class="lede">Five launcher directions and one new set of map marks, for the owner to pick from. Nothing here is wired into the app. The colours are the app's own theme tokens. Launcher art sits on Android's 108 adaptive canvas: the launcher shows the central 72, and the art stays inside the 66-diameter safe circle, so a circle, squircle or square mask never cuts it. The SVGs are in <code>handoff/PLAN/app-integration/icon-proposals/</code>, made by <code>build.mjs</code>.</p>

<h2 style="margin:8px 0 12px">1 · Launcher icon</h2>
${directionBlock(CURRENT, true)}
${DIRECTIONS.map((d) => directionBlock(d)).join("\n")}

<h2 style="margin:8px 0 12px">2 · Map marks: fish obstacles</h2>
<section>
<header><p>The per-type map marks are the fish obstacles (T173, owner 2026-10-28: "small, nice icons per type"). They are drawn from one table, <code>@app/core obstacleIcons.ts</code> <code>OBSTACLE_ICON_SHAPES</code> (a 16 grid), rasterised per theme by <code>@app/map obstacle-icons.ts</code> (3x, signed-distance AA, <code>addImage</code> / native <code>Images</code>), and named in the style by <code>layers.source.json</code> layer <code>obstacle</code> (<code>icon.images</code> by <code>type</code>, emitted by <code>tools/build-style.mjs</code>). The tap sheet and Layers key draw the same table (<code>ObstacleIcon</code> in <code>ui-native icons.tsx</code>). The other point marks are single-look marks, not per-type: gauge dot/pill (<code>gaugeDots.ts</code>), saved spot and parking square (<code>spot-marks.ts</code>), boundary marks, tap marker and pins (<code>point-marks.ts</code>).</p>
<p><b>The proposed set.</b> The grid stays at 16 (live area 2–14) and the ink and halo stay neutral (<code>color.obstacle.mark</code> / <code>.ring</code>), so the rule that colour on this map is an answer still holds. Changes:</p>
<ul>
<li><b>One stroke:</b> 2.0 with square caps and miter joins, where the current set mixes 1.4, 1.7 and 2.</li>
<li><b>Two tiers by shape.</b> A full barrier (dam, falls) is a solid ink plate with the glyph cut out. A partial barrier (canyon, cascade, velocity, culvert) is the bare glyph. The tiers follow the existing sortOrder, and you can tell them apart without reading the glyph.</li>
<li><b>Unknown becomes a diamond.</b> A dot is a gauge on this map.</li>
<li><b>Halo 1.5.</b> It is up from 1.4, so the mark holds on imagery.</li>
</ul>
<p><b>Why not Lucide's look</b> (round caps, stroke 1.75). At 14 px under a halo, round caps and joins blur into blobs: the cascade's steps and the chevrons' points go soft. Square ends match the app's square corners and stay crisp when a 3x raster is shown at 1x. A filled plate is also the only thing that stays visible over busy satellite texture.</p></header>
<div class="scroll"><table><thead><tr><th>Type</th><th>Current · 14 / 16 / 18 px · 3x</th><th>Proposed · 14 / 16 / 18 px · 3x</th><th></th></tr></thead><tbody>${markerTable}</tbody></table></div>
</section>
<section>
<header><p>At real map size (16 px) on a river with its sections. Left to right: dam, falls, canyon, cascade, velocity, culvert, unknown. Each pair is current above proposed.</p></header>
<div class="scenes">${SCENES.map(([t, label]) => `<div class="scene"><h3>${label}</h3><div class="pair"><span class="lbl">current</span>${mapScene(currentMarker, t, 360, 150, "c")}<span class="lbl">proposed</span>${mapScene(proposedMarker, t, 360, 150, "p")}</div></div>`).join("")}</div>
</section>

<h2 style="margin:8px 0 12px">Wiring a chosen set in (not done)</h2>
<section><ul>
<li><b>Launcher:</b> <code>app/tools/build-icons.mjs</code> composes everything from one <code>favicon.svg</code>. A chosen direction has separate background and foreground layers, so the script would read <code>background.svg</code>, <code>foreground.svg</code> and <code>monochrome.svg</code> instead of recolouring one drawing. It then writes icon-1024, play-512, adaptive-foreground, adaptive-monochrome, favicon-48 and the feature graphic. <code>app.config.js</code> sets the adaptive background colour from <code>color.ui.card</code>, so it would need the direction's ground instead, or a background image. The web favicon (<code>favicon.svg</code>, which is also the site's) changes with it.</li>
<li><b>Markers:</b> only data. Replace <code>OBSTACLE_ICON_SHAPES</code> in <code>app/packages/core/src/obstacleIcons.ts</code>. The rasteriser in <code>app/packages/map/src/obstacle-icons.ts</code> needs a "plate" shape kind, an inverted knockout, and a square-cap/miter stroke distance; today strokes are round by construction. Also update <code>OBSTACLE_ICON_HALO</code> and the <code>ObstacleIcon</code> SVG in <code>ui-native</code> (via <code>obstacleIconSvg</code>), and re-pin <code>obstacleIcons.test.ts</code> / <code>obstacle-icons.test.ts</code>. No tile, style or sprite change: the image names stay <code>obstacle-&lt;type&gt;</code>.</li>
</ul></section>
</main></body></html>`;
writeFileSync(dir + "icon-proposals.html", page);
console.log("wrote", dir + "icon-proposals.html");
