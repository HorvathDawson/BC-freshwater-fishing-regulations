/**
 * Dichromatic vision, simulated — so "is this palette colour-blind safe" is a measurement.
 *
 * The app ships a `cvd` theme. Whether it WORKS was, until this file, a matter of someone
 * looking at it and believing so, which is exactly the kind of claim that rots: the theme
 * was built by swapping the four status hues, and every later palette addition — the
 * stocking ramp, the donor identity hues, the flow ramp — inherited the light theme's
 * colours without anyone deciding they should.
 *
 * VIÉNOT–BRETTEL–MOLLON (1999) is the model, the one the accessibility tools use. Convert
 * to LMS (the cone responses), project onto the plane the missing cone leaves behind, and
 * come back. It is not a guess at what a dichromat "sees" — that is unknowable — but it is
 * the standard, reproducible answer to "which pairs of colours become the same signal",
 * which is the only question a palette has to survive.
 *
 * Separation is measured in ΔE2000, not in RGB distance: two hexes far apart in RGB can be
 * perceptually identical, and a palette check that cannot tell the difference is theatre.
 */

export type Deficiency = "protan" | "deutan" | "tritan";

/** sRGB byte -> linear. The gamma curve is not optional; skipping it moves ΔE by ~10. */
const toLinear = (c: number): number => {
  const s = c / 255;
  return s <= 0.04045 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
};

const toSrgb = (v: number): number => {
  const c = v <= 0.0031308 ? v * 12.92 : 1.055 * v ** (1 / 2.4) - 0.055;
  return Math.round(Math.min(1, Math.max(0, c)) * 255);
};

export function parseHex(hex: string): [number, number, number] {
  const h = hex.replace("#", "").trim();
  const full = h.length === 3 ? h.split("").map((c) => c + c).join("") : h;
  return [parseInt(full.slice(0, 2), 16), parseInt(full.slice(2, 4), 16),
          parseInt(full.slice(4, 6), 16)];
}

const hex = (r: number, g: number, b: number) =>
  `#${[r, g, b].map((v) => v.toString(16).padStart(2, "0")).join("")}`;

/** Linear RGB -> LMS (Hunt-Pointer-Estévez, as used by Viénot et al. 1999). */
const RGB_TO_LMS = [
  [0.31399022, 0.63951294, 0.04649755],
  [0.15537241, 0.75789446, 0.08670142],
  [0.01775239, 0.10944209, 0.87256922],
];
const LMS_TO_RGB = [
  [ 5.47221206, -4.6419601,  0.16963708],
  [-1.1252419,   2.29317094, -0.1678952],
  [ 0.02980165, -0.19318073, 1.16364789],
];

/**
 * The dichromat projection matrices, in LMS.
 *
 * Each collapses the colour space onto the plane spanned by the two remaining cones, which
 * is why two colours that differ ONLY along the missing axis come back identical.
 */
const PROJECT: Record<Deficiency, number[][]> = {
  protan: [[0, 1.05118294, -0.05116099], [0, 1, 0], [0, 0, 1]],
  deutan: [[1, 0, 0], [0.9513092, 0, 0.04866992], [0, 0, 1]],
  tritan: [[1, 0, 0], [0, 1, 0], [-0.86744736, 1.86727089, 0]],
};

const apply = (m: number[][], v: number[]): number[] =>
  m.map((row) => row[0]! * v[0]! + row[1]! * v[1]! + row[2]! * v[2]!);

/**
 * MACHADO, OLIVEIRA & FERNANDES (2009), severity 1.0 — the second model, in linear RGB.
 *
 * Viénot's single plane is known to be weakest for tritanopia, the deficiency it was not
 * built around. Machado's physiologically-based matrices disagree with it most exactly there,
 * so a palette that has to survive both is not resting on one model's blind spot.
 */
const MACHADO: Record<Deficiency, number[][]> = {
  protan: [[0.152286, 1.052583, -0.204868], [0.114503, 0.786281, 0.099216],
           [-0.003882, -0.048116, 1.051998]],
  deutan: [[0.367322, 0.860646, -0.227968], [0.280085, 0.672501, 0.047413],
           [-0.011820, 0.042940, 0.968881]],
  tritan: [[1.255528, -0.076749, -0.178779], [-0.078411, 0.930809, 0.147602],
           [0.004733, 0.691367, 0.303900]],
};

export type Model = "vienot" | "machado";
export const MODELS: readonly Model[] = ["vienot", "machado"];

/** What `colour` becomes for a dichromat of this type, under either model. */
export function simulate(colour: string, kind: Deficiency, model: Model = "vienot"): string {
  const [r, g, b] = parseHex(colour);
  const lin = [toLinear(r!), toLinear(g!), toLinear(b!)];
  const out = model === "machado" ? apply(MACHADO[kind], lin)
    : apply(LMS_TO_RGB, apply(PROJECT[kind], apply(RGB_TO_LMS, lin)));
  return hex(toSrgb(out[0]!), toSrgb(out[1]!), toSrgb(out[2]!));
}

/* ---------------------------------------------------------------- CIE Lab + ΔE2000 --- */

/** D65, 2°. */
function lab(colour: string): [number, number, number] {
  const [r, g, b] = parseHex(colour).map(toLinear) as [number, number, number];
  const x = (0.4124564 * r + 0.3575761 * g + 0.1804375 * b) / 0.95047;
  const y = (0.2126729 * r + 0.7151522 * g + 0.0721750 * b) / 1.0;
  const z = (0.0193339 * r + 0.1191920 * g + 0.9503041 * b) / 1.08883;
  const f = (t: number) => (t > 216 / 24389 ? Math.cbrt(t) : (841 / 108) * t + 4 / 29);
  const [fx, fy, fz] = [f(x), f(y), f(z)];
  return [116 * fy - 16, 500 * (fx - fy), 200 * (fy - fz)];
}

/** CIEDE2000. Long, but it is the only distance that matches how a person sorts colours. */
export function deltaE(a: string, b: string): number {
  const [L1, a1, b1] = lab(a);
  const [L2, a2, b2] = lab(b);
  const C1 = Math.hypot(a1, b1), C2 = Math.hypot(a2, b2);
  const Cb = (C1 + C2) / 2;
  const G = 0.5 * (1 - Math.sqrt(Cb ** 7 / (Cb ** 7 + 25 ** 7)));
  const ap1 = (1 + G) * a1, ap2 = (1 + G) * a2;
  const Cp1 = Math.hypot(ap1, b1), Cp2 = Math.hypot(ap2, b2);
  const deg = (r: number) => (r * 180) / Math.PI;
  const rad = (d: number) => (d * Math.PI) / 180;
  const hp = (bb: number, aa: number) => {
    if (bb === 0 && aa === 0) return 0;
    const h = deg(Math.atan2(bb, aa));
    return h >= 0 ? h : h + 360;
  };
  const hp1 = hp(b1, ap1), hp2 = hp(b2, ap2);
  const dL = L2 - L1, dC = Cp2 - Cp1;
  let dhp = 0;
  if (Cp1 * Cp2 !== 0) {
    dhp = hp2 - hp1;
    if (dhp > 180) dhp -= 360;
    else if (dhp < -180) dhp += 360;
  }
  const dH = 2 * Math.sqrt(Cp1 * Cp2) * Math.sin(rad(dhp) / 2);
  const Lb = (L1 + L2) / 2, Cpb = (Cp1 + Cp2) / 2;
  let hpb = hp1 + hp2;
  if (Cp1 * Cp2 !== 0) {
    if (Math.abs(hp1 - hp2) > 180) hpb += hp1 + hp2 < 360 ? 360 : -360;
    hpb /= 2;
  }
  const T = 1 - 0.17 * Math.cos(rad(hpb - 30)) + 0.24 * Math.cos(rad(2 * hpb))
            + 0.32 * Math.cos(rad(3 * hpb + 6)) - 0.20 * Math.cos(rad(4 * hpb - 63));
  const Sl = 1 + (0.015 * (Lb - 50) ** 2) / Math.sqrt(20 + (Lb - 50) ** 2);
  const Sc = 1 + 0.045 * Cpb;
  const Sh = 1 + 0.015 * Cpb * T;
  const Rt = -2 * Math.sqrt(Cpb ** 7 / (Cpb ** 7 + 25 ** 7))
             * Math.sin(rad(60 * Math.exp(-(((hpb - 275) / 25) ** 2))));
  return Math.sqrt((dL / Sl) ** 2 + (dC / Sc) ** 2 + (dH / Sh) ** 2
                   + Rt * (dC / Sc) * (dH / Sh));
}

/** WCAG relative luminance, and the 1..21 contrast ratio built from it. */
export function luminance(colour: string): number {
  const [r, g, b] = parseHex(colour).map(toLinear) as [number, number, number];
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

export function contrast(a: string, b: string): number {
  const [la, lb] = [luminance(a), luminance(b)];
  return (Math.max(la, lb) + 0.05) / (Math.min(la, lb) + 0.05);
}

export const DEFICIENCIES: readonly Deficiency[] = ["protan", "deutan", "tritan"];

/**
 * The worst ΔE between two colours across normal vision AND all three dichromacies.
 *
 * A palette is only as good as its worst reader, so every threshold in the test is applied
 * to this number rather than to the normal-vision distance.
 */
export function worstSeparation(a: string, b: string, models: readonly Model[] = ["vienot"]):
    { dE: number; under: string } {
  let worst = { dE: deltaE(a, b), under: "normal" };
  for (const m of models)
    for (const k of DEFICIENCIES) {
      const d = deltaE(simulate(a, k, m), simulate(b, k, m));
      if (d < worst.dE) worst = { dE: d, under: models.length > 1 ? `${k} (${m})` : k };
    }
  return worst;
}
