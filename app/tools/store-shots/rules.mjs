// STORE RULES FOR THE ONE SET (T1072): what Google Play and the App Store accept, and what the captions may say, checked in code so
// a reworded caption or a retuned size cannot slip past. Pure functions (unit-tested in rules.test.ts); store-shots.mjs calls them.
export const PLAY_CAPTION_MAX = 0.20;   // Play: the caption (title + line, padding above the card included) at most 20 % of the image height
// the exact pixel sizes each target must have
export const SIZES = { 'play-phone': [1080, 1920], 'appstore-6.9': [1320, 2868], 'appstore-6.5': [1242, 2688], 'appstore-6.3': [1206, 2622] };
const COUNTS = { play: [2, 8], appstore: [1, 10] };   // Play: 2-8 phone shots; App Store: 1-10 per set
// words a store refuses or that date a listing: rankings and superlatives, "new", "free", a call to download, prices, anything time-bound
export const BANNED = [/\bbest\b/i, /#\s*1\b/, /\btop\b/i, /\bnew\b/i, /\bfree\b/i, /\bdownload/i, /\bpremium\b/i, /\bnumber one\b/i,
  /[$€£]\s?\d|\b\d+\s?(?:usd|cad|dollars?)\b/i, /\b20\d\d\b/, /\b(?:limited|this (?:week|month|season|year)|for a limited|just launched|now available|coming soon)\b/i,
  /\b(?:android|ios|iphone|ipad|google play|app store|play store)\b/i];

/** the problems with a caption's words, [] when none */
export function captionProblems(shot) {
  return [shot.title, shot.line].flatMap((t, i) => BANNED.filter((re) => re.test(t ?? '')).map((re) => `${shot.scene}: the ${i ? 'line' : 'title'} "${t}" matches ${re} (stores refuse it, or it dates the listing)`));
}
/** the problems with the set a target would hold: its size, its count */
export function setProblems(name, t, count) {
  const out = [], want = SIZES[name], kind = name.startsWith('play') ? 'play' : 'appstore', [lo, hi] = COUNTS[kind];
  if (!want) out.push(`${name}: no known store size for this target`);
  else if (t.w !== want[0] || t.h !== want[1]) out.push(`${name}: ${t.w}x${t.h}, the store's size is ${want[0]}x${want[1]}`);
  if (kind === 'play' && (Math.min(t.w, t.h) < 320 || Math.max(t.w, t.h) > 3840)) out.push(`${name}: a Play side must be 320-3840 px (${t.w}x${t.h})`);
  if (kind === 'play' && Math.max(t.w, t.h) > 2 * Math.min(t.w, t.h)) out.push(`${name}: Play takes at most a 2:1 aspect (${t.w}x${t.h})`);
  if (name === 'play-phone' ? (count < lo || count > hi) : (kind === 'appstore' && (count < lo || count > hi))) out.push(`${name}: ${count} images, the store takes ${lo}-${hi}`);
  return out;
}
/** PNG header: [width, height, colour type]; colour type 2 = RGB, 0 = grey, 3 = palette: no alpha. 4 and 6 carry an alpha channel */
export const pngHead = (buf) => { if (buf.readUInt32BE(0) !== 0x89504e47) throw new Error('not a PNG'); return [buf.readUInt32BE(16), buf.readUInt32BE(20), buf[25]]; };
export const hasAlpha = (buf) => pngHead(buf)[2] === 4 || pngHead(buf)[2] === 6;
/** a caption block's share of the image height: its bottom edge (the top of the card) over the height */
export const captionShare = (cardTop, h) => cardTop / h;
export const captionProblem = (name, share) => (share > PLAY_CAPTION_MAX ? `${name}: the caption block takes ${(share * 100).toFixed(1)} % of the height, Play allows at most ${PLAY_CAPTION_MAX * 100} %` : null);
