// T137 — where a TALL shot of the Keep tab ends (the prototype's tall.mjs rule): `cards` ends at the lowest of the cards
// (keep-group / keep-plain) and the Other fish list under them (keep-otherfish), when the tab has one; `keep` ends at the
// Keep tab's foot. Pure, so the rule is tested without a browser; `END_IDS` are the blocks the page measures.
export const END_IDS = ['keep-group', 'keep-plain', 'keep-otherfish'];

/** bottoms: { 'keep-group': [..], 'keep-plain': [..], 'keep-otherfish': [..] } (CSS px), keepTab: the tab's bottom */
export function tallEnd(mode, bottoms, keepTab) {
  if (mode !== 'cards') return keepTab;
  const all = END_IDS.flatMap((k) => bottoms[k] ?? []);
  return all.length ? Math.max(...all) : keepTab;
}

/** the page-side expression: every END_IDS block's bottom, by test id */
export const END_BOTTOMS = `(() => Object.fromEntries(${JSON.stringify(END_IDS)}.map((k) =>
  [k, [...document.querySelectorAll('[data-testid="' + k + '"]')].map((e) => e.getBoundingClientRect().bottom)])))()`;
