// T138 — which block a reader's row frame (the reader frames; compare-regs.mjs, deleted in T87) puts under the pin bar: the Keep tab's
// row (`krow`: a kind row, a release row, a not-kept row) whose OWN picture strip carries the named fish, together with
// the tier head right above it when the row opens its tier (`khead`: "The Duncan River's rule: up to 5 rainbow trout a
// day … Its row covers its tributaries, so it applies here" is Glenn's rainbow row's title). It was a three-parent climb
// from the picture toward `keep-group`; on Chilliwack p2 the rainbow row sits beside the size box, so the climb
// overshot to the group's body and the frame opened on the box head with the rainbow row below the fold.
// DOM-only and pure: the same functions run in the page (`rowTopExpr`) and in a render test over a real Keep tab.
//
// (T810 R) THE TROUT TABLE (`TroutCard`, `keep-trout`): its rows are `KeepBox` rows (`keep-row`), each with ONE picture
// `pic:<row key>` (the fish code, or `<code>/<origin>` for a twin: `pic:ST/hatchery`), inside a keep box
// (`trout-box:<i>`) or a release / closed group (`trout-release`, `trout-closed`). The selectors match WHATEVER THE
// SCREEN SHOWS: the old group card's `krow` first, else the table's row; so they hold before the cutover (switch off:
// the old card) and after it (the table), with no flag. KeepBox's own bands and strip are `box-group` / `box-strip`
// (T810 cutover; they were `keep-group` / `keep-strip`), never a group card's: `cardGroups` leaves any inside a box out.
// the old group card's row and tier-head ids ('krow', 'khead': GroupCard, now drawn only in an Other fish sheet)
export const TABLE_ID = 'keep-trout';
/** (T1031) a table row whose picture and words are two presses (Identification | Sources of one species sheet) is the
 *  line holding both (`keep-row:line`: the picture's press `keep-row:id`, the words' `keep-row`); a row that is one press
 *  is that press (`keep-row`) */
export const TABLE_ROW = '[data-testid="keep-row:line"], [data-testid="keep-row"]';
/** a trout table block (a keep box, or the release / closed group) */
export const TABLE_BLOCK = '[data-testid^="trout-box:"], [data-testid="trout-release"], [data-testid="trout-closed"]';
/** any KeepBox (the salmon card's `keep-box` too): its bands of rows are never a group card */
export const KEEP_BOX = '[data-testid="keep-box"], [data-testid^="trout-box:"]';

/** the first `krow` under `root` whose pictures include `pic:<code>`, else the trout table's row of that fish
 *  (`pic:<code>` or a twin `pic:<code>/<origin>`), else null */
export function rowOf(root, code) {
  for (const r of root.querySelectorAll('[data-testid="krow"]'))
    if (r.querySelector(`[data-testid="pic:${code}"]`)) return r;
  for (const t of root.querySelectorAll(`[data-testid="${TABLE_ID}"]`))
    for (const r of t.querySelectorAll(TABLE_ROW)) {
      const k = r.querySelector('[data-testid^="pic:"]')?.getAttribute('data-testid')?.slice(4);
      if (k === code || k?.startsWith(`${code}/`)) return r;
    }
  return null;
}

/** the block the frame opens on: the row's tier head when the head directly precedes it (the row opens its tier); a
 *  table row that opens its box or group: the whole block (its strip "Keep 5 in all" / its column word); else the row */
export function blockOf(row) {
  if (row.matches(TABLE_ROW)) {
    const block = row.closest(TABLE_BLOCK);
    return block && block.querySelector(TABLE_ROW) === row ? block : row;
  }
  const prev = row.previousElementSibling;
  return prev && prev.getAttribute('data-testid') === 'khead' ? prev : row;
}

/** the old group cards (`GroupCard`'s `keep-group` article) under `root`, never a KeepBox's band of rows */
export function cardGroups(root) {
  return [...root.querySelectorAll('[data-testid="keep-group"]')].filter((g) => !g.closest(KEEP_BOX));
}

/** the page's trout entry as drawn: 'table' (the table, folded or open), 'card' (the old group card(s)), or null */
export function troutShown(root) {
  if (root.querySelector('[data-testid="glance:trout"]') || root.querySelector(`[data-testid="${TABLE_ID}"]`)) return 'table';
  return cardGroups(root).length || root.querySelector('[data-testid="keep-plain"]') ? 'card' : null;
}

/** the functions above as page source, for `Runtime.evaluate` (the browser tools): defines them in the page's scope */
export const IN_PAGE = [`const TABLE_ID = ${JSON.stringify(TABLE_ID)}, TABLE_ROW = ${JSON.stringify(TABLE_ROW)},`,
  `TABLE_BLOCK = ${JSON.stringify(TABLE_BLOCK)}, KEEP_BOX = ${JSON.stringify(KEEP_BOX)};`,
  rowOf.toString(), blockOf.toString(), cardGroups.toString(), troutShown.toString()].join('\n');
