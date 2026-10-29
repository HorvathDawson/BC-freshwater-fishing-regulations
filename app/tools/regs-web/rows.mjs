// T138 — which block a reader's row frame (the reader frames; compare-regs.mjs, deleted in T87) puts under the pin bar: the Keep tab's
// row (`krow`: a kind row, a release row, a not-kept row) whose OWN picture strip carries the named fish, together with
// the tier head right above it when the row opens its tier (`khead`: "The Duncan River's rule: up to 5 rainbow trout a
// day … Its row covers its tributaries, so it applies here" is Glenn's rainbow row's title). It was a three-parent climb
// from the picture toward `keep-group`; on Chilliwack p2 the rainbow row sits beside the size box, so the climb
// overshot to the group's body and the frame opened on the box head with the rainbow row below the fold.
// DOM-only and pure: the same functions run in the page (`rowTopExpr`) and in a render test over a real Keep tab.
export const ROW_ID = 'krow';
export const HEAD_ID = 'khead';

/** the first `krow` under `root` whose pictures include `pic:<code>`, else null */
export function rowOf(root, code) {
  for (const r of root.querySelectorAll('[data-testid="krow"]'))
    if (r.querySelector(`[data-testid="pic:${code}"]`)) return r;
  return null;
}

/** the block the frame opens on: the row's tier head when the head directly precedes it (the row opens its tier), else the row */
export function blockOf(row) {
  const prev = row.previousElementSibling;
  return prev && prev.getAttribute('data-testid') === 'khead' ? prev : row;
}
