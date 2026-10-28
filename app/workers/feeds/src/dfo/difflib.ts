// T225 DFO port: difflib.SequenceMatcher(None, a, b).ratio() exactly (CPython 3.13), autojunk included — a scope
// of 200+ characters drops its "popular" characters from b2j, which changes the score, and the score decides
// drift vs new (entries._candidates, >= 0.5) and the month repair in typed (get_close_matches, cutoff 0.8).

export function ratio(aStr: string, bStr: string): number {
  const a = Array.from(aStr);
  const b = Array.from(bStr);
  const la = a.length;
  const lb = b.length;
  if (la + lb === 0) return 1.0;
  // __chain_b
  const b2j = new Map<string, number[]>();
  for (let j = 0; j < lb; j++) {
    const k = b[j]!;
    const l = b2j.get(k);
    if (l) l.push(j);
    else b2j.set(k, [j]);
  }
  if (lb >= 200) {
    const ntest = Math.floor(lb / 100) + 1;
    for (const [k, idxs] of [...b2j]) if (idxs.length > ntest) b2j.delete(k);
  }
  const nothing: number[] = [];
  const longest = (alo: number, ahi: number, blo: number, bhi: number): [number, number, number] => {
    let besti = alo;
    let bestj = blo;
    let bestsize = 0;
    let j2len = new Map<number, number>();
    for (let i = alo; i < ahi; i++) {
      const newj2len = new Map<number, number>();
      for (const j of b2j.get(a[i]!) ?? nothing) {
        if (j < blo) continue;
        if (j >= bhi) break;
        const k = (j2len.get(j - 1) ?? 0) + 1;
        newj2len.set(j, k);
        if (k > bestsize) {
          besti = i - k + 1;
          bestj = j - k + 1;
          bestsize = k;
        }
      }
      j2len = newj2len;
    }
    // no isjunk: bjunk is empty, so only the "not junk" extensions can move
    while (besti > alo && bestj > blo && a[besti - 1] === b[bestj - 1]) {
      besti--;
      bestj--;
      bestsize++;
    }
    while (besti + bestsize < ahi && bestj + bestsize < bhi && a[besti + bestsize] === b[bestj + bestsize]) bestsize++;
    return [besti, bestj, bestsize];
  };
  let matches = 0;
  const queue: [number, number, number, number][] = [[0, la, 0, lb]];
  while (queue.length) {
    const [alo, ahi, blo, bhi] = queue.pop()!;
    const [i, j, k] = longest(alo, ahi, blo, bhi);
    if (k) {
      matches += k;
      if (alo < i && blo < j) queue.push([alo, i, blo, j]);
      if (i + k < ahi && j + k < bhi) queue.push([i + k, ahi, j + k, bhi]);
    }
  }
  return (2.0 * matches) / (la + lb);
}

/** difflib.get_close_matches(word, possibilities, n, cutoff) */
export function getCloseMatches(word: string, possibilities: readonly string[], n = 3, cutoff = 0.6): string[] {
  const scored: [number, string][] = [];
  for (const x of possibilities) {
    const r = ratio(x, word); // SequenceMatcher: seq1 = x, seq2 = word
    if (r >= cutoff) scored.push([r, x]);
  }
  // heapq.nlargest(n, result): by score, then by the string (tuple order), largest first
  scored.sort((p, q) => (q[0] - p[0]) || (q[1] < p[1] ? -1 : q[1] > p[1] ? 1 : 0));
  return scored.slice(0, n).map((x) => x[1]);
}
