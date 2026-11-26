// (T1131) the workflow's size check names a wrong-sized or missing picture
import { describe, expect, it } from 'vitest';
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path';
// @ts-expect-error plain .mjs tool, no types
import { sizeProblems } from './check-sizes.mjs';

const png = (w: number, h: number) => { const b = Buffer.alloc(24); b.writeUInt32BE(w, 16); b.writeUInt32BE(h, 20); return b; };
const cfg = { shots: [{ scene: 'a' }, { scene: 'b' }], targets: { t1: { w: 10, h: 20 }, t2: { w: 30, h: 40 } } };

describe('sizeProblems', () => {
  it('passes exact sizes, flags a wrong size and a short set, honours only', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'sz-'));
    for (const t of ['t1', 't2']) fs.mkdirSync(path.join(root, t));
    fs.writeFileSync(path.join(root, 't1/01-a.png'), png(10, 20)); fs.writeFileSync(path.join(root, 't1/02-b.png'), png(10, 20));
    fs.writeFileSync(path.join(root, 't2/01-a.png'), png(30, 41));
    expect(sizeProblems(root, cfg, ['t1'])).toEqual([]);
    expect(sizeProblems(root, cfg)).toEqual(['t2: 1 pictures, the list has 2', 't2/01-a.png: 30x41, the target declares 30x40']);
    fs.rmSync(root, { recursive: true });
  });
});
