// T1072: the store rules the one set must meet, each refused when broken.
import { describe, expect, it } from 'vitest';
// @ts-expect-error plain .mjs tool, no types
import { captionProblems, setProblems, hasAlpha, captionProblem, SIZES } from './rules.mjs';
import fs from 'node:fs'; import path from 'node:path';

const png = (type: number) => { const b = Buffer.alloc(33); b.writeUInt32BE(0x89504e47, 0); b.writeUInt32BE(10, 16); b.writeUInt32BE(10, 20); b[24] = 8; b[25] = type; return b; };

describe('store set rules', () => {
  it('captions: rankings, prices, calls to download and time-bound words are refused', () => {
    expect(captionProblems({ scene: 's', title: 'Can I fish here today?', line: "Every B.C. lake and stream, coloured by today's rules" })).toEqual([]);
    for (const bad of ['The best fishing app', '#1 for anglers', 'Top rivers', 'New this week', 'Free forever', 'Download now', 'Only $4.99', 'Updated 2026', 'On Android', 'For iPhone', 'On Google Play'])
      expect(captionProblems({ scene: 's', title: bad, line: 'ok' }).length, bad).toBeGreaterThan(0);
  });
  it('the checked-in captions pass', () => {
    const { shots } = JSON.parse(fs.readFileSync(path.join(__dirname, '../store-shots.json'), 'utf8'));
    expect(shots.length).toBe(7);
    expect(shots.flatMap((s: { title: string; line: string; scene: string }) => captionProblems(s))).toEqual([]);
    expect(shots.every((s: { only?: string[] }) => !s.only)).toBe(true);   // the same frames for every target
  });
  it('sizes and counts: exact store sizes, Play 2-8 phone shots, App Store 1-10', () => {
    const [w, h] = SIZES['play-phone'];
    expect(setProblems('play-phone', { w, h }, 7)).toEqual([]);
    expect(setProblems('play-phone', { w, h }, 1)[0]).toMatch(/2-8/);
    expect(setProblems('play-phone', { w, h }, 9)[0]).toMatch(/2-8/);
    expect(setProblems('play-phone', { w: 1000, h }, 7)[0]).toMatch(/store's size/);
    expect(setProblems('appstore-6.9', { w: 1320, h: 2868 }, 11)[0]).toMatch(/1-10/);
    expect(setProblems('appstore-6.5', { w: 1242, h: 2688 }, 7)).toEqual([]);
    expect(setProblems('appstore-9', { w: 1, h: 1 }, 7)[0]).toMatch(/no known/);
  });
  it('the caption block over 20 % is refused, and an alpha channel is seen', () => {
    expect(captionProblem('play-phone', 0.2)).toBeNull();
    expect(captionProblem('play-phone', 0.2001)).toMatch(/at most 20/);
    expect(hasAlpha(png(2))).toBe(false); expect(hasAlpha(png(6))).toBe(true); expect(hasAlpha(png(4))).toBe(true);
  });
});
