// T1072: which scenes need the store-shots build is one table; --web shoots the rest from a deployed site and says where each came from.
import { describe, expect, it } from 'vitest';
import fs from 'node:fs'; import path from 'node:path';
// @ts-expect-error plain .mjs tool, no types
import { needsBuild, NEEDS_STORE_BUILD } from './scenes.mjs';
// @ts-expect-error plain .mjs tool, no types
import { describeSite } from './site.mjs';

describe('store shot scenes and sites', () => {
  it('only the phone Spots and Help scenes need the build, and they are real scenes', () => {
    expect(Object.keys(NEEDS_STORE_BUILD).sort()).toEqual(['07-saved-spot', '20-help']);
    expect(needsBuild('07-saved-spot')).toBe(true); expect(needsBuild('01-map')).toBe(false); expect(needsBuild('toString')).toBe(false);
    const src = fs.readFileSync(path.join(__dirname, '../play-shots.mjs'), 'utf8');
    for (const k of Object.keys(NEEDS_STORE_BUILD)) expect(src).toContain(`'${k}':`);
    expect(src).not.toMatch(/'0[78]-saved-spot'.*needsBuild|\['07-saved-spot', '20-help'\]/);   // no second list of them
  });
  it('a site is described by its bundle, Last-Modified and any store-shots marker commit', async () => {
    const fake = async (u: string) => (u.endsWith('store-shots-build.json')
      ? { ok: false, text: async () => '', headers: new Map() }
      : { ok: true, text: async () => '<script src="/_expo/static/js/web/entry-abc123.js"></script>', headers: new Map([['last-modified', 'Tue, 24 Nov 2026 20:00:00 GMT']]) });
    expect(await describeSite('https://x.test/', fake)).toEqual({ site: 'https://x.test', bundle: 'entry-abc123.js', lastModified: 'Tue, 24 Nov 2026 20:00:00 GMT', storeShotsCommit: null });
    const marked = async (u: string) => (u.endsWith('store-shots-build.json') ? { ok: true, text: async () => '{"storeShots":true,"commit":"deadbeef0123"}' } : { ok: true, text: async () => '', headers: new Map() });
    expect((await describeSite('http://127.0.0.1:1', marked)).storeShotsCommit).toBe('deadbeef0123');
  });
});
