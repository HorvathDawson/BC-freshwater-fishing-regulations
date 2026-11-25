// T1076: a site that is not a store-shots build is refused before any scene; the marker is what makes it one.
import { describe, expect, it } from 'vitest';
// @ts-expect-error plain .mjs tool, no types
import { assertStoreShotsSite, parseMarker } from './site.mjs';

const reply = (status: number, body: string) => async () => ({ ok: status < 400, status, text: async () => body });

describe('store shots site marker', () => {
  it('accepts the marker', () => { expect(parseMarker('{"storeShots":true,"commit":"abc"}').commit).toBe('abc'); });
  it('refuses a page, a missing flag, or nothing, saying what to do', () => {
    for (const t of ['<!doctype html><html></html>', '{}', '{"storeShots":false}', '', 'null']) expect(() => parseMarker(t, 'https://staging.canifishthis.ca')).toThrow(/not a store-shots web build[\s\S]*builds and serves its own export/);
  });
  it('a deployed site (404, or its page for every path) is refused; the build is accepted', async () => {
    await expect(assertStoreShotsSite('https://staging.canifishthis.ca', reply(404, 'nope'))).rejects.toThrow(/not a store-shots web build/);
    await expect(assertStoreShotsSite('https://staging.canifishthis.ca', reply(200, '<!doctype html>'))).rejects.toThrow(/not a store-shots web build/);
    await expect(assertStoreShotsSite('http://127.0.0.1:1', reply(200, '{"storeShots":true}'))).resolves.toMatchObject({ storeShots: true });
  });
  it('an unreachable site is said, not guessed', async () => {
    await expect(assertStoreShotsSite('http://x', async () => { throw new Error('ECONNREFUSED'); })).rejects.toThrow(/cannot reach/);
  });
});
