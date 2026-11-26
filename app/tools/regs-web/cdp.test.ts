// (T1131) CHROME_FLAGS: extra headless-Chrome flags (software GL on the store-shots runner); nothing by default
import { describe, expect, it } from 'vitest';
// @ts-expect-error plain .mjs tool, no types
import { extraFlags } from './cdp.mjs';

describe('extraFlags', () => {
  it('is empty without CHROME_FLAGS', () => expect(extraFlags({})).toEqual([]));
  it('splits on spaces and keeps only flags', () =>
    expect(extraFlags({ CHROME_FLAGS: ' --use-gl=angle  --use-angle=swiftshader junk ' })).toEqual(['--use-gl=angle', '--use-angle=swiftshader']));
});
