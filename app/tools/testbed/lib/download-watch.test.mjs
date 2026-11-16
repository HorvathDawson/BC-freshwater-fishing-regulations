// (T756) node --test app/tools/testbed/lib/download-watch.test.mjs
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { stallWatch, thrownAway } from './download-watch.mjs';

const U = 'https://data-staging.canifishthis.ca/release/basemap/basemap-6960ed933a82/basemap-6960ed933a82.pmtiles';
const js = (t) => `11-15 14:48:19.490 26400 26422 I ReactNativeJS: ${t}`;

test('the app\'s own "queued again from zero" line with bytes down is a download thrown away', () => {
  const lines = [
    js(`[download] mobile data: go (479277504 bytes)`),
    js(`[download] ${U}: queued again from zero (was running, 17413559 of 225356297 bytes): it was queued for Wi-Fi only and mobile data is now allowed`),
    js(`[download] ${U}: paused (reason 1), 17413559 of 225356297 bytes`),
  ];
  assert.deepEqual(thrownAway(lines), [{ url: U, was: 'running', bytes: 17413559, total: 225356297, why: 'it was queued for Wi-Fi only and mobile data is now allowed' }]);
});

test('queued again with nothing down, a failed-job retry line and the system downloader\'s own cancel are not', () => {
  const lines = [
    js(`[download] ${U}: queued again from zero (was pending, 0 of 225356297 bytes): it was queued for Wi-Fi only and mobile data is now allowed`),
    js(`[download] ${U}: the download failed (reason 1008); queued again (1)`),
    js(`[download] ${U}: queued again from zero (was failed, 17413559 of 225356297 bytes): it failed (reason 1008)`),
    // the downloader's own line after a retry (its uid, not ours): never read as the app stopping a job
    '11-15 14:18:25.779  1926  1926 D DownloadManager: onStopJob id=454, reason=cancel() called by app, callingUid=10094 uid=10094 jobId=454',
  ];
  assert.deepEqual(thrownAway(lines), []);
});

test('a stall is no change for the whole limit; any change starts the count again', () => {
  const w = stallWatch(1000);
  assert.equal(w.stalled(0), false);          // nothing seen yet: not a stall
  assert.equal(w.see('10 MB|100', 0), 0);
  assert.equal(w.see('10 MB|100', 999), 999);
  assert.equal(w.stalled(999), false);
  assert.equal(w.see('10 MB|101', 1500), 0);  // one byte moved
  assert.equal(w.stalled(2499), false);
  assert.equal(w.see('10 MB|101', 2500), 1000);
  assert.equal(w.stalled(2500), true);
});
