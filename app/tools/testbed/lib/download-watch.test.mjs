// (T756) node --test app/tools/testbed/lib/download-watch.test.mjs
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { cancelledByApp, diskBytes, stallWatch } from './download-watch.mjs';

// the downloader's own lines from the run that showed the defect (emulator, 2026-11-15; the app reopened on Wi-Fi after
// "Wait for Wi-Fi": jobs 441 and 443 were stopped by the app, their partial files deleted, 445 and 446 began from zero)
const CAPTURED = `
         1794769084.901  1621  1621 D DownloadManager: onStopJob id=441, reason=cancel() called by app, callingUid=10094 uid=10094 jobId=441
         1794769084.902  1621  4001 V DownloadManager: Deleting /storage/emulated/0/Android/data/ca.canifishthis.app/files/bg-download/atlas-b9de120e9ee60b6b.6441dc020a6d.pmtiles-197196706 via provider delete
         1794769084.937  1621  1621 D DownloadManager: onStopJob id=443, reason=cancel() called by app, callingUid=10094 uid=10094 jobId=443
         1794769084.937  1621  6311 D DownloadManager: [445] Starting
         1794769085.024  1621  5406 D DownloadManager: [441] Finished with status WAITING_TO_RETRY
         1794769207.813  1621  1633 V DownloadManager: Deleting /storage/emulated/0/Android/data/ca.canifishthis.app/files/bg-download/bc_outside.geojson-62563 via provider delete
         1794769770.428  1621  6316 D DownloadManager: [446] Finished with status SUCCESS
`.split('\n');

test('the captured restart reads as two jobs cancelled by the app', () => {
  const c = cancelledByApp(CAPTURED);
  assert.deepEqual(c.map((x) => x.id), [441, 443]);
  assert.match(c[0].line, /^1794769084\.901 .* onStopJob id=441, reason=cancel\(\) called by app/);
});

test('a job the SYSTEM stopped, a finished job and a tidied-up file are not cancels', () => {
  const lines = [
    '11-15 11:58:04.901  1621  1621 D DownloadManager: onStopJob id=7, reason=constraints not satisfied, callingUid=1000 uid=10094 jobId=7',
    '11-15 11:58:05.024  1621  5406 D DownloadManager: [7] Finished with status WAITING_FOR_NETWORK',
    '11-15 12:00:07.813  1621  1633 V DownloadManager: Deleting /storage/emulated/0/Android/data/ca.canifishthis.app/files/bg-download/bc_outside.geojson-62563 via provider delete',
    '11-15 12:09:30.428  1621  6316 D DownloadManager: [8] Finished with status SUCCESS',
  ];
  assert.deepEqual(cancelledByApp(lines), []);
});

test('the same cancel read twice (the log is read again and again) counts once', () => {
  assert.equal(cancelledByApp([...CAPTURED, ...CAPTURED]).length, 2);
});

test('the bytes on disk are the files\' sizes, not the header or a folder', () => {
  const ls = [
    'total 412660',
    'drwxrwx--- 2 u0_a94 ext_data_rw      4096 2026-11-15 11:02 sub',
    '-rw-rw---- 1 u0_a94 ext_data_rw 197196706 2026-11-15 11:02 atlas-b9de120e9ee60b6b.6441dc020a6d.pmtiles',
    '-rw-rw---- 1 u0_a94 ext_data_rw     62563 2026-11-15 11:01 bc_outside.geojson',
  ].join('\n');
  assert.equal(diskBytes(ls), 197196706 + 62563);
  assert.equal(diskBytes('ls: /sdcard/Android/data/ca.canifishthis.app/files/bg-download/: No such file or directory'), 0);
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
