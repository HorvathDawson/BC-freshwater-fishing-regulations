// (T760) node --test app/tools/testbed/lib/kept-log.test.mjs
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { keptLog } from './kept-log.mjs';

const js = (t, s = '20.797') => `11-15 22:02:${s}  6831  6853 I ReactNativeJS: ${t}`;
const dm = (t) => `11-15 22:03:01.100  2101  2150 I DownloadManager: ${t}`;
const other = '11-15 22:02:21.000  1372  1400 I AiAiTextClassifier: GenerateLinks did not find valid links';

test('lines kept before a flow cleared the log are read back with the log as it is now, in order, once', () => {
  const log = keptLog();
  log.keep(['--------- beginning of main', js('[download] mobile data: go'), other, dm('[1] Starting'), ''].join('\r\n'));
  // the flow cleared the device's log; what came after it:
  const now = [js('[setup] installed 812 ms after the last byte', '59.001'), dm('[1] Finished')].join('\n');
  assert.deepEqual(log.lines('ReactNativeJS', now), [js('[download] mobile data: go'), js('[setup] installed 812 ms after the last byte', '59.001')]);
  assert.deepEqual(log.lines('DownloadManager', now), [dm('[1] Starting'), dm('[1] Finished')]);
});

test('a flow that failed before clearing leaves the same lines in the log: read once, not twice', () => {
  const log = keptLog();
  log.keep(js('[boot] start failed: x'));
  assert.deepEqual(log.lines('ReactNativeJS', js('[boot] start failed: x')), [js('[boot] start failed: x')]);
});

test('only the kept tags are held, and a scenario\'s own clear drops them', () => {
  const log = keptLog();
  log.keep([other, js('[heap] 1')].join('\n'));
  assert.deepEqual(log.lines('AiAiTextClassifier'), []);
  log.clear();
  assert.deepEqual(log.lines('ReactNativeJS'), []);
});
