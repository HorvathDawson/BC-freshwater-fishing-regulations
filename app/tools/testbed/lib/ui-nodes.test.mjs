// (T760) node --test app/tools/testbed/lib/ui-nodes.test.mjs
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { downloadJobs, findNode, parseNodes } from './ui-nodes.mjs';

const xml = `<?xml version='1.0' encoding='UTF-8' standalone='yes' ?><hierarchy rotation="0">
<node index="0" text="" resource-id="a" class="x" content-desc="" bounds="[0,0][1080,2400]" />
<node index="1" text="Downloading BC fishing rules &amp; maps" resource-id="b" content-desc="" bounds="[100,300][980,420]" />
<node index="2" text="" content-desc="2 files downloading" bounds="[0,500][1080,700]"><node text="" content-desc="" bounds="[0,0][0,0]" /></node>
</hierarchy>`;

test('nodes with a text or a description come back with the middle of their bounds', () => {
  const n = parseNodes(xml);
  assert.deepEqual(n.map((x) => x.text), ['Downloading BC fishing rules & maps', '2 files downloading']);
  assert.deepEqual([n[0].cx, n[0].cy, n[1].cx, n[1].cy], [540, 360, 540, 600]);
  assert.equal(findNode(n, /^Downloading BC/).y1, 300);
  assert.equal(findNode(n, /nothing/), null);
});

const dm = (t, s) => `11-16 11:22:${s}.512  1673  7298 D DownloadManager: ${t}`;
const log = [dm('[500] Starting', '00'), dm('[500] Finished with status SUCCESS', '01'), dm('[587] Starting', '05'), dm('[588] Starting', '05'), dm('[588] Finished with status SUCCESS', '07')];

test('jobs are read from the stamp on, and the ones still running are the started minus the finished', () => {
  const j = downloadJobs(log, '11-16 11:22:04');
  assert.deepEqual(j.started, ['587', '588']);
  assert.deepEqual(j.running, ['587']);
  assert.equal(j.startedAt, '11:22:05');
  assert.equal(j.finishedAt, '11:22:07');
  assert.deepEqual(downloadJobs(log, '').started, ['500', '587', '588']);
  assert.deepEqual(downloadJobs(log, '11-16 11:23:00').started, []);
});
