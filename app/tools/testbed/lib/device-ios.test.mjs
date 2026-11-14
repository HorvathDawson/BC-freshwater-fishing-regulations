// (T719) the iOS log reading and the literal swipe flow: pure parts, no simulator
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { useAppLog, logcatClear, logcatJs, logTime, clockSkew, touchLog } from './device-ios.mjs';
import { swipeYaml } from './ios-flow.mjs';

const z = execFileSync('date', ['+%z'], { encoding: 'utf8' }).trim();
const off = (z[0] === '-' ? -1 : 1) * (Number(z.slice(1, 3)) * 60 + Number(z.slice(3))) * 60_000;
const stamp = (t) => new Date(t + off).toISOString().replace('T', ' ').slice(0, 23);

test('logTime reads a compact line on the Mac zone (not Node\'s), and clockSkew agrees with it', () => {
  const t = 1793868519188;
  assert.equal(logTime(`${stamp(t)} I  CanIFish[1:2] x`), t);
  assert.equal(logTime('no time here'), null);
  const f = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'dios-')), 'app-os.log');
  fs.writeFileSync(f, 'old [com.facebook.react.log:javascript] [stall] {"at":1,"ms":99}\n');
  useAppLog(f); logcatClear();
  fs.appendFileSync(f, `${stamp(t + 3)} I  CanIFish[1:2] [com.facebook.react.log:javascript] [heap] {"at":${t},"size":1}\n`
    + `${stamp(t + 10)} Df CanIFish[1:2] [com.apple.UIKit:EventDispatch] Evaluating dispatch of UIEvent: 0x1; type: 0; subtype: 0\n`
    + `${stamp(t + 20)} A  CanIFish[1:2] (UIKitCore) send gesture actions\n`);
  assert.equal(clockSkew(), -3);
  assert.deepEqual(touchLog(), { touches: [t + 10], actions: [t + 20] });
  assert.ok(!logcatJs().includes('"ms":99'), 'the mark hides what came before');
  assert.ok(logcatJs().includes('[heap] '));
});

test('swipeYaml writes literal points and refuses a bad one', () => {
  assert.match(swipeYaml([1.4, 200], [300, 200.6], 400), /start: "1,200", end: "300,201", duration: 400/);
  assert.throws(() => swipeYaml([1], [2, 2], 100));
  assert.throws(() => swipeYaml([1, 1], [2, 2], 0));
});
