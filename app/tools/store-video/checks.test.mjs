// (T709) The store video's pure checks: node --test app/tools/store-video/checks.test.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import { SCENES, CAPTION_BAND, clipOffset, settleAfter, planTimeline, captionSvg, specProblems } from './checks.mjs';

test('every scene carries its numbers (a lost field reads as NaN and breaks the cut)', () => {
  for (const s of SCENES) for (const k of ['capY', 'lead', 'hold', 'minHold']) assert.ok(Number.isFinite(s[k]), `${s.id}.${k}`);
});

test('the touch is the first frame after a still gap near the host tap', () => {
  assert.equal(clipOffset([0, 1.56, 1.58, 1.6], 1.6).toFixed(2), '0.04');
  // a frame that keeps coming (no still gap) is not taken as the touch: the scene is not cut by guesswork
  assert.throws(() => clipOffset([0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4, 1.6], 1.6), /no still-then-touch frame/);
});

test('the end state is where the screen comes to rest after the last touch', () => {
  assert.equal(settleAfter([0, 1.6, 1.7, 1.8, 2.9], 1.65), 1.8);
  assert.equal(settleAfter([0, 1.6, 1.7], 1.65), 1.7);   // the clip's last frame
});

const scene = (id, over = {}) => ({ ...SCENES.find((s) => s.id === id), tapV: 1.6, doneV: 2.5, avail: 5, ...over });
test('holds shrink together to fit 30 s, and a scene slower than its slot fails', () => {
  const plan = planTimeline(SCENES.map((s) => scene(s.id, { doneV: 4 })));
  assert.ok(plan.total <= 29.5 && plan.total >= 25, String(plan.total));
  for (const p of plan.scenes) assert.ok(p.hold >= SCENES.find((s) => s.id === p.id).minHold - 1e-9);
  assert.throws(() => planTimeline(SCENES.map((s) => scene(s.id, { doneV: 9 }))), /nothing is sped up/);
  assert.ok(planTimeline(SCENES.map((s) => scene(s.id, { doneV: 1.8, avail: 8 }))).total >= 25);   // short: holds grow within the recording
  assert.throws(() => planTimeline(SCENES.map((s) => scene(s.id, { doneV: 1.8, avail: 2.2 }))), /under the 25 s/);
});

test('every caption sits inside the band the stores leave clear', () => {
  for (const s of SCENES) {
    const { box } = captionSvg(s.caption, s.capY);
    assert.ok(box.y >= 1920 * CAPTION_BAND[0] && box.y + box.h <= 1920 * CAPTION_BAND[1], s.id);
    assert.ok(box.x >= 0 && box.x + box.w <= 864, s.id);
  }
  assert.throws(() => captionSvg('Too low', 0.95), /safe band/);
});

test('the spec check names each broken spec', () => {
  const ok = { streams: [{ codec_type: 'video', codec_name: 'h264', profile: 'High', level: 40, width: 864, height: 1920, pix_fmt: 'yuv420p', r_frame_rate: '30/1', avg_frame_rate: '30/1' },
    { codec_type: 'audio', codec_name: 'aac', channels: 2, sample_rate: '48000' }], format: { duration: '29.0' } };
  assert.deepEqual(specProblems(ok, { bytes: 4e7, poster: { w: 864, h: 1920 } }), []);
  const bad = structuredClone(ok); bad.streams[0].level = 41; bad.format.duration = '31'; bad.streams.pop();
  const p = specProblems(bad, { bytes: 6e8, poster: null }).join(' | ');
  for (const re of [/level 41/, /duration 31/, /audio missing/, /file size/, /poster PNG missing/]) assert.match(p, re);
});
