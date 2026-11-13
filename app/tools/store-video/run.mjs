#!/usr/bin/env node
// STORE PREVIEW VIDEO, ONE COMMAND (T709 step 1, Android; owner 2026-11-13: "we want to generate a nice App preview"):
//   node app/tools/store-video/run.mjs [--out DIR] [--at 2026-11-13T12:00:00-07:00]
// Records the real app on the test-bed emulator (the kept test-bed APK, reading the STAGING release) through the test bed
// (scripts/heavy.sh → app/tools/testbed/session.sh → drive.mjs: real taps at real speed, `adb screenrecord`, one clip per
// scene, each scene's screen checked before and after), then cuts it with ffmpeg into ONE portrait MP4 for the stores:
// 864x1920 (the phone's 9:20 frame, the largest H.264 High 4.0 holds), a constant 30 fps, H.264 High 4.0 at ~11 Mbit/s, a
// silent stereo AAC track, 25-30 s, captions burned in, plus the poster PNG (the coloured map, before the first tap).
// Nothing is sped up: a scene that is slower than its slot fails the run. Nothing is uploaded.
// CAPTIONS: this ffmpeg has no drawtext/subtitles filter (no libfreetype/libass), so each caption is drawn to a transparent
// PNG by rsvg-convert (SVG text) and laid over its scene with ffmpeg's overlay, faded in.
// PINNED: the phone's clock (--at, default now; the day the rules are read for), the status bar (demo mode at --at's B.C.
// time), the water (the Vedder), its day in June, the saved spot and its note, the GPS. NOT pinned: the live feeds (gauge
// readings, the DFO salmon feed): checked on screen instead. A failed check = no video, exit 1, DIR/report.json says why.
// Output: DIR (default app/dist/store-video, outside git): preview.mp4, poster.png, report.json; work/ holds the raw clips,
// the device's own screenshots and one frame per scene (work/frames) for a look before anyone uploads it. A copy of the MP4,
// the poster and the report goes to ~/bcfish-session-backup-2026-11-12/store-video/.
import { spawnSync, execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { readStamp, stampLine } from '../testbed/lib/build-stamp.mjs';
import { checkChannel } from '../testbed/lib/channel.mjs';
import { SCENES, END_CARD, SPEC, clipOffset, settleAfter, planTimeline, captionSvg, endCardSvg, specProblems } from './checks.mjs';

const HERE = path.dirname(new URL(import.meta.url).pathname), APP = path.resolve(HERE, '../..'), ROOT = path.resolve(APP, '..');
const args = process.argv.slice(2), val = (k, dflt) => (args.includes(k) ? args[args.indexOf(k) + 1] : dflt);
for (const a of args) if (a.startsWith('--') && !['--out', '--at'].includes(a)) throw new Error(`store-video/run: unknown flag ${a} (flags: --out, --at)`);
const OUT = path.resolve(val('--out', path.join(APP, 'dist/store-video')));
const AT = val('--at', new Date().toISOString());
if (Number.isNaN(Date.parse(AT))) throw new Error(`store-video/run: --at ${AT} is not a date`);
const WORK = path.join(OUT, 'work');
const BACKUP = path.join(os.homedir(), 'bcfish-session-backup-2026-11-12/store-video');
const APK = path.join(APP, 'tools/testbed/out/app-testbed.apk');

const report = { ok: false, at: AT, out: OUT, started: new Date().toISOString() };
const writeReport = () => { fs.mkdirSync(OUT, { recursive: true }); fs.writeFileSync(path.join(OUT, 'report.json'), JSON.stringify(report, null, 2) + '\n'); };
const fail = (why) => { report.ok = false; report.why = why; writeReport(); console.error(`store-video/run: FAILED, no video delivered: ${why}\n  (details: ${path.join(OUT, 'report.json')})`); process.exit(1); };
const run = (cmd, argv, what) => { const r = spawnSync(cmd, argv, { encoding: 'utf8', maxBuffer: 256 << 20 }); if (r.status !== 0) fail(`${what}: ${cmd} exited ${r.status}: ${(r.stderr || r.stdout || String(r.error)).trim().split('\n').slice(-4).join(' / ')}`); return r.stdout; };
const git = (...a) => execFileSync('git', a, { cwd: ROOT, encoding: 'utf8' }).trim();

// ---------- 0. the tools ----------
for (const [bin, flag] of [['ffmpeg', '-version'], ['ffprobe', '-version'], ['rsvg-convert', '--version']]) run(bin, [flag], `${bin} is needed`);
const filters = run('ffmpeg', ['-hide_banner', '-filters'], 'ffmpeg filters');
for (const f of ['overlay', 'fps', 'tpad', 'fade', 'concat', 'anullsrc']) if (!new RegExp(`\\s${f}\\s`).test(filters)) fail(`this ffmpeg has no ${f} filter`);
report.captionBurnIn = /\sdrawtext\s/.test(filters) ? 'rsvg-convert PNG + overlay (drawtext also present)' : 'rsvg-convert PNG + overlay: this ffmpeg build has no drawtext (no libfreetype)';

// ---------- 1. the data and the build ----------
const latest = fs.readFileSync(path.join(ROOT, 'data/release/LATEST'), 'utf8').trim();
let channel;
try { channel = await checkChannel(); } catch (e) { fail(`the staging test channel: ${e.message}`); }
if (channel.release.data_release !== latest) fail(`data/release/LATEST is ${latest} but staging serves ${channel.release.data_release}: the video would show another release than the one named`);
report.release = { latest, staging: channel.release.data_release };

const stamp = readStamp(APK);
if (!stamp) fail(`${path.relative(ROOT, APK)} has no build stamp: build it with scripts/heavy.sh testbed 30 -- bash app/tools/testbed/session.sh --build`);
if (stamp.dirty) fail(`the test-bed APK was built from a dirty tree (${stamp.dirty_files.slice(0, 3).join(', ')}): rebuild it`);
const head = git('rev-parse', 'HEAD');
let allowStale = null;
if (stamp.commit !== head) {
  // the APK may be older than HEAD only when nothing that ships changed since: the app tools (not in the app), and in
  // app.json only the store's version numbers (versionCode / buildNumber, which the Play build bumps after the code)
  const changed = git('diff', '--name-only', stamp.commit, head, '--', 'app', 'package.json', 'pnpm-lock.yaml').split('\n').filter(Boolean);
  const shipping = changed.filter((f) => !f.startsWith('app/tools/') && !f.startsWith('app/dist/'));
  const bad = shipping.filter((f) => f !== 'app/apps/mobile/app.json'
    || git('diff', '-U0', stamp.commit, head, '--', f).split('\n').filter((l) => /^[+-]\s/.test(l)).some((l) => !/"(versionCode|buildNumber)"/.test(l)));
  if (bad.length) fail(`the test-bed APK (${stamp.commit.slice(0, 9)}) is older than app code at HEAD (${bad.slice(0, 4).join(', ')}): rebuild it with scripts/heavy.sh testbed 30 -- bash app/tools/testbed/session.sh --build`);
  allowStale = `T709 store video: app code at ${head.slice(0, 9)} is the APK's ${stamp.commit.slice(0, 9)}${shipping.length ? ' (app.json store version numbers only)' : ''}`;
}
report.build = { apk: path.relative(ROOT, APK), stamp: stampLine(stamp), head: head.slice(0, 9), ...(allowStale ? { olderThanHead: allowStale } : {}) };

// ---------- 2. the recording (the emulator is heavy: through heavy.sh, one bounded batch) ----------
fs.rmSync(OUT, { recursive: true, force: true }); fs.mkdirSync(WORK, { recursive: true });
console.log(`store-video/run: release ${latest}, clock pinned to ${AT}, ${stampLine(stamp)} -> ${OUT}`);
const session = spawnSync(path.join(ROOT, 'scripts/heavy.sh'), ['store-video', '50', '--', 'bash', path.join(APP, 'tools/testbed/session.sh'), '--max-min', '25',
  ...(allowStale ? ['--allow-stale', allowStale] : []), '--', process.execPath, path.join(HERE, 'drive.mjs'), '--work', WORK, '--at', AT, '--release', latest],
{ stdio: 'inherit', cwd: ROOT });
let capture = null;
try { capture = JSON.parse(fs.readFileSync(path.join(WORK, 'capture.json'), 'utf8')); } catch (e) { fail(`the recording session wrote no capture.json (heavy.sh exited ${session.status}): ${e.message}`); }
report.capture = { target: capture.target, closedRedPixels: capture.closedRedPixels, findings: capture.findings, water: capture.water, spot: capture.spot, pick: capture.pick };
if (session.status !== 0 || !capture.ok) fail(`the recording failed: ${capture.error ?? `heavy.sh exited ${session.status}`} (the device's screenshots: ${path.join(WORK, 'device')})`);

// ---------- 3. the cut ----------
const framesOf = (clip) => run('ffprobe', ['-v', 'error', '-select_streams', 'v:0', '-show_entries', 'frame=best_effort_timestamp_time', '-of', 'csv=p=0', clip], `reading ${clip}'s frames`)
  .split('\n').filter((l) => l.trim() !== '').map(Number).filter((x) => Number.isFinite(x));
const placed = SCENES.map((s) => {
  const c = capture.scenes[s.id];
  if (!c) fail(`no clip for scene ${s.id}`);
  const pts = framesOf(path.join(WORK, c.clip));
  let off, doneV;
  try { off = clipOffset(pts, c.marks.tap); doneV = settleAfter(pts, c.marks.last - off); } catch (e) { fail(`${s.id}: ${e.message}`); }
  const tapV = c.marks.tap - off, stopV = c.marks.stop - off;
  return { ...s, clip: path.join(WORK, c.clip), offset: off, tapV, doneV, avail: stopV - doneV - 0.4, marks: c.marks };
});
let plan;
try { plan = planTimeline(placed); } catch (e) { fail(e.message); }
report.timeline = plan.scenes.map((p, i) => ({ ...p, caption: SCENES[i].caption, captionBacks: SCENES[i].backs, clipOffset: +placed[i].offset.toFixed(3), touchAt: +(placed[i].tapV - p.start).toFixed(2) }));
report.endCard = END_CARD;

// captions and the end card, drawn as PNGs
const capDir = path.join(WORK, 'captions'); fs.mkdirSync(capDir, { recursive: true });
const png = (svg, name) => { const s = path.join(capDir, `${name}.svg`), p = path.join(capDir, `${name}.png`); fs.writeFileSync(s, svg); run('rsvg-convert', ['-o', p, s], `drawing ${name}`); return p; };
const caps = SCENES.map((s) => png(captionSvg(s.caption, s.capY).svg, s.id));
const icon = `data:image/png;base64,${fs.readFileSync(path.join(APP, 'apps/mobile/assets/icon/icon-1024.png')).toString('base64')}`;
const endPng = png(endCardSvg(icon), 'end-card');

const MP4 = path.join(OUT, 'preview.mp4'), POSTER = path.join(OUT, 'poster.png');
const inputs = [], chains = [];
plan.scenes.forEach((p, i) => inputs.push('-i', placed[i].clip));
plan.scenes.forEach((p, i) => inputs.push('-loop', '1', '-framerate', String(SPEC.fps), '-t', (p.end - p.start).toFixed(3), '-i', caps[i]));
inputs.push('-loop', '1', '-framerate', String(SPEC.fps), '-t', String(END_CARD.seconds), '-i', endPng);
inputs.push('-f', 'lavfi', '-t', plan.total.toFixed(3), '-i', 'anullsrc=r=48000:cl=stereo');
const n = plan.scenes.length;
plan.scenes.forEach((p, i) => {
  // a caption comes in with the scene's first touch (scene 1: once the answer is on screen, choice 7: the question is
  // answered before any caption about features)
  const capAt = SCENES[i].captionAfterDone ? p.doneAt : report.timeline[i].touchAt;
  chains.push(`[${i}:v]setpts=PTS-STARTPTS,fps=${SPEC.fps},tpad=stop_mode=clone:stop_duration=30,trim=start=${p.start.toFixed(3)}:end=${p.end.toFixed(3)},setpts=PTS-STARTPTS,scale=${SPEC.w}:${SPEC.h}:flags=lanczos,setsar=1,format=rgba[b${i}]`);
  chains.push(`[${n + i}:v]format=rgba,fade=t=in:st=${capAt.toFixed(3)}:d=0.25:alpha=1[c${i}]`);
  chains.push(`[b${i}][c${i}]overlay=0:0:shortest=1,format=rgba[v${i}]`);
});
chains.push(`[${2 * n}:v]scale=${SPEC.w}:${SPEC.h},setsar=1,format=rgba,fps=${SPEC.fps}[v${n}]`);
chains.push(`${Array.from({ length: n + 1 }, (_, i) => `[v${i}]`).join('')}concat=n=${n + 1}:v=1:a=0,format=yuv420p[v]`);
run('ffmpeg', ['-hide_banner', '-loglevel', 'error', '-y', ...inputs, '-filter_complex', chains.join(';'), '-map', '[v]', '-map', `${2 * n + 1}:a`,
  '-c:v', 'libx264', '-profile:v', 'high', '-level:v', '4.0', '-preset', 'slow', '-b:v', '11M', '-maxrate', '12M', '-bufsize', '24M', '-pix_fmt', 'yuv420p',
  '-r', String(SPEC.fps), '-g', String(SPEC.fps * 2), '-c:a', 'aac', '-b:a', '256k', '-ar', '48000', '-ac', '2', '-t', plan.total.toFixed(3), '-movflags', '+faststart', MP4], 'encoding the video');

// the poster: the coloured map, before the first tap (half a second into scene 1, which has no caption yet)
const posterAt = Math.max(0.1, Math.min(0.5, report.timeline[0].touchAt - 0.3));
run('ffmpeg', ['-hide_banner', '-loglevel', 'error', '-y', '-ss', posterAt.toFixed(3), '-i', MP4, '-frames:v', '1', POSTER], 'the poster frame');
report.poster = { file: 'poster.png', at: +posterAt.toFixed(2), what: 'scene 1, the coloured map before the tap' };

// one frame per scene for a look (scene 1 twice: the map, then the answer with its caption)
const frames = path.join(WORK, 'frames'); fs.mkdirSync(frames, { recursive: true });
let t = 0;
const looks = [];
plan.scenes.forEach((p, i) => {
  const d = p.end - p.start;
  if (i === 0) looks.push([`01a-${p.id}-map`, t + posterAt]);
  looks.push([`${String(i + 1).padStart(2, '0')}${i === 0 ? 'b' : ''}-${p.id}`, t + d - 0.25]);
  t += d;
});
looks.push([`${String(n + 1).padStart(2, '0')}-end-card`, t + END_CARD.seconds - 0.3]);
for (const [name, at] of looks) run('ffmpeg', ['-hide_banner', '-loglevel', 'error', '-y', '-ss', at.toFixed(3), '-i', MP4, '-frames:v', '1', path.join(frames, `${name}.png`)], `frame ${name}`);
report.frames = looks.map(([name, at]) => ({ file: `work/frames/${name}.png`, at: +at.toFixed(2) }));

// ---------- 4. the spec check ----------
const probe = JSON.parse(run('ffprobe', ['-v', 'error', '-show_streams', '-show_format', '-of', 'json', MP4], 'probing the video'));
const pngSize = (f) => { if (!fs.existsSync(f)) return null; const b = fs.readFileSync(f); return { w: b.readUInt32BE(16), h: b.readUInt32BE(20) }; };
const bytes = fs.statSync(MP4).size;
const problems = specProblems(probe, { bytes, poster: pngSize(POSTER) });
const v = probe.streams.find((s) => s.codec_type === 'video'), a = probe.streams.find((s) => s.codec_type === 'audio');
report.file = { mp4: 'preview.mp4', bytes, seconds: Number(probe.format.duration), video: `${v?.codec_name} ${v?.profile} level ${v?.level} ${v?.width}x${v?.height} ${v?.r_frame_rate} fps ${v?.pix_fmt} ${Math.round(Number(v?.bit_rate) / 1000)} kbit/s`,
  audio: a ? `${a.codec_name} ${a.channels} ch ${a.sample_rate} Hz (silent)` : null };
if (problems.length) fail(`the video breaks the spec: ${problems.join('; ')}`);

report.ok = true; report.finished = new Date().toISOString();
writeReport();
fs.mkdirSync(BACKUP, { recursive: true });
for (const f of ['preview.mp4', 'poster.png', 'report.json']) fs.copyFileSync(path.join(OUT, f), path.join(BACKUP, f));
console.log(`store-video/run: ${report.file.seconds.toFixed(2)} s, ${(bytes / 1e6).toFixed(1)} MB, ${report.file.video}; ${path.join(OUT, 'preview.mp4')} (+ poster.png, report.json; copied to ${BACKUP})`);
