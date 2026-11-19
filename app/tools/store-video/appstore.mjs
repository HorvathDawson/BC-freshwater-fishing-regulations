#!/usr/bin/env node
// (T319 / T709) THE APP STORE CUT of a preview clip: re-encodes any portrait clip to Apple's app-preview rules for the iPhone
// 6.9"/6.5" sets and proves each rule with ffprobe on the written file. Nothing is uploaded.
//   node app/tools/store-video/appstore.mjs <in.mp4|mov> <out.mp4> [--start S] [--seconds S]
// Rules (Apple, app preview specifications, as T709 lists them): 886x1920 portrait; 15-30 s; at most 30 fps (written at a
// constant 30); H.264 High, level 4.0 at most, progressive, ~10-12 Mbit/s; a stereo AAC track at 256 kbit/s (here silent,
// replacing whatever the clip had), 48 kHz; at most 500 MB. The picture is scaled to fit 886x1920 and padded with the frame's
// own edge colour (never stretched). Apple also wants the clip to show the app on the device the set is for: that is the
// clip's source (record it on the iPhone simulator), which this tool cannot check.
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';

const args = process.argv.slice(2), val = (k, d) => (args.includes(k) ? args[args.indexOf(k) + 1] : d);
const [IN, OUT] = args.filter((a, i) => !a.startsWith('--') && !['--start', '--seconds'].includes(args[i - 1]));
if (!IN || !OUT) throw new Error('usage: appstore.mjs <in> <out> [--start S] [--seconds S]');
export const APPLE = { w: 886, h: 1920, fps: 30, minS: 15, maxS: 30, audioKbps: 256, maxBytes: 500 * 1024 * 1024 };

const probe = (f) => {
  const r = spawnSync('ffprobe', ['-v', 'error', '-show_entries', 'stream=codec_type,codec_name,profile,level,width,height,r_frame_rate,avg_frame_rate,field_order,channels,sample_rate,bit_rate:format=duration,size,bit_rate', '-of', 'json', f], { encoding: 'utf8' });
  if (r.status) throw new Error(`ffprobe ${f}: ${r.stderr}`);
  return JSON.parse(r.stdout);
};
/** every rule the file breaks ([] = it fits) */
export function appleProblems(info) {
  const v = info.streams.find((s) => s.codec_type === 'video'), a = info.streams.filter((s) => s.codec_type === 'audio');
  const dur = Number(info.format.duration), out = [];
  if (!v) return ['no video stream'];
  const fps = (r) => { const [n, d] = r.split('/').map(Number); return d ? n / d : 0; };
  if (v.width !== APPLE.w || v.height !== APPLE.h) out.push(`size ${v.width}x${v.height}, not ${APPLE.w}x${APPLE.h}`);
  if (v.codec_name !== 'h264') out.push(`codec ${v.codec_name}, not H.264`);
  if (!/^High$|^Main$|Baseline/.test(v.profile ?? '')) out.push(`H.264 profile ${v.profile}`);
  if (Number(v.level) > 40) out.push(`H.264 level ${v.level / 10}, above 4.0`);
  if (v.field_order && !['progressive', 'unknown'].includes(v.field_order)) out.push(`interlaced (${v.field_order})`);
  if (fps(v.avg_frame_rate) > APPLE.fps + 0.01 || fps(v.r_frame_rate) > APPLE.fps + 0.01) out.push(`frame rate ${v.avg_frame_rate}, above ${APPLE.fps}`);
  if (!(dur >= APPLE.minS && dur <= APPLE.maxS)) out.push(`${dur.toFixed(2)} s, not ${APPLE.minS}-${APPLE.maxS} s`);
  if (Number(info.format.size) > APPLE.maxBytes) out.push('over 500 MB');
  if (a.length !== 1) out.push(`${a.length} audio tracks, want one`);
  else {
    if (a[0].codec_name !== 'aac') out.push(`audio ${a[0].codec_name}, not AAC`);
    if (a[0].channels !== 2) out.push(`audio ${a[0].channels} channels, not stereo`);
    if (!['44100', '48000'].includes(String(a[0].sample_rate))) out.push(`audio ${a[0].sample_rate} Hz`);
    if (Math.abs(Number(a[0].bit_rate) / 1000 - APPLE.audioKbps) > 16) out.push(`audio ${Math.round(Number(a[0].bit_rate) / 1000)} kbit/s, not ${APPLE.audioKbps}`);
  }
  return out;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const src = probe(IN), dur = Number(src.format.duration);
  const start = Number(val('--start', 0)), seconds = Number(val('--seconds', Math.min(APPLE.maxS, dur - start)));
  if (!(seconds >= APPLE.minS && seconds <= APPLE.maxS)) throw new Error(`appstore: ${seconds.toFixed(2)} s from ${start} s; Apple takes ${APPLE.minS}-${APPLE.maxS} s (--start / --seconds)`);
  // the audio is a silent stereo track encoded at 256k (AAC encoders write fewer bits for silence than asked: -b:a is a ceiling,
  // so a near-inaudible noise floor at -90 dB keeps the bit rate at the asked rate; it is not heard)
  const vf = `fps=${APPLE.fps},scale=${APPLE.w}:${APPLE.h}:force_original_aspect_ratio=decrease:flags=lanczos,pad=${APPLE.w}:${APPLE.h}:(ow-iw)/2:(oh-ih)/2:color=white,setsar=1,format=yuv420p`;
  const r = spawnSync('ffmpeg', ['-y', '-hide_banner', '-loglevel', 'error', '-ss', String(start), '-t', String(seconds), '-i', IN,
    '-f', 'lavfi', '-t', String(seconds), '-i', 'anoisesrc=color=white:amplitude=0.00003:sample_rate=48000,aformat=channel_layouts=stereo',
    '-map', '0:v:0', '-map', '1:a:0', '-vf', vf, '-r', String(APPLE.fps), '-c:v', 'libx264', '-profile:v', 'high', '-level:v', '4.0',
    '-b:v', '11M', '-maxrate', '12M', '-bufsize', '24M', '-preset', 'slow', '-x264-params', 'nal-hrd=vbr',
    '-c:a', 'aac', '-b:a', `${APPLE.audioKbps}k`, '-ar', '48000', '-ac', '2', '-shortest', '-movflags', '+faststart', OUT], { encoding: 'utf8' });
  if (r.status) throw new Error(`appstore: ffmpeg: ${r.stderr}`);
  const info = probe(OUT), bad = appleProblems(info);
  const v = info.streams.find((s) => s.codec_type === 'video'), a = info.streams.find((s) => s.codec_type === 'audio');
  console.log(`appstore: ${OUT}: ${v.width}x${v.height} ${v.codec_name} ${v.profile} L${v.level / 10} ${v.avg_frame_rate} fps, ` +
    `${Number(info.format.duration).toFixed(2)} s, ${(Number(info.format.size) / 1048576).toFixed(1)} MB, video ${Math.round(Number(v.bit_rate) / 1000)} kbit/s; ` +
    `audio ${a?.codec_name} ${a?.channels} ch ${a?.sample_rate} Hz ${Math.round(Number(a?.bit_rate) / 1000)} kbit/s`);
  if (bad.length) { console.error(`appstore: BREAKS Apple's rules:\n  - ${bad.join('\n  - ')}`); fs.rmSync(OUT, { force: true }); process.exit(1); }
  console.log("appstore: fits every rule checked (886x1920, 15-30 s, <=30 fps, H.264 High <=4.0, stereo AAC 256k, <=500 MB)");
}
