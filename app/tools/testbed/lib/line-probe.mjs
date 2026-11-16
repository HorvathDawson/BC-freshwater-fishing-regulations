// (T760) A LINE PROBE for a stalled bench step: is the network line dead on the emulator only, on the Mac too, or up on
// both (then the stall is the app's or the downloader's)? Plain functions, tested without a device
// (node --test app/tools/testbed/lib/line-probe.test.mjs). A probe only counts if it checks the status code AND the byte
// count: a 404 answers fast and proves nothing.
import { execFileSync } from 'node:child_process';

export const PROBE_URL = 'https://data-staging.canifishthis.ca/release/manifest.json';

/** curl's `-w '%{http_code} %{size_download} %{time_total}'` output → { code, bytes, secs, ok } (ok = HTTP 200 and bytes > 0). */
export function parseCurl(out) {
  const m = /^\s*(\d{3})\s+(\d+)\s+([\d.]+)\s*$/.exec(String(out));
  if (!m) return { code: 0, bytes: 0, secs: 0, ok: false };
  const code = Number(m[1]), bytes = Number(m[2]);
  return { code, bytes, secs: Number(m[3]), ok: code === 200 && bytes > 0 };
}

/** `ping` summary text → packets received (0 when unreadable). */
export function pingReceived(text) {
  const m = /(\d+) (?:packets )?received/.exec(String(text));
  return m ? Number(m[1]) : 0;
}

/** The one-line verdict. mac = parseCurl result; emu = { fetched: bytes | null (no fetch tool), pinged: n }. */
export function lineVerdict(mac, emu) {
  if (!mac.ok) return `line dead on the Mac too (Mac probe: http ${mac.code}, ${mac.bytes} bytes in ${mac.secs.toFixed(1)} s)`;
  // the emulator's ping is answered by its own network stack even when the line is dead: a fetch, when it can, decides
  const emuUp = emu.fetched === null ? emu.pinged > 0 : emu.fetched > 0;
  if (!emuUp) return `line dead on the emulator only (Mac fetched ${mac.bytes} bytes in ${mac.secs.toFixed(1)} s)`;
  return 'line up on both (the stall is the app\'s or the downloader\'s)';
}

/** The Mac half: one small file, max 15 s. Never throws. */
export function macProbe(url = PROBE_URL) {
  let out = '';
  try { out = execFileSync('curl', ['-sS', '-o', '/dev/null', '-m', '15', '-w', '%{http_code} %{size_download} %{time_total}', url], { encoding: 'utf8', timeout: 20_000 }); }
  catch (e) { out = String(e.stdout ?? ''); }
  return { raw: out.trim(), ...parseCurl(out) };
}

/** The emulator half: what the image has, ping, a fetch if it can, and the connectivity lines. `d` = lib/device.mjs. Never throws. */
export function emuProbe(d, url = PROBE_URL) {
  const host = new URL(url).host;
  const run = (c) => { try { return d.sh(c); } catch (e) { return `(failed: ${e.message})`; } };
  const tools = run('which ping curl wget nc toybox 2>&1').trim();
  const ping = run(`ping -c 3 -W 2 ${host} 2>&1`);
  const conn = run('dumpsys connectivity | grep -E "VALIDATED|Active default network" | head -8');
  let fetched = null, fetchRaw = '(no curl or wget on the image)';
  if (/curl/.test(tools)) { fetchRaw = run(`curl -sS -m 15 -o /dev/null -w "%{http_code} %{size_download}" ${url} 2>&1`); const m = /^(\d{3}) (\d+)/.exec(fetchRaw.trim()); fetched = m && m[1] === '200' ? Number(m[2]) : 0; }
  else if (/wget/.test(tools)) { fetchRaw = run(`wget -q -T 15 -O - ${url} 2>&1 | wc -c`); fetched = Number(fetchRaw.trim()) || 0; }
  return { tools, ping, pinged: pingReceived(ping), conn, fetched, fetchRaw };
}

/** Both halves, the verdict, and the raw text for the log file. */
export function lineProbe(d, url = PROBE_URL) {
  const mac = macProbe(url), emu = emuProbe(d, url);
  const verdict = lineVerdict(mac, emu);
  const raw = [`=== line probe ${new Date().toISOString()}`, `verdict: ${verdict}`, `--- Mac: curl ${url}`, `raw: ${mac.raw} -> http ${mac.code}, ${mac.bytes} bytes, ${mac.secs} s`,
    '--- emulator: which', emu.tools, '--- emulator: ping', emu.ping, `--- emulator: fetch (${emu.fetched === null ? 'none' : emu.fetched + ' bytes'})`, emu.fetchRaw, '--- emulator: connectivity', emu.conn].join('\n');
  return { verdict, raw };
}
