// (T242) THE LOCAL CRASH WORKER, STARTED BY THE SCENARIO THAT NEEDS IT. A stub APK (`session.sh --crash-stub`) sends every
// crash report to the phone's localhost:<port>, `adb reverse`d to this machine; session.sh does NOT start a server there.
// With nothing listening, adb still accepts the phone's connection and closes it, so the app sees "unexpected end of
// stream" (OkHttp), never a refusal: the `crash` scenario failed that way on a stub APK (2026-11-16) because only
// `crash-bed` and `retire-swap` started the stub. Every scenario that makes the app send a report starts it here, and the
// start waits until the stub answers (or fails naming why), so a missing server is never read as the app's fault.
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

const HERE = path.dirname(new URL(import.meta.url).pathname);
const STUB_SERVER = path.join(HERE, '../crash-stub/server.mjs');

/** every POST the stub logged ({at, status, answer, body}), oldest first */
export const stubPosts = (log) => { try { return fs.readFileSync(log, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l)); } catch { return []; } };

/**
 * Start `crash-stub/server.mjs` on `port`, logging each POST to `<dir>/<name>.jsonl` and its output to `<dir>/<name>.out`;
 * resolves once `GET /__stub/kinds` answers. Throws (and stops the child) when it exits or does not answer in `ms`.
 * Returns { log, posts(), kinds(), stop() }.
 */
export async function startCrashStub({ dir, port, name = 'crash-stub', ms = 15_000 }) {
  const log = path.join(dir, `${name}.jsonl`), out = path.join(dir, `${name}.out`);
  fs.rmSync(log, { force: true });
  const child = spawn(process.execPath, [STUB_SERVER, '--port', String(port), '--log', log], { stdio: ['ignore', 'pipe', 'pipe'] });
  child.stdout.on('data', (b) => fs.appendFileSync(out, b));
  child.stderr.on('data', (b) => fs.appendFileSync(out, b));
  let exited = null;
  child.on('exit', (code, sig) => { exited = `exited (${sig ?? code})`; });
  const kinds = async () => {
    const r = await fetch(`http://localhost:${port}/__stub/kinds`, { signal: AbortSignal.timeout(5000) });
    if (!r.ok) throw new Error(`stub answered HTTP ${r.status}`);
    return r.json();
  };
  const stop = () => { if (exited === null) child.kill(); };
  const end = Date.now() + ms;
  let last = 'no answer';
  for (;;) {
    if (exited) break;
    try { await kinds(); return { log, posts: () => stubPosts(log), kinds, stop }; }
    catch (e) { last = String(e?.cause?.code ?? e?.message ?? e); }
    if (Date.now() > end) break;
    await new Promise((r) => setTimeout(r, 200));
  }
  stop();
  const said = (() => { try { return fs.readFileSync(out, 'utf8').trim().split('\n').slice(-3).join(' | '); } catch { return ''; } })();
  throw new Error(`the crash stub on :${port} ${exited ?? `did not answer in ${ms / 1000} s (${last})`}${said ? `: ${said}` : ''}`);
}
