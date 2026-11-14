// (T721-T723) ONE-OFF MAESTRO FLOWS ON iOS, for what a shared flow cannot take as a parameter. Maestro checks a swipe's
// points before it fills `${…}` (flows/edge-back.yaml: "one literal flow per drag"), so a timed swipe at given points is
// written as a literal flow into the run's own folder and run there. Same Maestro call as lib/maestro.mjs (the sandbox
// that keeps it off the emulator ports, `--platform ios`); resolves { ok, ms, said } and never throws, like `flow`.
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { maestroCmd } from './maestro.mjs';
import { udid } from './device-ios.mjs';

const BIN = process.env.MAESTRO_BIN ?? path.join(os.homedir(), '.maestro/maestro/bin/maestro');
const pt = (p) => {
  if (!Array.isArray(p) || p.length !== 2 || !p.every(Number.isFinite)) throw new Error(`a point is [x, y] in iOS points (got ${JSON.stringify(p)})`);
  return `${Math.round(p[0])},${Math.round(p[1])}`;
};

/** The literal YAML of one swipe from `from` to `to` (iOS points) taking `ms`. */
export function swipeYaml(from, to, ms) {
  if (!(ms > 0)) throw new Error(`a swipe takes a time in ms (got ${ms})`);
  return `appId: ca.canifishthis.app\n---\n- swipe: { start: "${pt(from)}", end: "${pt(to)}", duration: ${Math.round(ms)} }\n`;
}

/** Run a literal flow (YAML text) written under `dir/flows-ios/<name>.yaml`; Maestro's log beside it. */
export function runYaml(dir, name, yaml) {
  const d = path.join(dir, 'flows-ios');
  fs.mkdirSync(d, { recursive: true });
  const file = path.join(d, `${name}.yaml`);
  fs.writeFileSync(file, yaml);
  const t0 = Date.now();
  const [cmd, argv] = maestroCmd(BIN, ['--device', udid(), 'test', file, '--test-output-dir', path.join(d, `${name}-out`)]);
  const r = spawnSync(cmd, argv, {
    encoding: 'utf8', timeout: 3 * 60_000,
    env: { ...process.env, MAESTRO_CLI_NO_ANALYTICS: '1', MAESTRO_CLI_ANALYSIS_NOTIFICATION_DISABLED: 'true',
           JAVA_HOME: process.env.JAVA_HOME ?? '/Applications/Android Studio.app/Contents/jbr/Contents/Home' },
  });
  const log = `${r.stdout ?? ''}${r.stderr ?? ''}`;
  fs.writeFileSync(path.join(d, `${name}.log`), log);
  return { ok: r.status === 0, ms: Date.now() - t0, said: log.split('\n').filter((l) => /FAILED|COMPLETED|not found/.test(l)).slice(-6).join('\n') };
}
