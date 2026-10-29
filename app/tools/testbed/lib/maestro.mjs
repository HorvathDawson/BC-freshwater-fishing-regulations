// (T255) Maestro (https://maestro.mobile.dev, a CLI: YAML flows over the accessibility tree; no app code, no test
// build). One flow per UI step; the runner owns screenshots and the report. RN's `testID` is Maestro's `id:`.
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { serial } from './device.mjs';

const HERE = path.dirname(new URL(import.meta.url).pathname);
export const FLOWS = path.join(HERE, '..', 'flows');
const BIN = process.env.MAESTRO_BIN ?? path.join(os.homedir(), '.maestro/maestro/bin/maestro');

/**
 * Run `flows/<name>.yaml` with `env` (each `${KEY}` in the flow). Resolves { ok, ms, log } — never throws: a failed
 * flow is a failed step, said in the report with Maestro's own words.
 */
export function flow(name, env = {}, { logDir } = {}) {
  const file = path.join(FLOWS, `${name}.yaml`);
  if (!fs.existsSync(file)) throw new Error(`no flow ${file}`);
  const args = ['--device', serial(), 'test', file, ...Object.entries(env).flatMap(([k, v]) => ['-e', `${k}=${v}`])];
  if (logDir) args.push('--test-output-dir', logDir);
  const t0 = Date.now();
  const r = spawnSync(BIN, args, {
    encoding: 'utf8', timeout: 6 * 60_000,
    env: { ...process.env, MAESTRO_CLI_NO_ANALYTICS: '1', MAESTRO_CLI_ANALYSIS_NOTIFICATION_DISABLED: 'true',
           JAVA_HOME: process.env.JAVA_HOME ?? '/Applications/Android Studio.app/Contents/jbr/Contents/Home' },
  });
  const log = `${r.stdout ?? ''}${r.stderr ?? ''}`;
  const lines = log.split('\n').filter((l) => /FAILED|COMPLETED|Assertion|not found|Element/.test(l)).slice(-12).join('\n');
  return { ok: r.status === 0, ms: Date.now() - t0, log, said: lines.trim() };
}
