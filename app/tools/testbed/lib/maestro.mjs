// (T255) Maestro (https://maestro.mobile.dev, a CLI: YAML flows over the accessibility tree; no app code, no test
// build). One flow per UI step; the runner owns screenshots and the report. RN's `testID` is Maestro's `id:`.
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { serial as androidSerial, maestroDeviceArgs } from './device.mjs';
import { udid } from './device-ios.mjs';

// (T313) the iOS simulator target: TESTBED_PLATFORM=ios (session-ios.sh) — the same flows on the booted simulator
const serial = () => (process.env.TESTBED_PLATFORM === 'ios' ? udid() : androidSerial());

/**
 * (T313, 2026-11-05) ON iOS MAESTRO IS KEPT OFF THE EMULATOR PORTS. It looks for Android emulators by connecting to
 * localhost:5555–5585 (adb's emulator ports) even with `--platform ios`, and waits for an adb answer with no timeout: a
 * program that listens there and never answers adb (a VS Code helper held 5555/5556 on 2026-11-05) left every iOS
 * call blocked for the full 6-minute timeout. A sandbox profile refuses those ports (any address: Java connects over IPv6-mapped sockets, which a "localhost" rule misses) to Maestro alone (it fails at once);
 * the listener itself is someone else's and is never touched. The adb server (5037) stays reachable.
 */
const EMULATOR_PORTS = Array.from({ length: 31 }, (_, i) => `(remote tcp "*:${5555 + i}")`).join(' ');
const NO_EMULATORS = `(version 1)(allow default)(deny network-outbound ${EMULATOR_PORTS})`;
/**
 * [command, args] for one Maestro call: on iOS, `sandbox-exec` around it and `--platform ios`.
 * ANDROID EMULATOR (T688 lane, 2026-11-12): Maestro asks the adb server for every device (`Dadb.list`), and one OFFLINE
 * entry throws away the whole list ("Device emulator-5580 was requested, but it is not connected"). A foreign listener on
 * 5555 (the VS Code helper, never touched) makes adb register a phantom offline `emulator-5554`, so every flow failed.
 * Maestro's hidden `--host/--port` connects straight to the emulator's own adb port (console N, adb N+1) and never asks
 * the server: used for every `emulator-<N>` serial (instead of `--device`). A physical device keeps `--device`.
 */
export function maestroCmd(bin, args) {
  if (process.env.TESTBED_PLATFORM !== 'ios') return [bin, args];
  return ['/usr/bin/sandbox-exec', ['-p', NO_EMULATORS, bin, '--platform', 'ios', ...args]];
}
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
  const args = [...(process.env.TESTBED_PLATFORM === 'ios' ? ['--device', serial()] : maestroDeviceArgs()), 'test', file, ...Object.entries(env).flatMap(([k, v]) => ['-e', `${k}=${v}`])];
  if (logDir) args.push('--test-output-dir', logDir);
  const t0 = Date.now();
  const [cmd, argv] = maestroCmd(BIN, args);
  const r = spawnSync(cmd, argv, {
    encoding: 'utf8', timeout: 6 * 60_000,
    env: { ...process.env, MAESTRO_CLI_NO_ANALYTICS: '1', MAESTRO_CLI_ANALYSIS_NOTIFICATION_DISABLED: 'true',
           JAVA_HOME: process.env.JAVA_HOME ?? '/Applications/Android Studio.app/Contents/jbr/Contents/Home' },
  });
  const log = `${r.stdout ?? ''}${r.stderr ?? ''}`;
  if (logDir) {
    fs.mkdirSync(logDir, { recursive: true });
    fs.writeFileSync(path.join(logDir, `${new Date().toISOString().replace(/[:.]/g, '-')}-${name}.log`), log);
  }
  const lines = log.split('\n').filter((l) => /FAILED|COMPLETED|Assertion|not found|Element/.test(l)).slice(-12).join('\n');
  return { ok: r.status === 0, ms: Date.now() - t0, log, said: lines.trim() };
}
