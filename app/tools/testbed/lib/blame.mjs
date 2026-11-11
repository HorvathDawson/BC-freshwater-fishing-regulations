// (T542) A FAILED STEP NAMES WHERE TO LOOK: the commit that last touched the scenario file it ran from, and the app screen
// file(s) holding the testIDs / labels the step used (so a rename shows up as "that file changed in <commit>").
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

const HERE = path.dirname(new URL(import.meta.url).pathname);
const ROOT = path.resolve(HERE, '../../../..');

/** The scenarios/*.mjs file that declares `id: '<id>'`, or null. */
export function scenarioFile(id, dir = path.join(HERE, '../scenarios')) {
  const hit = fs.readdirSync(dir).filter((n) => n.endsWith('.mjs')).find((n) => fs.readFileSync(path.join(dir, n), 'utf8').includes(`id: '${id}'`));
  return hit ? path.join(dir, hit) : null;
}

/** The ids / words a flow call carried (the env values a step passes to Maestro), for the screen lookup. */
export function usedValues(calls) {
  const out = new Set();
  for (const { name, env } of calls) {
    for (const k of ['ID', 'TEXT', 'TAB']) {
      const v = env?.[k];
      if (typeof v === 'string' && v.replace(/\\./g, '.').length >= 3) out.add(v.replace(/\\\./g, '.').replace(/[.*+?()[\]|].*$/, '').trim());
    }
    if (name === 'tap-sync') out.add('tap-sync');
  }
  return [...out].filter((v) => v.length >= 3).sort();
}

const gitGrep = (needle) => {
  try { return execFileSync('git', ['-C', ROOT, 'grep', '-l', '-F', '-e', needle, '--', 'app/apps', 'app/packages', ':!*.test.*'], { encoding: 'utf8' }).split('\n').filter(Boolean); }
  catch { return []; }   // git grep exits 1 for no match
};

/** { commit, screens }: for the step's failure line in the report. `grep` is injectable for tests. */
export function blame({ scenarioPath, calls, grep = gitGrep, log = (p) => execFileSync('git', ['-C', ROOT, 'log', '-1', '--format=%h %s', '--', p], { encoding: 'utf8' }).trim() }) {
  const commit = scenarioPath ? log(path.relative(ROOT, scenarioPath)) : '';
  const screens = [...new Set(usedValues(calls).flatMap((v) => grep(v)))].sort().slice(0, 6);
  return { scenario: scenarioPath ? path.relative(ROOT, scenarioPath) : '', commit, screens };
}

export const blameLine = (b) => `step defined in ${b.scenario || '?'} (last changed ${b.commit || 'unknown'}); app files it reads: ${b.screens.join(', ') || 'none found for the ids/labels it used'}`;
