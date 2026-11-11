#!/usr/bin/env node
// (T538) THE APP VERSION HAS ITS CHANGELOG ENTRY: app/apps/mobile/app.json `expo.version` has an entry in
// app/packages/core/src/changelog.ts with at least one bullet, and it is the NEWEST entry (the list is newest first).
// Caught: build-apk.sh only WARNED on a version with no entry and published `notes: []`, so the app said "No notes were
// published". Sub-second; run by scripts/fast-fail.sh and `pnpm check` (check:fast), and build-apk.sh refuses without it.
//
//   node scripts/check-changelog.mjs [--version X.Y.Z]      exit 0 ok, 1 missing / empty / not the newest
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const args = process.argv.slice(2);
const i = args.indexOf('--version');
const version = i >= 0 ? args[i + 1]
  : JSON.parse(fs.readFileSync(path.join(root, 'app/apps/mobile/app.json'), 'utf8')).expo.version;
const file = path.join(root, 'app/packages/core/src/changelog.ts');
const { CHANGELOG } = await import(pathToFileURL(file).href);
const fail = (m) => { console.error(`changelog: ${m}`); process.exit(1); };
const entry = CHANGELOG.find((e) => e.version === version);
if (!entry) fail(`app.json's version ${version} has no entry in app/packages/core/src/changelog.ts — add 3–5 plain bullets for anglers first`);
const bullets = (entry.notes ?? []).filter((n) => typeof n === 'string' && n.trim());
if (!bullets.length) fail(`${version}'s entry in changelog.ts has no bullet`);
if (CHANGELOG[0].version !== version) fail(`${version} is not the newest entry (the list is newest first; the first is ${CHANGELOG[0].version})`);
console.log(`changelog: ${version} has ${bullets.length} bullet(s) and is the newest entry${entry.draft ? ' (draft: the owner has not cleared it)' : ''}`);
