// (T255) The scenarios, in run order. Each is { id, title, run(ctx) } (run.mjs's ctx). `install` comes first: it
// leaves the app holding the current staging release, which every later scenario builds on (none clears the app's
// data again — a fresh install is ~500 MB).
import { install } from './install.mjs';
import { feeds } from './feeds.mjs';
import { bigUpdate } from './big-update.mjs';
import { offline } from './offline.mjs';
import { spots } from './spots.mjs';
import { report } from './report.mjs';
import { crash } from './crash.mjs';
import { nearCold } from './near-cold.mjs';
import { pan } from './pan.mjs';
import { satellite } from './satellite.mjs';

export const SCENARIOS = [install, feeds, bigUpdate, offline, spots, report, crash, nearCold, satellite, pan];
