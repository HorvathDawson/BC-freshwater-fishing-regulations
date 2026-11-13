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
import { crashBed } from './crash-bed.mjs';
import { nearCold } from './near-cold.mjs';
import { pan } from './pan.mjs';
import { perf } from './perf.mjs';
import { satellite } from './satellite.mjs';
import { regsOpen } from './regs-open.mjs';
import { updateFreeze } from './update-freeze.mjs';
import { bgInstall } from './bg-install.mjs';
import { locate } from './locate.mjs';
import { gestures } from './gestures.mjs';
import { walkthrough } from './walkthrough.mjs';
import { gaugeOffline } from './gauge-offline.mjs';
import { memory } from './memory.mjs';
import { waterNav } from './water-nav.mjs';
import { heapprof } from './heapprof.mjs';
import { edge } from './edge.mjs';
import { retireSwap } from './retire-swap.mjs';
import { feel } from './feel.mjs';
import { partSteps } from './part-steps.mjs';
import { tabSwitch } from './switch.mjs';
import { glide } from './glide.mjs';
import { photoZoom } from './photo-zoom.mjs';
import { stepShots } from './step-shots.mjs';
import { mapIdle, paneOpen } from './pane.mjs';
import { switchPan } from './switch-pan.mjs';
import { regsSwitch } from './regs-switch.mjs';
import { savedSpots } from './saved-spots.mjs';
import { twoDays } from './two-days.mjs';
import { routeScroll } from './route-scroll.mjs';
import { zoomFlick } from './zoom-flick.mjs';

export const SCENARIOS = [install, feeds, bigUpdate, offline, spots, savedSpots, twoDays, routeScroll, report, crash, crashBed, nearCold, satellite, pan, perf, regsOpen, updateFreeze, bgInstall, locate, gestures, walkthrough, gaugeOffline, memory, waterNav, heapprof, edge, feel, partSteps, tabSwitch, glide, photoZoom, stepShots, paneOpen, mapIdle, switchPan, regsSwitch, retireSwap, zoomFlick];
