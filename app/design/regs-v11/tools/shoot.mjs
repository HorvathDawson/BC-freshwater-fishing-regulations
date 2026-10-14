// Re-shoot regs-v11's screens with headless Chrome (one CLI call per screen).
//   node app/design/regs-v11/tools/shoot.mjs [name-filter]   -> app/design/regs-v11/screens/*.png
import { execFileSync } from 'node:child_process';
import path from 'node:path'; import { fileURLToPath, pathToFileURL } from 'node:url';
import { CHROME } from './cdp.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PAGE = pathToFileURL(path.resolve(HERE, '../../regs-v11.html')).href;
const OUT = path.resolve(HERE, '../screens');
const V = 'w=vedder&d=1010';
const SCREENS = [
  // What you can keep: D2 "Where you can't fish" sits with the open/closed strip
  ['01-keep-vedder-top', V],
  ['02-keep-cowichan-closed', 'w=cowichan&d=1010&p=0'],
  // B: Gear & licence, one page
  ['03-gear-vedder-glance', `${V}&sec=gear`],
  ['04-gear-vedder-licence', `${V}&at=%23g-licence`],
  ['05-gear-vedder-your-gear', `${V}&at=%23g-line`],
  ['06-gear-vedder-bait-ways', `${V}&at=%23g-bait`],
  ['07-gear-vedder-always', `${V}&at=%23g-always`],
  ['08-gear-vedder-u16', `${V}&age=u16&at=%23g-licence`],
  ['09-gear-vedder-may15', 'w=vedder&d=515&at=%23g-line'],
  ['10-gear-kootenay', 'w=klwa&d=1010&at=%23g-line'],
  ['11-sheet-gear-sources', `${V}&sec=gear&sheet=srcs:line|vedder_1010`],
  ['12-gear-cowichan-closed', 'w=cowichan&d=1010&p=0&sec=gear'],
  // C: the water map
  ['13-map-vedder-open', `${V}&scroll=600&map=1`],
  ['14-map-vedder-step-up-1', `${V}&map=1&mp=1`],
  ['15-map-vedder-step-up-2', `${V}&map=1&mp=2`],
  ['16-map-vedder-boundary-tap', `${V}&map=1&mk=0`],
  ['17-map-vedder-whole-river', `${V}&map=1&mw=1`],
  ['18-map-cowichan-mouth-closed', 'w=cowichan&d=1010&p=0&map=1'],
  ['19-map-cowichan-step-down', 'w=cowichan&d=1010&p=1&map=1&mp=0'],
  ['20-map-kootenay-lake', 'w=klwa&d=1010&map=1'],
  ['21-map-kootenay-boundary', 'w=klwa&d=1010&map=1&mk=0'],
  ['22-map-vedder-jun15-closed', 'w=vedder&d=615&map=1'],
  ['23-regs-after-show-chilliwack', 'w=chilliwack&d=1010&p=0'],
  ['24-map-roundtrip-1-regs-scrolled', `${V}&scroll=600`],
  ['25-map-roundtrip-2-back-same-scroll', `${V}&scroll=600`],
  // dark mode and 200% text
  ['26-dark-map', `${V}&theme=dark&map=1&mk=0`],
  ['27-dark-gear', `${V}&theme=dark&at=%23g-line`],
  ['28-dark-keep', `${V}&theme=dark`],
  ['29-text-200-gear', `${V}&ts=2&at=%23g-line`],
  ['30-text-200-map', `${V}&ts=2&map=1`],
  // round 2: the answers 2.8 quota card
  ['31-card-vedder', `${V}&at=.g28`],
  ['32-card-vedder-how-it-adds-up', `${V}&at=.worked`],
  ['33-sheet-fished-elsewhere', `${V}&sheet=else:vedder_1010|0`],
  ['34-card-kootenay', 'w=klwa&d=1010&at=.g28'],
  ['35-card-cowichan-part2', 'w=cowichan&d=1010&p=1&at=.g28'],
  ['36-card-chilliwack', 'w=chilliwack&d=1010&p=0&at=.g28'],
  ['37-card-wilkes-plain-r2-stream', 'w=wilkes&d=1010&at=.g28'],
  ['38-card-anderson-plain-r3-stream', 'w=anderson&d=1010&at=.g28'],
  ['39-dark-card', `${V}&theme=dark&at=.g28`],
  ['40-text-200-card', `${V}&ts=2&at=.g28`],
];
const only = process.argv[2];
for (const [name, hash] of SCREENS) {
  if (only && !name.includes(only)) continue;
  execFileSync(CHROME, ['--headless=new', '--hide-scrollbars', '--window-size=440,900', '--virtual-time-budget=4000',
    `--screenshot=${path.join(OUT, name + '.png')}`, `${PAGE}#shot=1&${hash}`], { stdio: 'ignore' });
  console.log(name);
}
