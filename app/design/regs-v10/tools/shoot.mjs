// Re-shoot regs-v10's screens with headless Chrome (one CLI call per screen).
//   node app/design/regs-v10/tools/shoot.mjs [name-filter]   -> app/design/regs-v10/screens/*.png
import { execFileSync } from 'node:child_process';
import path from 'node:path'; import { fileURLToPath, pathToFileURL } from 'node:url';
import { CHROME } from './cdp.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PAGE = pathToFileURL(path.resolve(HERE, '../../regs-v10.html')).href;
const OUT = path.resolve(HERE, '../screens');
const V = 'w=vedder&d=1010';
const SCREENS = [
  ['02-regs-vedder-top', V],
  ['03-regs-vedder-group', `${V}&at=.lead`],
  ['04-regs-vedder-kinds', `${V}&at=.sblock`],
  ['05-regs-vedder-other', `${V}&at=.olist`],
  ['06-sheet-species', `${V}&sheet=sp:EB`],
  ['07-sheet-kind-decided', `${V}&sheet=kind:vedder_1010|r0|0`],
  ['08-sheet-hatchery', `${V}&sheet=hatch`],
  ['09-regs-cowichan-closed', 'w=cowichan&d=1010&p=0'],
  ['10-regs-vedder-jun-closed', 'w=vedder&d=615'],
  ['11-regs-kootenay-sat', 'w=klwa&d=1010'],
  ['12-regs-kootenay-mon', 'w=klwa&d=1012'],
  ['13-gear-vedder', `${V}&sec=gear`],
  ['14-gear-vedder-line-bait', `${V}&sec=gear&kit=line&at=.ktile.on`],
  ['15-gear-vedder-ways-always', `${V}&sec=gear&kit=always&at=.ktile.on`],
  ['16-gear-vedder-u16', `${V}&sec=gear&kit=licence&age=u16&at=.ktile.on`],
  ['17-gear-cowichan-closed', 'w=cowichan&d=1010&p=0&sec=gear'],
  ['18-sheet-rule', `${V}&sheet=rule:z2:trout_char_quota::trout_char_quota.r1`],
  ['19-sheet-glossary', `${V}&sheet=gl:region_2,stream`],
  ['20-sheet-date', `${V}&sheet=date`],
  ['21-dark-regs', `${V}&theme=dark&at=.sblock`],
  ['22-dark-gear', `${V}&theme=dark&sec=gear&kit=bait&at=.ktile.on`],
  ['23-sheet-source', `${V}&sheet=src:vedder_1010|r0`],
  ['24-map-roundtrip-1-regs-scrolled', `${V}&scroll=600`],
  ['25-map-roundtrip-2-water-map', `${V}&scroll=600&map=1`],
  ['26-map-roundtrip-3-back-same-scroll', `${V}&scroll=600`],
  ['27-map-cowichan', 'w=cowichan&d=1010&p=1&map=1'],
  ['28-regs-cowichan-part2', 'w=cowichan&d=1010&p=1'],
  ['29-text-200', `${V}&ts=2`],
  ['35-regs-vedder-may15', 'w=vedder&d=515'],
  ['36-regs-vedder-may15-kinds', 'w=vedder&d=515&at=.sblock'],
  ['37-gear-vedder-may15', 'w=vedder&d=515&sec=gear&kit=licence&at=.ktile.on'],
  ['38-sheet-other-fish', `${V}&sheet=other:vedder_1010|o0`],
  ['39-sheet-fished-elsewhere', `${V}&sheet=else:vedder_1010|0`],
];
const only = process.argv[2];
for (const [name, hash] of SCREENS) {
  if (only && !name.includes(only)) continue;
  execFileSync(CHROME, ['--headless=new', '--hide-scrollbars', '--window-size=440,900', '--virtual-time-budget=4000',
    `--screenshot=${path.join(OUT, name + '.png')}`, `${PAGE}#shot=1&${hash}`], { stdio: 'ignore' });
  console.log(name);
}
