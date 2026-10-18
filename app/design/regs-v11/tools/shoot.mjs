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
  // What you can keep: "Where these rules apply" under the header; no part strip (v12, owner 5)
  ['01-keep-vedder-top', V],
  ['02-keep-cowichan-closed', 'w=cowichan&d=1010&p=0'],
  // Gear & licence: 1 Gear & methods (one what's-allowed block), 2 Always, 3 Licence last (v12, owner 3)
  ['03-gear-vedder-whats-allowed', `${V}&sec=gear`],
  ['04-gear-vedder-methods', `${V}&at=%5Bdata-k%3D%22methods%22%5D`],
  ['05-gear-vedder-always-licence', `${V}&at=%23g-always`],
  ['06-gear-vedder-licence', `${V}&at=%23g-licence`],
  ['07-gear-kootenay-boat-lines', 'w=klwa&d=1010&sec=gear'],
  ['08-gear-vedder-u16', `${V}&age=u16&at=%23g-licence`],
  ['09-gear-vedder-may15', 'w=vedder&d=515&sec=gear'],
  ['10-gear-kootenay-main-body', 'w=klmb&d=1010&sec=gear'],
  ['11-sheet-gear-sources', `${V}&sec=gear&sheet=srcs:line|vedder_1010`],
  ['12-gear-cowichan-closed', 'w=cowichan&d=1010&p=0&sec=gear'],
  // C: the water map ("Where these rules apply")
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
  ['27-dark-gear', `${V}&theme=dark&sec=gear`],
  ['28-dark-keep', `${V}&theme=dark`],
  ['29-text-200-gear', `${V}&ts=2&sec=gear`],
  ['30-text-200-map', `${V}&ts=2&map=1`],
  // the answers 2.8 quota card; v12: whole pictures under each kind's name
  ['31-card-vedder', `${V}&at=.g28`],
  ['32-card-vedder-how-it-adds-up', `${V}&at=.worked`],
  ['34-card-kootenay', 'w=klwa&d=1010&at=.g28'],
  ['35-card-cowichan-part2', 'w=cowichan&d=1010&p=1&at=.g28'],
  ['36-card-chilliwack', 'w=chilliwack&d=1010&p=0&at=.g28'],
  ['37-card-wilkes-plain-r2-stream', 'w=wilkes&d=1010&at=.g28'],
  ['38-card-anderson-plain-r3-stream', 'w=anderson&d=1010&at=.g28'],
  ['39-dark-card', `${V}&theme=dark&at=.g28`],
  ['40-text-200-card', `${V}&ts=2&at=.g28`],
  // v12 (round 10)
  ['41-keep-vedder-other-fish-and-nofish-note', `${V}&at=.nofish`],
  ['42-sheet-bull-trout-picture', `${V}&sheet=sp:DV`],
  ['43-sheet-other-fish-pictures', `${V}&at=.olist&sheet=sp:LT`],
  ['44-keep-cowichan-part2-where', 'w=cowichan&d=1010&p=1'],
  ['45-gear-kitimat-boats', 'w=x14p&d=1010&p=0&sec=gear'],
  ['46-sheet-parts-no-map', 'w=x14p&d=1010&p=0&sheet=parts'],
  // v13 (round 11): the place in the header (owner 4); bucket number + note, one-line kinds (owner 2); the gear sentence
  // with dated chips and the reference fold (owner 3); the inherited line (A3); braid review (owner 6)
  ['47-header-chilliwack-part', 'w=chilliwack&d=1010&p=0'],
  ['48-header-kitimat-other-parts', 'w=x14p&d=1010&p=1'],
  ['49-card-vedder-bucket', `${V}&at=.bkt`],
  ['50-gear-cowichan-fly-only-sentence', 'w=cowichan&d=1010&p=1&sec=gear'],
  ['51-gear-vedder-reference', `${V}&at=%23g-always`],
  ['52-keep-iron-jul20-inherited', 'w=iron&d=720'],
  ['53-braid-dean-in', `${V}&map=1&br=11&bj=2`],
  ['54-braid-dean-out', `${V}&map=1&br=12&bj=2`],
  ['55-braid-peace-site-c', `${V}&map=1&br=6&bj=1`],
  ['56-braid-review-panel', `${V}&map=1&br=3&bj=0`, 'desk'],
  // v14 (round 12): the header's place line (owner 1 + 4), compact; ⓘ sheets with the book's words folded and page links,
  // Bait two columns, ways inline, dated rows (owner 3); "How to tell" holds the hatchery term; a release-only group's species
  ['57-header-scrolled-same-shape', 'w=chilliwack&d=1010&p=0&scroll=900'],
  ['58-sheet-term-book-words', `${V}&sheet=gl:bait_ban,single_barbless_hook`],
  ['59-sheet-how-to-tell', `${V}&sheet=hatch&at=.htw`],      // user round 1 (E): shot over the water the panel reads (was Iron River, Jul 20)
  ['60-gear-cowichan-dated-rows', 'w=cowichan&d=1010&p=1&at=%23g-gear'],
  ['61-gear-kootenay-bait-ways', 'w=klwa&d=1010&at=%5Bdata-k%3D%22bait%22%5D'],
  ['62-keep-iron-char-release', 'w=iron&d=720&at=%5Bdata-row%3D%22r1%22%5D'],
  // round 12 phase 2 (answers rows 11): own bucket first, sizes said whole, every cap a bucket note, one "How it adds up"
  ['63-card-vedder-rows11', `${V}&at=.g28`],
  ['64-card-vedder-how-it-adds-up', `${V}&at=.worked`],
  ['65-card-wahleach-lake', 'w=x34p&d=1010&at=.g28'],
  ['66-card-bennett-lake-slot', 'w=x35p&d=1010&at=.g28'],
  ['67-card-morley-lake', 'w=x12p&d=1010&at=.g28'],
  ['68-card-bella-coola', 'w=bellacoola&d=1010&p=1&at=.g28'],
  // round 13: one header shape at the top and scrolled, slim tabs (H); Under 16 shows the default angler only, the other
  // residencies in their own "Rules for other anglers" dropdowns (L); Bella Coola on May 1, each part, and its quick pills (B)
  ['69-header-vedder-top', V],
  ['70-header-vedder-scrolled', `${V}&scroll=900`],
  ['71-licence-vedder-u16-you-need', `${V}&age=u16&at=%23g-licence`],
  ['72-licence-vedder-other-anglers-open', `${V}&age=u16&open=%5Bdata-res%3D%22non_resident%22%5D&at=%5Bdata-res%3D%22non_resident%22%5D`],
  ['73-card-bella-coola-may1', 'w=bellacoola&d=501&p=1&at=.g28'],
  ['74-card-bella-coola-side-channels-may1', 'w=bellacoola&d=501&p=0&at=.g28'],
  ['75-panel-bella-coola-pills', 'w=bellacoola&d=501&p=1', 'desk'],
  // round 13 phase 2 (rows 12, gear 9): no lift box, the own-rule bucket's replaces note (D); the fish nobody may keep (S);
  // a kind keeping none says "catch and release" (Z); Ape Creek, a Bella Coola tributary
  ['76-card-vedder-replaces', `${V}&at=.bkt`],
  ['77-card-bella-coola-not-kept', 'w=bellacoola&d=1010&p=1&at=.nkt'],
  ['78-card-ape-creek', 'w=ape&d=1010&at=.g28'],
  ['79-card-ape-creek-may1', 'w=ape&d=501&at=.nkt'],
  ['80-gear-vedder-sentence', `${V}&at=%23g-gear`],
  // round 14: nested count group (Bella Coola May 1), steelhead presence in its own section, weekday window in words
  // (Kootenay LWA kokanee, Sat + Mon), merged release lines (Vedder May 15, Ape Creek), Under 16 "You need"
  ['81-card-bella-coola-may1-group', 'w=bellacoola&d=501&p=1&at=.g28'],
  ['82-card-bella-coola-may1-steelhead', 'w=bellacoola&d=501&p=1&at=.nkt'],
  ['83-card-vedder-oct10', `${V}&at=.g28`],
  ['84-card-vedder-may15', 'w=vedder&d=515&at=.nkt'],
  ['85-sheet-kootenay-kokanee-sat', 'w=klwa&d=1010&sheet=other:klwa_1010|o0'],
  ['86-sheet-kootenay-kokanee-mon', 'w=klwa&d=1012&sheet=other:klwa_1012|o0'],
  ['87-card-ape-creek-char', 'w=ape&d=1010&at=.nkt'],
  // round 15: no bare 0 above "How it adds up" (Vedder May 15); the licence answer says whose it is; the weekday kokanee
  // sheet as one story (status block, then how it was decided)
  ['88-card-vedder-may15-no-bare-zero', 'w=vedder&d=515&at=.nkt'],
  ['89-licence-vedder-adult-for-whom', `${V}&at=%23g-licence`],
  ['90-licence-vedder-u16-for-whom', `${V}&age=u16&at=%23g-licence`],
  // round 15 phase 2 (rows 14, licence 5, display 11): size groups boxed + own-rule shared cap (Bella Coola May 1), the
  // tributaries rule (Ape Creek), hatchery in kind details (Vedder May 15), no presence on a closed day (Vedder Jun 15),
  // weekday parts + the combined-quota note (kokanee), the Under 16 summary by residency
  ['91-card-bella-coola-may1-size-group', 'w=bellacoola&d=501&p=0&at=.g28'],
  ['92-card-bella-coola-may1-side-channels', 'w=bellacoola&d=501&p=1&at=.g28'],
  ['93-card-ape-creek-oct10', 'w=ape&d=1010&at=.nkt'],
  ['94-card-ape-creek-may1', 'w=ape&d=501&at=.g28'],
  ['95-card-vedder-oct10', `${V}&at=.g28`],
  ['96-card-vedder-may15', 'w=vedder&d=515&at=.g28'],
  ['97-keep-vedder-jun15-closed', 'w=vedder&d=615'],
  ['98-licence-vedder-u16-by-residency', `${V}&age=u16&at=%23g-licence`],
  ['99-card-bella-coola-may1-size-box', 'w=bellacoola&d=501&p=0&at=.sgrp'],
  // round 15 follow-up: the two nearer-row tributaries on Jun 15 (Glenn: Trout Lake's rule for its tributaries releases
  // bull trout; Hamill: the Duncan River's rule keeps 2); the closed card saying each sentence once
  ['100-card-glenn-creek-jun15', 'w=glenn&d=615&at=.g28'],
  ['101-card-glenn-creek-jun15-not-kept', 'w=glenn&d=615&at=.nkt'],
  ['102-card-hamill-creek-jun15', 'w=hamill&d=615&at=.g28'],
  ['103-keep-vedder-jun15-closed-once', 'w=vedder&d=615'],
  // round 16: the gear sentence as labelled lines (5); How it adds up closes its group (2); the
  // (round 16 table screens 108-116 retired in round 17); rule titles, cautions, restatements (3, Q1, Q2)
  ['104-gear-vedder-lines', `${V}&sec=gear`],
  ['105-gear-vedder-may15-fly-only-lines', 'w=vedder&d=515&sec=gear'],
  ['106-gear-kootenay-main-body-lines', 'w=klmb&d=1010&sec=gear'],
  ['107-card-ape-creek-may1-adds-up-last', 'w=ape&d=501&at=.nkt'],
  ['117-sheet-bella-coola-sources-titles', `w=bellacoola&d=501&p=0&sheet=${encodeURIComponent('src:bc0_501|r0')}`],
  ['118-keep-glenn-may1-caution', 'w=glenn&d=501'],
  ['119-sheet-schooler-grayling-restates', `w=x41p&d=1010&sheet=${encodeURIComponent('other:x41p_1010|o3')}`],
  ['120-licence-vedder-u16-book-words', `${V}&age=u16&at=%23g-licence`],
  // round 17: the table and the catch checker are gone (108-116 retired); the bucket card, every row with its own number,
  // the size cap a compact line with the rows it covers, each covered row tagged
  ['121-card-bella-coola-may1-side-channels', 'w=bellacoola&d=501&p=0&at=.g28'],
  ['122-card-bella-coola-may1-side-channels-size-line', 'w=bellacoola&d=501&p=0&at=.szl'],
  ['123-card-bella-coola-may1-mainstem', 'w=bellacoola&d=501&p=1&at=.g28'],
  ['124-card-bella-coola-may1-mainstem-size-line', 'w=bellacoola&d=501&p=1&at=.szl'],
  ['125-card-vedder-oct10', `${V}&at=.g28`],
  ['126-card-vedder-oct10-size-line', `${V}&at=.szl`],
  ['127-card-vedder-may15', 'w=vedder&d=515&at=.g28'],
  ['128-card-bennett-lake', 'w=x35p&d=1010&at=.szl'],
  ['129-card-ape-creek-may1', 'w=ape&d=501&at=.g28'],
  ['130-card-hamill-creek-jun15', 'w=hamill&d=615&at=.g28'],
  // round 18: a real phone (390 x 844 inside, the app's tabs and home indicator); the compact header (Chilliwack part 2,
  // Oct 10: 179 -> 103 px); the group's top as one block
  ['131-phone-chilliwack-part2-header', 'w=chilliwack&d=1010&p=1'],
  ['132-header-chilliwack-part2-map-full-label', 'w=chilliwack&d=1010&p=1&map=1'],
  ['133-card-vedder-oct10-group-head', `${V}&at=.g28`],
  ['134-card-vedder-may15-group-head', 'w=vedder&d=515&at=.g28'],
  // round 18 phase 2: the quota as folders (group.tree) — tier lines, kinds as sentences, sizes as cm + in chips
  ['135-tree-vedder-oct10', `${V}&at=.qtree`],
  ['136-tree-vedder-oct10-share', `${V}&at=%5Bdata-tnode%3D%22share%22%5D`],
  ['137-tree-vedder-may15', 'w=vedder&d=515&at=.qtree'],
  ['138-tree-bella-coola-may1-side-channels', 'w=bellacoola&d=501&p=0&at=.qtree'],
  ['139-tree-bella-coola-may1-mainstem-count', 'w=bellacoola&d=501&p=1&at=%5Bdata-tnode%3D%22count%22%5D'],
  ['140-tree-glenn-jun15-why-here', 'w=glenn&d=615&at=.qtree'],
  ['141-tree-bennett-lake', 'w=x35p&d=1010&at=.qtree'],
  ['142-tree-ape-may1', 'w=ape&d=501&at=.qtree'],
  ['143-tree-kootenay-lwa', 'w=klwa&d=1010&at=.qtree'],
  ['144-dark-tree-vedder', `${V}&theme=dark&at=.qtree`],
  ['145-text-200-tree-vedder', `${V}&ts=2&at=.qtree`],
  // round 19 (owner 4): the label two lines at a fixed height (full label when it fits, else the short one), the map /
  // parts action a compact button on the name's row
  ['146-header-chilliwack-part2-two-lines', 'w=chilliwack&d=1010&p=1'],
  ['147-header-chilliwack-part1-full-label', 'w=chilliwack&d=1010&p=0'],
  ['148-header-kootenay-lwa-whole-water', 'w=klwa&d=1010'],
  ['149-header-west-road-long-name', 'w=x27p&d=1010'],
  ['150-header-text-200-chilliwack-part2', 'w=chilliwack&d=1010&p=1&ts=2'],
  // round 19 phase 2 (rows 17): a size tier is a box holding every kind it caps, the steelhead inside with its size note;
  // the origin in the fish names, no tag; "50 cm (20 in)"
  ['151-tree-chilliwack-part2-oct10', 'w=chilliwack&d=1010&p=1&at=.qtree'],
  ['152-tree-vedder-oct10-size-box', `${V}&at=%5Bdata-tnode%3D%22size%22%5D`],
  ['153-tree-chilliwack-part2-size-box', 'w=chilliwack&d=1010&p=1&at=%5Bdata-tnode%3D%22size%22%5D'],
];
const only = process.argv[2];
for (const [name, hash, desk] of SCREENS) {
  if (only && !name.includes(only)) continue;
  // 'desk': the desktop page with its side panel (the braid review list lives there)
  execFileSync(CHROME, ['--headless=new', '--hide-scrollbars', `--window-size=${desk ? '1280,1000' : '440,900'}`, '--virtual-time-budget=4000',
    `--screenshot=${path.join(OUT, name + '.png')}`, `${PAGE}#${desk ? '' : 'shot=1&'}${hash}`], { stdio: 'ignore' });
  console.log(name);
}
