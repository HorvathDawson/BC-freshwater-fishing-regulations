// (T723 F2, G10) DOES THE iOS SOFTWARE KEYBOARD COVER THE FIELD BEING TYPED IN? Each step opens one at-risk field, types
// a few characters with the software keyboard up, and screenshots it (`T723-kb-<field>.png` beside the report). The
// accessibility tree does not list the iOS keyboard, so its top edge is read off the screenshot's pixels
// (`lib/png.mjs keyboardTop`, checked by eye once) and compared with the field's frame from the tree: a field whose box
// ends below the keyboard's top is COVERED (a finding). Android resizes the window for the keyboard: iOS-only by nature.
//
//   node app/tools/testbed/run-ios.mjs keyboard       (a --testbed build; run through session-ios.sh)
//
// Places, not words, for the tab bar: on iOS "More", "Spots" … are also words on the pages, and a text tap took the first
// one (the run was left on Runs). The simulator hides its software keyboard while "Connect Hardware Keyboard" is on:
// `softKeyboard()` turns that off for THIS device only (Simulator's per-device preference).
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { runYaml } from '../lib/ios-flow.mjs';
import { keyboardTop, readPng } from '../lib/png.mjs';

const APP = 'ca.canifishthis.app';
/** the tab bar by place (iPhone 17e: five tabs, centres at 10/30/50/70/90% across, 92% down) */
export const TABS = { map: '10%,92%', conditions: '30%,92%', runs: '50%,92%', spots: '70%,92%', more: '90%,92%' };
/** Cultus Lake's middle: any tap near the capture's pin picks the lake (scenarios/spots.mjs LAKE) */
const LAKE = { lon: -121.9870, lat: 49.0535 };

/** The software keyboard on for this simulator only (Simulator.app's DevicePreferences, keyed by udid). */
export function softKeyboard(device, on = true) {
  const id = device.udid();
  try {
    execFileSync('defaults', ['write', 'com.apple.iphonesimulator', 'DevicePreferences', '-dict-add', id,
      `<dict><key>ConnectHardwareKeyboard</key><${on ? 'false' : 'true'}/></dict>`], { encoding: 'utf8' });
  } catch (e) { return `defaults failed: ${e.message.split('\n')[0]}`; }
  return `ConnectHardwareKeyboard=${!on} for ${id}`;
}

/**
 * More → its row `key` (`menu-item-<key>`: by id, the words are also on the panes kept in the tree under it). Centred
 * first: the last row ("report") counts as visible while the tab bar lies over it, and a tap at its middle opened Runs.
 */
export async function menuPage(ctx, key) {
  ctx.flow('tap-point', { POINT: TABS.more });
  await ctx.device.sleep(1000);
  must(ctx, `more-${key}`, `- scrollUntilVisible:\n    element: { id: "menu-item-${key}" }\n    centerElement: true\n    timeout: 20000\n- tapOn: { id: "menu-item-${key}" }\n`);
  await ctx.device.sleep(1500);
}

/**
 * The map tab, nothing over it: `toMap` stops as soon as the tab bar is in the tree, which it also is under the search
 * sheet (a step that failed mid-search left it up and every later step started there). The keyboard down, the
 * search closed by its ‹ (place: it has no words on iOS), then `toMap`.
 */
export async function home(ctx) {
  const d = ctx.device;
  for (let i = 0; i < 3 && d.screenText().some((x) => /NAMED WATERS/i.test(x)); i++) {
    runYaml(ctx.dir, 'home-hide', `appId: ${APP}\n---\n- hideKeyboard\n`);
    try { ctx.flow('tap-point', { POINT: '9%,9.5%' }); } catch { /* the next look says */ }
    await d.sleep(1200);
  }
  await ctx.ios.toMap(ctx);
}

/** A literal flow; throws with Maestro's words when it fails. */
function must(ctx, name, steps) {
  const r = runYaml(ctx.dir, name, `appId: ${APP}\n---\n${steps}`);
  if (!r.ok) throw new Error(`${name}: ${r.said || 'see flows-ios/'}`);
  return r;
}
const sel = (target) => (target.startsWith('id:') ? `{ id: "${target.slice(3)}" }` : `{ text: "${target}", index: 0 }`);

/**
 * The screen now against the keyboard: a screenshot `T723-kb-<name>.png`, the keyboard's top from its pixels, the field
 * (`find`: an id, or a test on the node) from the tree. Returns { note, covered, kbTop, field } (iOS points).
 */
export function measure(ctx, name, find) {
  const file = path.join(ctx.dir, `T723-kb-${name}.png`);
  ctx.device.screenshot(file);
  const img = readPng(file);
  const ns = ctx.device.nodes();
  const win = ns.find((n) => n.box.x0 === 0 && n.box.y0 === 0 && n.box.x1 > 0);   // the app's window, not the widest node
  const W = win ? win.box.x1 : 390;
  const scale = img.w / W;                                   // pixels per iOS point
  const px = keyboardTop(img);
  const kbTop = px === null ? null : px / scale;
  const field = typeof find === 'string' ? ns.find((n) => n.id === find) : ns.find(find);
  if (kbTop === null) return { note: `${name}: no keyboard drawn (T723-kb-${name}.png)`, covered: null, kbTop, field };
  if (!field) return { note: `${name}: keyboard top ${Math.round(kbTop)} pt; the field is not in the tree (look at T723-kb-${name}.png)`, covered: null, kbTop, field };
  const { y0, y1 } = field.box;
  const covered = y1 > kbTop + 1 ? (y0 >= kbTop - 1 ? 'COVERED' : 'PARTLY COVERED') : 'clear';
  const note = `${name}: field ${Math.round(y0)}-${Math.round(y1)} pt, keyboard top ${Math.round(kbTop)} pt → ${covered}`;
  if (covered !== 'clear') ctx.finding(`F2 ${note}`);
  return { note, covered, kbTop, field };
}

/** Tap `target` (a Maestro text regex or `id:<testID>`), type `text`, and measure it against the keyboard. */
async function typeIn(ctx, name, target, text, find = target.startsWith('id:') ? target.slice(3) : (n) => (n.text || '').includes(text.trim())) {
  // target null: the field already has the focus (autoFocus); a tap on a field under the keyboard lands on a key
  must(ctx, `kb-${name}`, `${target ? `- tapOn: ${sel(target)}\n` : ''}- inputText: "${text}"\n`);
  await ctx.device.sleep(1200);
  return measure(ctx, name, find);
}

/** The keyboard still drawn? (its pixels: the tree does not list it) */
function keyboardUp(ctx, name) {
  const file = path.join(ctx.dir, `T723-kb-${name}.png`);
  ctx.device.screenshot(file);
  return keyboardTop(readPng(file)) !== null;
}

/** (iOS) Spots → At my location → the capture's steps → the draft (its "Save spot"); scenarios/spots.mjs capture, by place. */
async function captureIos(ctx) {
  const d = ctx.device;
  let picks = 0;
  for (let i = 0; i < 12; i++) {
    const w = d.screenText();
    if (w.some((x) => /^(Save spot|Save)$/.test(x))) return w;
    if (w.some((x) => /Allow .* to use your location/.test(x))) ctx.flow('ios-alert', { BUTTON: 'Allow While Using App' });
    else if (w.some((x) => /^Pick the water$|^Now the exact spot$/.test(x))) {
      const off = [4, 0, 1, -1, 2, -2][picks++ % 6];                 // % of the height: below the pin, on it, either side
      ctx.flow('tap-point', { POINT: `50%,${44 + off}%` });
      await d.sleep(1500);
      ctx.flow('tap-text', { TEXT: 'Next' });
    } else if (w.includes('Continue')) ctx.flow('tap-text', { TEXT: 'Continue' });
    else if (w.includes('Next')) ctx.flow('tap-text', { TEXT: 'Next' });
    await d.sleep(2500);
  }
  throw new Error(`no "Save spot" after the capture's steps (saw: ${d.screenText().slice(0, 10).join(' | ').slice(0, 300)})`);
}

export const keyboardIos = {
  id: 'keyboard',
  inAll: false,
  title: 'T723 F2/G10: the software keyboard against each at-risk field (iOS)',
  async run(ctx) {
    const { appUp, toMap } = ctx.ios;
    const d = ctx.device;
    const open = (name, steps) => ctx.step(name, async () => { await home(ctx); return steps(); });
    await ctx.step('software keyboard on for this simulator', async () => {
      const said = softKeyboard(d, true);
      // a first open downloads the whole staging release (~7 min): the app is NOT restarted meanwhile
      await appUp(ctx, { launch: !d.pidOf(), ms: 1_200_000 });
      return said;
    });
    await open('report a problem: the what-happened field', async () => {
      await menuPage(ctx, 'report');
      return (await typeIn(ctx, 'report', 'id:report-what', 'keyboard probe')).note;
    });
    await open('glossary search (a menu page, field at the top)', async () => {
      await menuPage(ctx, 'glossary');
      return (await typeIn(ctx, 'glossary', 'Search.*', 'bait')).note;
    });
    await open('notifications: the gauge search, a pick with the keyboard up, the alert value', async () => {
      await menuPage(ctx, 'notifications');
      ctx.flow('scroll-to', { TEXT: 'Add an alert' });
      ctx.flow('tap-text', { TEXT: 'Add an alert' });
      await d.sleep(1500);
      // (G10) autocorrect: "Coquihala " (one l short) typed; iOS autocorrect on space would rewrite it
      // the field takes the focus itself (autoFocus): no tap (under the keyboard, a tap typed a "T": the key there)
      const s = await typeIn(ctx, 'notify-search', null, 'Coquihala ', 'notify-add-search');
      const back = s.field?.text ?? '';
      const fixed = back !== '' && back.trim() !== 'Coquihala';
      if (fixed) ctx.finding(`G10: the gauge search was autocorrected: typed "Coquihala " read back "${back}"`);
      // a real name for hits, then (G10) ONE tap on the first hit with the keyboard up: does it pick, or only close it?
      must(ctx, 'kb-notify-name', '- eraseText: 40\n- inputText: "Coquihalla"\n');
      await d.sleep(2000);
      const kb = measure(ctx, 'notify-hits', 'notify-add-search').kbTop;
      let hit = d.nodes().find((n) => /^notify-add-hit-/.test(n.id));
      let tap = 'no gauge hit listed';
      // with room to scroll above the keyboard (the fix), a reader drags the list up a little with the keyboard still up
      if (hit && kb !== null && (hit.box.y0 + hit.box.y1) / 2 > kb) {
        must(ctx, 'kb-notify-drag', '- swipe: { start: "50%, 45%", end: "50%, 25%", duration: 600 }\n');
        await d.sleep(1000);
        const again = d.nodes().find((n) => n.id === hit.id);
        const kb2 = measure(ctx, 'notify-hits-dragged', 'notify-add-search').kbTop;
        if (again && kb2 !== null && (again.box.y0 + again.box.y1) / 2 < kb2) hit = again;
      }
      if (hit && kb !== null && (hit.box.y0 + hit.box.y1) / 2 > kb) {
        tap = `the first hit (${Math.round(hit.box.y0)} pt) is UNDER the keyboard (${Math.round(kb)} pt): not tappable; keyboard hidden to reach it`;
        must(ctx, 'kb-notify-hide', '- hideKeyboard\n');
        ctx.flow('tap-id', { ID: hit.id });
        await d.sleep(4000);
      } else if (hit) {
        const x = Math.round((hit.box.x0 + hit.box.x1) / 2), y = Math.round((hit.box.y0 + hit.box.y1) / 2);
        ctx.flow('ios-tap-at', { POINT: `${x},${y}` });
        await d.sleep(2500);
        const picked = d.screenText().some((x) => /^Alert on /.test(x)) || d.hasId('alert-form-value');
        d.screenshot(path.join(ctx.dir, 'T723-kb-notify-pick-once.png'));
        tap = picked ? 'one tap picked the gauge' : 'EATEN (one tap only closed the keyboard)';
        if (!picked) { ctx.finding('G10: with the keyboard up, the first tap on a gauge hit did not pick it'); ctx.flow('tap-id', { ID: hit.id }); await d.sleep(2500); }
      }
      return `${s.note}; read back "${back}" (${fixed ? 'AUTOCORRECTED' : 'kept'}); ${tap}`;
    });
    await ctx.step('notifications: the alert value field (decimal pad) and how the pad closes', async () => {
      // the editor's value box is below the fold (the tree lists only what is on screen): scrolled to as a reader would
      if (!d.screenText().some((x) => /^Alert on /.test(x)) && !d.hasId('alert-form-value')) ctx.skip('the alert editor is not open (the step before did not pick a gauge)');
      must(ctx, 'kb-alert-scroll', '- scrollUntilVisible:\n    element: { id: "alert-form-value" }\n    timeout: 10000\n');
      const v = await typeIn(ctx, 'alert-value', 'id:alert-form-value', '12');
      // (G10) the decimal pad has no return key: a Done above it (an accessory bar) is in the app's tree when there is one
      const done = d.nodes().find((n) => /^(Done)$/.test(n.text || n.desc) && v.kbTop !== null && n.box.y0 >= v.kbTop - 60);
      let closes = 'not tried';
      if (done) {
        ctx.flow('ios-tap-at', { POINT: `${Math.round((done.box.x0 + done.box.x1) / 2)},${Math.round((done.box.y0 + done.box.y1) / 2)}` });
        await d.sleep(1200);
        closes = keyboardUp(ctx, 'alert-value-done') ? 'Done tapped: the pad STAYED' : 'Done tapped: the pad closed';
      } else {
        ctx.finding('G10: the number pad has no Done / Return key');
        // the only other way: a tap on the page's own words (the editor's title), away from any control
        ctx.flow('tap-point', { POINT: '50%,20%' });
        await d.sleep(1200);
        closes = keyboardUp(ctx, 'alert-value-tap-away') ? 'no Done; a tap on the page head: the pad STAYED' : 'no Done; a tap on the page head closed it';
      }
      try { ctx.flow('tap-id', { ID: 'alert-form-cancel' }); } catch { /* the page closes it anyway */ }
      return `${v.note}; Done key ${done ? 'present' : 'ABSENT'}; ${closes}`;
    });
    await open('saved spot: title and notes on a new spot; one tap on Save spot with the keyboard up', async () => {
      d.privacy('grant');
      d.geo(LAKE.lon, LAKE.lat);
      ctx.flow('tap-point', { POINT: TABS.spots });
      await d.sleep(1500);
      // (T819 item 11) with spots saved, the ways to add sit behind the header's Add (`spots-add`)
      if (d.hasId('spots-add') && !d.hasId('spots-adds')) { ctx.flow('tap-id', { ID: 'spots-add' }); await d.sleep(1000); }
      ctx.flow('tap-text', { TEXT: 'At my location' });
      await d.sleep(3000);
      await captureIos(ctx);
      const name = `kb probe ${Date.now().toString(36).slice(-4)}`;
      must(ctx, 'kb-spot-title-clear', '- tapOn: { text: "Name this spot", index: 0 }\n- eraseText: 60\n');
      const t = await typeIn(ctx, 'spot-title', 'Name this spot', name, (n) => n.text === name);
      // as a reader: the keyboard down, the page scrolled until the notes box shows, then a tap in it (the box is read by
      // its label "Notes" and its height from the tree: the block's heading has the same word, one line tall)
      // the notes box straight from the title when it shows above the keyboard (no keyboard dance); else, as a reader, the
      // keyboard down and the page scrolled to it. (L6 2026-11-20: Maestro's hideKeyboard failed on this page, and after a
      // drag down the page its scrollUntilVisible did not find "Add from photos" on screen: both kept as the fallback)
      const notesBox = (ns) => ns.find((n) => (n.desc === 'Notes' || /^What happened/.test(n.text)) && n.box.y1 - n.box.y0 > 60);
      // (L6 2026-11-20) a drag down the page with the keyboard (and its Save bar) up: the keyboard goes away, and the app's
      // main thread does not spin. With keyboardDismissMode "interactive" it stayed half way down and the app logged ~7,000
      // keyboard preference reads a second ("KeyboardBias") for 45 minutes
      d.logcatClear();
      must(ctx, 'kb-spot-drag', '- swipe: { start: "50%, 40%", end: "50%, 97%", duration: 700 }\n');
      await d.sleep(5000);
      const spin = d.appLogLines().filter((l) => l.includes('KeyboardBias')).length;
      const kbAfterDrag = keyboardUp(ctx, 'spot-after-drag');
      const dragSaid = `a drag down: keyboard ${kbAfterDrag ? 'STILL UP' : 'away'}, ${spin} keyboard preference reads in 5 s`;
      if (spin > 500) throw new Error(`${dragSaid}: the main thread is looping (T723-kb-spot-after-drag.png)`);
      let ns = d.nodes();
      const seen = notesBox(ns);
      if (!(seen && seen.box.y1 < 800 && (!kbAfterDrag || (t.kbTop !== null && seen.box.y0 + 30 < t.kbTop)))) {
        must(ctx, 'kb-spot-notes-find', '- hideKeyboard\n- scrollUntilVisible:\n    element: { text: "Add from photos" }\n    timeout: 15000\n');
        await d.sleep(800);
        ns = d.nodes();
      }
      const box = notesBox(ns);
      // else by place: the box (96 pt tall) ends ~50 pt above "Add from photos" (iPhone 17e, T723-kb shots)
      const add = ns.find((n) => /^Add from photos$/.test(n.text || n.desc));
      if (!box && !add) { fs.writeFileSync(path.join(ctx.dir, 'T723-kb-spot-tree.json'), JSON.stringify(ns, null, 1)); throw new Error('neither the notes box nor "Add from photos" is in the tree (T723-kb-spot-tree.json)'); }
      const at = box ? [(box.box.x0 + box.box.x1) / 2, box.box.y0 + 20] : [(add.box.x0 + 150), add.box.y0 - 90];
      ctx.flow('ios-tap-at', { POINT: `${Math.round(at[0])},${Math.round(at[1])}` });
      const nn = await typeIn(ctx, 'spot-notes', null, 'kb notes', (n) => n.text === 'kb notes');
      // (G10) ONE tap on "Save spot" with the keyboard up: it must save, not only close the keyboard
      // (T723, owner 2026-11-16) the Save row lifted: the same Save rides on the keyboard's bar (`spot-save-bar-notes-save`, an
      // InputAccessoryView, so its box is ABOVE the keys and the pixel read of the keyboard's top may include the bar);
      // before the fix only the page's row exists, under the keyboard
      const ns2 = d.nodes();
      // the bar lives in the keyboard's own window, so it may be missing from the app's tree: then it is read off the
      // screenshot (`T723-kb-spot-notes.png`) and tapped where it rides, just above the keys (the kb top read off the pixels
      // is the keys' top: the bar is white like the page, iPhone 17e shots, its Save ~35 pt above it)
      const onBar = ns2.find((n) => n.id === 'spot-save-bar-notes-save');
      const row = ns2.find((n) => /^(Save spot)$/.test(n.text || n.desc) && n.id !== 'spot-save-bar-notes-save');
      let once = 'Save spot not in the tree';
      const barSaid = onBar ? `the keyboard's bar carries Save (${Math.round(onBar.box.y0)}-${Math.round(onBar.box.y1)} pt; keyboard top read ${nn.kbTop === null ? 'none' : Math.round(nn.kbTop)} pt)`
        : `the bar is not in the tree (the keyboard's window): Save tapped where it rides, ${nn.kbTop === null ? '?' : Math.round(nn.kbTop - 35)} pt`;
      const tapAt = onBar ? [(onBar.box.x0 + onBar.box.x1) / 2, (onBar.box.y0 + onBar.box.y1) / 2]
        : nn.kbTop !== null ? [195, nn.kbTop - 35] : null;
      if (row && nn.kbTop !== null && row.box.y0 >= nn.kbTop) ctx.finding(`(for the record) the page's own Save row (${Math.round(row.box.y0)} pt) is under the keyboard, as designed: the bar carries Save`);
      if (tapAt) {
        ctx.flow('ios-tap-at', { POINT: `${Math.round(tapAt[0])},${Math.round(tapAt[1])}` });
        await d.sleep(2500);
        d.screenshot(path.join(ctx.dir, 'T723-kb-spot-save-once.png'));
        const still = d.screenText().includes('Save spot');
        once = still ? 'EATEN (the draft is still open)' : 'saved';
        if (still) ctx.finding('the first tap on the keyboard bar\'s Save (keyboard up) did not save');
      }
      if (once !== 'saved') {
        if (d.screenText().includes('Save spot')) { try { must(ctx, 'kb-spot-save', '- hideKeyboard\n- scrollUntilVisible:\n    element: { text: "Save spot" }\n    timeout: 15000\n- tapOn: { text: "Save spot" }\n'); } catch { /* said below */ } await d.sleep(2500); }
        throw new Error(`${t.note}; ${dragSaid}; ${nn.note}; ${barSaid}; one tap on Save spot: ${once}`);
      }
      return `${t.note}; ${dragSaid}; ${nn.note}; ${barSaid}; one tap on Save spot: ${once}`;
    });
    await open("alert fields on a water's Conditions (Coquihalla River → Conditions → Alert me)", async () => {
      ctx.flow('search', { Q: 'Coquihalla River' });
      // the row by its second line: the field's own value matches the name, and a tap there opened the edit menu
      ctx.flow('open-result', { LINE: '.*near Hope.*' });
      await d.sleep(2500);
      // Maestro's own scroll starts mid-screen, which is the page's small map here (it panned the map): swiped from low on
      // the page instead, until "Alert me" shows
      must(ctx, 'kb-cond-tab', '- tapOn: { text: "Conditions", index: 0 }\n');
      for (let i = 0; i < 8 && !d.hasId('alert-me-open'); i++) {
        must(ctx, `kb-cond-swipe-${i}`, '- swipe: { start: "50%, 90%", end: "50%, 45%", duration: 700 }\n');
        await d.sleep(800);
      }
      must(ctx, 'kb-cond-alert-open', '- tapOn: { id: "alert-me-open" }\n');
      await d.sleep(1500);
      for (let i = 0; i < 4 && !d.hasId('alert-form-value'); i++) {
        must(ctx, `kb-cond-swipe-v${i}`, '- swipe: { start: "50%, 90%", end: "50%, 60%", duration: 700 }\n');
        await d.sleep(800);
      }
      const v = await typeIn(ctx, 'cond-alert-value', 'id:alert-form-value', '12');
      try { ctx.flow('tap-id', { ID: 'alert-form-cancel' }); } catch { /* left to toMap */ }
      return v.note;
    });
    await home(ctx);
  },
};
