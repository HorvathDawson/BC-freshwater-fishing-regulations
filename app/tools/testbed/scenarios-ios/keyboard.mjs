// (T723 F2) DOES THE iOS SOFTWARE KEYBOARD COVER THE FIELD BEING TYPED IN? Each step opens one at-risk field, types a
// few characters with the software keyboard up, and screenshots it (`T723-kb-<field>.png` beside the report). The check
// is read off the accessibility tree, not guessed: the field's box must end above the keyboard's top. Android resizes
// the window for the keyboard, so this scenario is iOS-only by nature.
//
//   node app/tools/testbed/run-ios.mjs keyboard       (a --testbed build; run through session-ios.sh)
//
// The simulator hides its software keyboard while "Connect Hardware Keyboard" is on: `softKeyboard()` turns that off for
// THIS device only (Simulator's per-device preference), so other lanes' simulators keep theirs.
import { execFileSync } from 'node:child_process';
import path from 'node:path';
import { runYaml } from '../lib/ios-flow.mjs';

const APP = 'ca.canifishthis.app';

/** The software keyboard on for this simulator only (Simulator.app's DevicePreferences, keyed by udid). */
export function softKeyboard(device, on = true) {
  const id = device.udid();
  try {
    execFileSync('defaults', ['write', 'com.apple.iphonesimulator', 'DevicePreferences', '-dict-add', id,
      `<dict><key>ConnectHardwareKeyboard</key><${on ? 'false' : 'true'}/></dict>`], { encoding: 'utf8' });
  } catch (e) { return `defaults failed: ${e.message.split('\n')[0]}`; }
  return `ConnectHardwareKeyboard=${!on} for ${id}`;
}

/** The keyboard's top edge (iOS points) from the tree, or null when no keyboard node is listed. */
function keyboardTop(nodes) {
  const ks = nodes.filter((n) => /Keyboard/i.test(n.cls) && n.box.y1 > n.box.y0 + 100);
  return ks.length ? Math.min(...ks.map((k) => k.box.y0)) : null;
}

/** Tap `target` (a Maestro text regex or `id:<testID>`), type `text`, report where the field ends against the keyboard. */
async function typeIn(ctx, name, target, text, { scroll = null } = {}) {
  const sel = target.startsWith('id:') ? `{ id: "${target.slice(3)}" }` : `{ text: "${target}" }`;
  const yaml = `appId: ${APP}\n---\n`
    + (scroll ? `- scrollUntilVisible:\n    element: { text: "${scroll}" }\n    timeout: 15000\n` : '')
    + `- tapOn: ${sel}\n- inputText: "${text}"\n`;
  const r = runYaml(ctx.dir, `kb-${name}`, yaml);
  if (!r.ok) throw new Error(`could not type in ${name}: ${r.said}`);
  await ctx.device.sleep(1200);
  ctx.device.screenshot(path.join(ctx.dir, `T723-kb-${name}.png`));
  return report(ctx, name, text);
}

/** Where the typed field ends against the keyboard's top, from the tree (a COVERED field is a finding). */
function report(ctx, name, text) {
  const ns = ctx.device.nodes();
  const top = keyboardTop(ns);
  const field = ns.find((n) => n.text === text || (n.text || '').endsWith(text));
  if (top === null) return `${name}: no keyboard node in the tree (look at T723-kb-${name}.png)`;
  if (!field) return `${name}: keyboard top ${top}; typed field not found in the tree (look at the shot)`;
  const covered = field.box.y1 > top;
  const note = `${name}: field ${Math.round(field.box.y0)}-${Math.round(field.box.y1)}, keyboard top ${Math.round(top)} → ${covered ? 'COVERED' : 'clear'}`;
  if (covered) ctx.finding(note);
  return note;
}

export const keyboardIos = {
  id: 'keyboard',
  inAll: false,
  title: 'T723 F2: the software keyboard against each at-risk field (iOS)',
  async run(ctx) {
    const { appUp, toMap } = ctx.ios;
    const open = (name, steps) => ctx.step(name, async () => {
      await toMap(ctx);
      return steps();
    });
    await ctx.step('software keyboard on for this simulator', async () => {
      const said = softKeyboard(ctx.device, true);
      // a first open downloads the whole staging release (~7 min): the app is NOT restarted meanwhile (a stop restarts the download)
      await appUp(ctx, { launch: !ctx.device.pidOf(), ms: 1_200_000 });
      return said;
    });
    await open('report a problem: the what-happened field', async () => {
      ctx.flow('report-open');
      return typeIn(ctx, 'report', 'id:report-what', 'keyboard probe');
    });
    await open('glossary search', async () => {
      ctx.flow('tab', { TAB: 'More' });
      ctx.flow('scroll-to', { TEXT: 'Glossary' });
      ctx.flow('tap-text', { TEXT: 'Glossary' });
      await ctx.device.sleep(1000);
      return typeIn(ctx, 'glossary', 'Search.*', 'bait');
    });
    await open('notifications search', async () => {
      ctx.flow('tab', { TAB: 'More' });
      ctx.flow('scroll-to', { TEXT: 'Notifications' });
      ctx.flow('tap-text', { TEXT: 'Notifications' });
      await ctx.device.sleep(1500);
      // (G10) autocorrect: "Coquihalla " typed, then read back; a rewritten word is a finding
      const said = await typeIn(ctx, 'notifications', '(Search|Find).*', 'Coquihalla ');
      const back = ctx.device.nodes().map((n) => n.text).find((t) => /^coq/i.test(t || ''));
      if (back !== undefined && back.trim() !== 'Coquihalla') ctx.finding(`G10: the gauge search was autocorrected: typed "Coquihalla " read back "${back}"`);
      return `${said}; read back "${back ?? 'not found'}"`;
    });
    await open('saved spot: title', async () => {
      ctx.device.geo(-121.44, 49.38);
      ctx.flow('tab', { TAB: 'Spots' });
      ctx.flow('tap-text', { TEXT: 'At my location' });
      await ctx.device.sleep(4000);
      const title = await typeIn(ctx, 'spot-title', '(Name this spot|Add a title)', 'kb');
      const name = `kb probe ${Date.now().toString(36).slice(-4)}`;
      ctx.flow('spot-name-save', { TITLE: name });
      await ctx.device.sleep(3000);
      return title;
    });
    await open('saved spot: notes (Spots → the spot → Edit → Notes)', async () => {
      ctx.flow('tab', { TAB: 'Spots' });
      ctx.flow('spot-notes', { TITLE: 'kb probe.*', NOTES: 'kb notes', SHOT: path.join(ctx.dir, 'T723-kb-spot-notes-flow') });
      await ctx.device.sleep(1200);
      ctx.device.screenshot(path.join(ctx.dir, 'T723-kb-spot-notes.png'));
      const note = report(ctx, 'spot-notes', 'kb notes');
      // (G10) ONE tap on "Save changes" with the keyboard up: it must save, not only close the keyboard
      runYaml(ctx.dir, 'kb-save-once', `appId: ${APP}\n---\n- tapOn: { text: "Save changes", index: 0 }\n`);
      await ctx.device.sleep(2000);
      const still = ctx.device.screenText().includes('Save changes');
      ctx.device.screenshot(path.join(ctx.dir, 'T723-kb-spot-save-once.png'));
      if (still) ctx.finding('G10: the first tap on Save changes (keyboard up) did not save: the form is still open');
      return `${note}; one tap on Save changes: ${still ? 'EATEN (form still open)' : 'saved'}`;
    });
    await open('alert fields: value and percent (a water\'s Conditions → Add an alert)', async () => {
      ctx.flow('search', { Q: 'Fraser River' });
      ctx.flow('open-result', { LINE: '.*Fraser River.*' });
      await ctx.device.sleep(2000);
      const yaml = `appId: ${APP}\n---\n- tapOn: { text: "Conditions", index: 0 }\n- scrollUntilVisible:\n    element: { id: "alert-me-open" }\n    timeout: 30000\n- tapOn: { id: "alert-me-open" }\n- scrollUntilVisible:\n    element: { id: "alert-form-value" }\n    timeout: 15000\n`;
      const r = runYaml(ctx.dir, 'kb-alert-open', yaml);
      if (!r.ok) throw new Error(`could not open the alert form: ${r.said}`);
      const v = await typeIn(ctx, 'alert-value', 'id:alert-form-value', '12');
      // (G10) the decimal pad has no return key on iOS: is there a Done / Return key above the pad's top?
      const ns = ctx.device.nodes(), top = keyboardTop(ns);
      const done = ns.some((n) => /^(Done|return|Return)$/.test(n.text || n.desc) && top !== null && n.box.y0 >= top - 60);
      if (!done) ctx.finding('G10: the number pad has no Done / Return key');
      return `${v}; Done key: ${done ? 'present' : 'ABSENT'}`;
    });
  },
};
