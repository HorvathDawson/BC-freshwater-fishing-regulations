// (T603) SAVED SPOTS, THROUGH THE APP'S OWN UI: a spot saved online on a gauged river (the Fraser at Hope, "At my
// location", the day before: a day whose record is whole), then everything a saved spot shows is read back — the Spots
// row, the spot page's visit chip and rating, the week-before strip (weather columns, the gauge's daily means with their
// short "N in 10 days higher" place, the trend line) — and the alert editor on a gauge card (its chips, the level-for-
// the-day presets, the one line saying what will happen), one alert saved, read in More → Notifications and removed.
//
// NO SEEDING HELPER: the capture itself reads the week before (weather and the gauge's daily means), so the UI path
// fills everything the checks need (seen 2026-11-11). If that ever stops, seed through one helper that writes the
// store's own records; never a second screen.
//
// FAILS when a chip, the alert editor's wording or a lead-up row is missing or cut off: every label is checked for
// being drawn, on screen, inside its chip, and not squeezed (lib/screen.mjs `labelProblem` / `chipProblem`). A label
// the phone ellipsizes inside a box that is wide enough for it cannot be seen from here (uiautomator reports the whole
// string): the shots are the check for that.
//
//   node app/tools/testbed/run.mjs saved-spots        SAVED_AT="lon,lat"  ALERT_Q / ALERT_ROW: the gauged water
import { capture } from './spots.mjs';
import { coldStart, openWater, toMap, closeWaters } from './common.mjs';
import { all, chipProblem, find, labelProblem, look, screenSize, scrollTo, shown, tap, within } from '../lib/screen.mjs';

/** the Fraser at Hope, mid-river by the bridge (gauge 08MF005 just upstream) */
const AT = process.env.SAVED_AT ? (([lon, lat]) => ({ lon, lat }))(process.env.SAVED_AT.split(',').map(Number)) : { lon: -121.4517, lat: 49.3862 };
const ALERT_Q = process.env.ALERT_Q ?? 'Coquitlam River';
const ALERT_ROW = process.env.ALERT_ROW ?? '.* · Region 2 · MU 2-8.*';
/** the spot page: under its Back / Edit / Delete row, above the tab bar */
const SPOT_BAND = { top: 300, bottom: 2170 };
/** a water's pane: under its header and notices, above the screen's foot */
const PANE_BAND = { top: 720, bottom: 2300 };
/** T598: no reader-facing "percentile" */
const noPercentile = (words, where) => {
  const bad = words.find((w) => /percentile/i.test(w));
  if (bad) throw new Error(`${where} says "percentile" (T598: days in 10): "${bad}"`);
};
const fail = (problems) => { const p = problems.filter(Boolean); if (p.length) throw new Error(p.join('; ')); };

export const savedSpots = {
  id: 'saved-spots',
  title: 'Saved spots (T603): saved through the UI on the Fraser at Hope; row, visit chip, week-before strip; the alert editor and one alert line',
  async run(ctx) {
    const d = ctx.device;
    const spot = { title: `Testbed hope ${Date.now().toString(36).slice(-5)}` };
    const { W } = screenSize(d);
    let saved = false;

    await ctx.step('setup: Wi-Fi, GPS on the Fraser at Hope, a cold start', async () => {
      await d.net('wifi'); d.link('full', 'none');
      d.geo(AT.lon, AT.lat);
      await coldStart(ctx);
      await closeWaters(ctx);
      await toMap(ctx);
    }, { shot: false, stop: true });

    await ctx.step('Spots → At my location → the water → Yesterday → the draft reads the river', async () => {
      ctx.flow('tab', { TAB: 'Spots' });
      ctx.flow('tap-text', { TEXT: 'At my location' });
      ctx.captureDay = 'Yesterday';
      const words = await capture(ctx);
      if (!words.some((w) => /^on Fraser River$/.test(w))) throw new Error(`the draft is not on the Fraser River (saw: ${words.slice(0, 8).join(' | ').slice(0, 300)})`);
      return words.find((w) => /days in 10|normal/i.test(w)) ?? 'draft up';
    }, { stop: true });

    await ctx.step(`named "${spot.title}" and saved: its page opens`, async () => {
      ctx.flow('spot-name-save', { TITLE: spot.title });
      await ctx.until(() => ctx.hasId('spot-visits'), { ms: 20_000, what: 'the saved spot\'s page (spot-visits)' });
      saved = true;
      const ns = await look(d);
      const title = ns.find((n) => n.text === spot.title);
      fail([labelProblem(title, { screenW: W, what: 'the spot\'s title' }),
            labelProblem(find(ns, 'resource-id="spot-when"'), { screenW: W })]);
      return find(ns, 'resource-id="spot-when"').text;
    }, { stop: true });

    await ctx.step('the visit chip, Add a visit and the five rating stars', async () => {
      const ns = await look(d);
      const chip = all(ns, 'resource-id="visit-"', { prefix: true }).find((n) => /^Visit on /.test(n.desc));
      if (!chip) throw new Error('no visit chip on the spot page');
      const stars = all(ns, 'resource-id="visit-star-"', { prefix: true });
      fail([chipProblem(ns, chip, { screenW: W, what: 'the visit chip' }),
            chipProblem(ns, find(ns, 'resource-id="visit-add"'), { screenW: W }),
            stars.length === 5 ? null : `${stars.length} rating stars, not 5`,
            labelProblem(find(ns, 'resource-id="visit-rate-word"'), { screenW: W })]);
      if (!/not rated$/i.test(chip.desc)) throw new Error(`the new visit's chip says "${chip.desc}", not "…, not rated"`);
      return `${within(ns, chip).find((n) => n.text)?.text} (${chip.desc})`;
    });

    await ctx.step('the week before: a weather column a day', async () => {
      await scrollTo(d, 'resource-id="leadup-rows"', { ...SPOT_BAND, bottom: 1900 });
      const ns = await look(d);
      const note = find(ns, 'resource-id="leadup-weather-note"');
      if (note) throw new Error(`no week-before weather: "${note.text}"`);
      const days = ns.filter((n) => shown(n) && /^\w{3} \w{3} \d+: .*(high|rain|wind)/.test(n.desc));
      if (days.length < 3) throw new Error(`${days.length} weather day column(s) on screen under THE WEEK BEFORE (want the week's)`);
      fail(days.map((c) => (c.box.x1 > W ? `the ${c.desc.slice(0, 10)} column runs off the screen` : null)));
      const src = find(ns, 'resource-id="leadup-weather-source"');
      return `${days.length} day columns on screen · ${days.at(-1).desc}${src ? ` · ${src.text}` : ''}`;
    });

    await ctx.step('the week before: the gauge\'s daily means, each with its place in short words, and the trend', async () => {
      const meta = await scrollTo(d, 'resource-id="leadup-gauge-meta"', SPOT_BAND).catch(() => null);
      const ns = await look(d);
      const note = find(ns, 'resource-id="leadup-gauge-note"') ?? find(ns, 'resource-id="leadup-gauge-pending"');
      if (!meta) throw new Error(`no gauge rows under THE WEEK BEFORE${note ? `: "${note.text}"` : ''}`);
      const rows = ns.filter((n) => shown(n) && /^\w{3} \w{3} \d+: [\d.,]+ (m³\/s|m)\b/.test(n.desc));
      if (rows.length < 3) throw new Error(`${rows.length} gauge day row(s) on screen (want the week's)`);
      const problems = [];
      const places = [];
      for (const r of rows) {
        const cells = within(ns, r).filter((n) => n.text);
        if (cells.length < 3) { problems.push(`${r.desc.slice(0, 10)}: ${cells.length} of day / value / place drawn`); continue; }
        problems.push(...cells.map((c) => labelProblem(c, { screenW: W, what: `${r.desc.slice(0, 10)} "${c.text}"` })));
        places.push(cells.at(-1).text);
      }
      const trend = find(ns, 'resource-id="leadup-trend"');
      problems.push(labelProblem(trend, { screenW: W }), labelProblem(meta, { screenW: W }));
      fail(problems);
      noPercentile([...places, ...rows.map((r) => r.desc), trend.text], 'the week-before strip');
      const odd = places.find((p) => !/\d\+? in 10 days (higher|lower)|median|highest|lowest|every day/i.test(p));
      if (odd) throw new Error(`a day's place is not the short days-in-10 form: "${odd}"`);
      return `${rows.length} rows · ${places.at(-1)} · ${trend.text} · ${meta.text}`;
    });

    await ctx.step('the Spots list: the row, its lines and Show', async () => {
      d.sh('input keyevent 4');
      await ctx.until(() => ctx.hasId('spots-screen'), { ms: 10_000, what: 'the Spots list (spots-screen)' });
      const ns = await look(d);
      const title = ns.find((n) => n.text === spot.title);
      if (!title) throw new Error(`"${spot.title}" is not in the Spots list`);
      const lines = all(ns, 'resource-id="spot-row-"', { prefix: true }).find((n) => Math.abs(n.box.x0 - title.box.x0) < 4 && n.box.y0 >= title.box.y1 - 4 && n.box.y0 < title.box.y1 + 40);
      const show = ns.find((n) => n.desc === `Show ${spot.title} on the map`);
      fail([labelProblem(title, { screenW: W, what: 'the row\'s title' }), labelProblem(lines, { screenW: W, what: 'the row\'s lines' }),
            show && shown(show) ? null : 'no "Show" button on the row']);
      if (!/m³\/s/.test(lines.text)) throw new Error(`the row does not carry the day's flow: "${lines.text}"`);
      noPercentile([lines.text], 'the Spots row');
      return lines.text.replace(/\n/g, ' / ');
    });

    // ---- the alert editor, on a gauge's card ----
    let editor = false;
    await ctx.step(`${ALERT_Q} → Conditions → the gauge card's "Alert me" → the editor`, async () => {
      await openWater(ctx, { q: ALERT_Q, row: ALERT_ROW, view: 'conditions' });
      await ctx.until(() => ctx.hasId('conditions-day-label'), { ms: 30_000, what: 'the Conditions view (conditions-day-label)' });
      const open = await scrollTo(d, 'resource-id="alert-me-open"', { ...PANE_BAND, bottom: 1500 });
      tap(d, open);
      await ctx.until(() => ctx.hasId('alert-form'), { ms: 8000, what: 'the alert editor (alert-form)' });
      await scrollTo(d, 'resource-id="alert-form-line"', PANE_BAND);
      const ns = await look(d);
      const form = find(ns, 'resource-id="alert-form"');
      const head = within(ns, form).find((n) => /^Alert on /.test(n.text));
      fail([labelProblem(head, { screenW: W, what: 'the editor\'s title' }),
            chipProblem(ns, find(ns, 'resource-id="alert-form-above"'), { screenW: W }),
            chipProblem(ns, find(ns, 'resource-id="alert-form-below"'), { screenW: W }),
            chipProblem(ns, find(ns, 'resource-id="alert-form-line-value"'), { screenW: W }),
            chipProblem(ns, find(ns, 'resource-id="alert-form-line-percentile"'), { screenW: W }),
            labelProblem(find(ns, 'resource-id="alert-form-context"'), { screenW: W }),
            labelProblem(find(ns, 'resource-id="alert-form-line"'), { screenW: W })]);
      const line = find(ns, 'resource-id="alert-form-line"').text;
      if (!/^You get one notification each time the .* crosses above [\d.,]+ .*\.$/.test(line)) throw new Error(`the value line reads: "${line}"`);
      editor = true;
      return `${head.text} · ${find(ns, 'resource-id="alert-form-context"').text} · ${line}`;
    });

    await ctx.step('the level-for-the-day presets (five and Custom) and the line they make', async () => {
      if (!editor) ctx.skip('the editor was not reached');
      const pick = find(await look(d), 'resource-id="alert-form-line-percentile"');
      tap(d, pick);
      await ctx.until(() => ctx.hasId('alert-form-p-custom'), { ms: 8000, what: 'the presets (alert-form-p-custom)', soft: true });
      let ns = await look(d);
      const why = find(ns, 'resource-id="alert-form-no-percentile"');
      if (!find(ns, 'resource-id="alert-form-p-custom"')) throw new Error(`no presets${why ? `: "${why.text}"` : ''}`);
      await scrollTo(d, 'resource-id="alert-form-line"', PANE_BAND);
      ns = await look(d);
      // core PERCENTILE_PRESETS, then Custom (the id is built from the number: the fixed part is the selector)
      const chips = ['p10', 'p25', 'p50', 'p75', 'p90', 'p-custom'].map((tail) => find(ns, 'resource-id="alert-form-"', { ends: tail }));
      fail(chips.map((c, i) => chipProblem(ns, c, { screenW: W, what: `preset ${i + 1}` })));
      const labels = chips.map((c) => c.desc);
      noPercentile(labels, 'a preset');
      const odd = labels.slice(0, 5).find((l) => !/^(\d in 10 days (higher|lower)|Median)$/.test(l));
      if (odd) throw new Error(`a preset is not the short days-in-10 form: "${odd}" (all: ${labels.join(' | ')})`);
      const line = find(ns, 'resource-id="alert-form-line"');
      fail([labelProblem(line, { screenW: W })]);
      noPercentile([line.text], 'the alert line');
      if (!/^You get one notification each time the .* crosses above the level (higher|lower) than \d days in 10 for the day\. .*\.$/.test(line.text))
        throw new Error(`the level line reads: "${line.text}"`);
      return `${labels.join(' | ')} · ${line.text}`;
    });

    let alertSaved = false;
    await ctx.step('Save alert: the card counts it and says where to manage it', async () => {
      if (!editor) ctx.skip('the editor was not reached');
      const save = await scrollTo(d, 'resource-id="alert-form-save"', PANE_BAND);
      tap(d, save);
      // (Android 13+) the first alert asks for the notification permission: allowed, as a reader would
      await ctx.until(async () => {
        if (d.screenText().some((x) => /send you notifications/i.test(x))) ctx.flow('tap-text', { TEXT: 'Allow' });
        return !ctx.hasId('alert-form');
      }, { ms: 15_000, what: 'the editor closing after Save' });
      alertSaved = true;
      let ns = await look(d);
      const denied = find(ns, 'resource-id="alert-me-denied"') ?? find(ns, 'resource-id="alert-me-error"');
      if (denied) throw new Error(`the save said: "${denied.text}"`);
      const said = find(ns, 'resource-id="alert-me-saved"');
      if (!said) throw new Error('no "Saved. Manage alerts in More → Notifications." after Save');
      // where the reader is left: the confirmation should be on screen without a scroll
      const onScreen = shown(said) && said.box.y0 >= PANE_BAND.top - 40;
      if (!onScreen) ctx.finding('alert editor: after Save the page is left scrolled past the card, so "Saved. Manage alerts in More → Notifications." and the "1 alert on this gauge" count are off screen until the reader scrolls up');
      const count = await scrollTo(d, 'resource-id="alert-me-count"', PANE_BAND);
      ns = await look(d);
      fail([labelProblem(count, { screenW: W }), labelProblem(find(ns, 'resource-id="alert-me-saved"'), { screenW: W })]);
      if (!/^\d+ alerts? on this gauge$/.test(count.text)) throw new Error(`the count reads "${count.text}"`);
      return `${count.text} · ${find(ns, 'resource-id="alert-me-saved"').text}${onScreen ? '' : ' (off screen after Save: finding)'}`;
    });

    await ctx.step('More → Notifications: the alert\'s line, whole; then removed', async () => {
      if (!alertSaved) ctx.skip('no alert was saved');
      await closeWaters(ctx);
      await toMap(ctx);
      ctx.flow('tab', { TAB: 'More' });
      ctx.flow('scroll-to', { TEXT: 'Notifications' });
      ctx.flow('tap-text', { TEXT: 'Notifications' });
      await ctx.until(() => ctx.hasId('notifications-page'), { ms: 10_000, what: 'the Notifications page' });
      let ns = await look(d);
      const rows = all(ns, 'resource-id="notify-alert-on-"', { prefix: true });
      if (!rows.length) throw new Error('no alert listed under GAUGE ALERTS');
      const lines = rows.map((r) => within(ns, r).find((n) => n.text && n.text.length > 8));
      fail(lines.map((l) => labelProblem(l, { screenW: W, what: 'the alert\'s line' })));
      noPercentile(lines.map((l) => l.text), 'the alert\'s line');
      const mine = lines.find((l) => /crosses above the level (higher|lower) than \d days in 10/.test(l.text));
      if (!mine) throw new Error(`no alert line in the days-in-10 form (saw: ${lines.map((l) => l.text).join(' / ')})`);
      const said = mine.text;
      // remove every alert this scenario (or a run cut short) left: the list ends empty
      for (let i = 0; i < 6; i++) {
        const rm = find(await look(d), 'resource-id="notify-remove-"', { prefix: true });
        if (!rm) break;
        tap(d, rm);
        await ctx.settle({ ms: 2500 });
      }
      ns = await look(d);
      const left = all(ns, 'resource-id="notify-alert-on-"', { prefix: true }).length;
      if (left) throw new Error(`${left} alert(s) still listed after Delete`);
      d.sh('input keyevent 4');
      return said;
    });

    await ctx.step('clean up: the spot deleted', async () => {
      if (!saved) ctx.skip('no spot was saved');
      await closeWaters(ctx);
      await toMap(ctx);
      ctx.flow('tab', { TAB: 'Spots' });
      await ctx.waitText(new RegExp(spot.title), 15_000);
      ctx.flow('tap-text', { TEXT: spot.title });
      await ctx.until(() => ctx.hasId('spot-visits'), { ms: 10_000, what: 'the spot\'s page' });
      ctx.flow('spot-delete');
      await ctx.until(() => ctx.hasId('spots-screen') && !d.screenText().includes(spot.title), { ms: 10_000, what: 'the spot gone from the list' });
      await toMap(ctx);
    });
  },
};
