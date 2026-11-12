// (T603) node --test app/tools/testbed/lib/screen.test.mjs
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { all, bcDay, blankShare, BLANK_AT, chipProblem, find, idOf, inside, labelProblem, parse, shown, squeezed, within } from './screen.mjs';

const node = (id, cls, text, desc, b) => `<node index="0" text="${text}" resource-id="${id}" class="android.widget.${cls}" content-desc="${desc}" bounds="[${b[0]},${b[1]}][${b[2]},${b[3]}]" />`;
const XML = `<hierarchy>${[
  node('alert-form-p10', 'RadioButton', '', '9 in 10 days higher', [110, 1579, 479, 1669]),
  node('', 'TextView', '9 in 10 days higher', '', [149, 1605, 439, 1642]),
  node('alert-form-p25', 'RadioButton', '', '8 in 10 days higher', [500, 1579, 869, 1669]),
  node('', 'TextView', '8 in 10 days higher', '', [539, 1605, 900, 1642]),               // runs past its chip
  node('alert-me-open', 'Button', '', 'Alert me', [76, 699, 343, 294]),                 // scrolled under the header
  node('visit-spot_1', 'RadioButton', '', 'Visit on 10 Nov, not rated', [47, 825, 406, 914]),
  node('', 'TextView', '10 Nov · Not rated', '', [86, 851, 366, 888]),
  node('visit-add', 'Button', '', 'Add a visit', [47, 936, 350, 1073]),
  node('spot-when', 'TextView', 'A &amp; B &quot;c&quot;', '', [47, 420, 1033, 468]),
].join('')}</hierarchy>`;

test('parse: ids, class tail, words unescaped, boxes', () => {
  const ns = parse(XML);
  assert.equal(ns.length, 9);
  assert.deepEqual(ns[0], { id: 'alert-form-p10', cls: 'RadioButton', text: '', desc: '9 in 10 days higher', box: { x0: 110, y0: 1579, x1: 479, y1: 1669 } });
  assert.equal(find(ns, 'resource-id="spot-when"').text, 'A & B "c"');
});

test('a selector is uiautomator\'s own attribute, whole; anything else is refused', () => {
  assert.equal(idOf('resource-id="alert-form-line"'), 'alert-form-line');
  assert.throws(() => idOf('alert-form-line'), /resource-id="<testID>"/);
  const ns = parse(XML);
  assert.equal(all(ns, 'resource-id="visit-"', { prefix: true }).length, 2);
  assert.equal(all(ns, 'resource-id="visit-"').length, 0);
});

test('shown: a node scrolled under the header (bottom above top) is not on screen', () => {
  const ns = parse(XML);
  assert.equal(shown(find(ns, 'resource-id="alert-me-open"')), false);
  assert.equal(shown(find(ns, 'resource-id="visit-add"')), true);
  assert.equal(shown(null), false);
});

test('within / inside: a chip\'s label', () => {
  const ns = parse(XML);
  const chip = find(ns, 'resource-id="visit-spot_1"');
  assert.deepEqual(within(ns, chip).map((n) => n.text), ['10 Nov · Not rated']);
  assert.equal(inside(ns[1], ns[0]), true);
  assert.equal(inside(ns[3], ns[2]), false);
});

test('chipProblem: whole chip passes; a label past the chip, a missing chip and one off the screen are named', () => {
  const ns = parse(XML);
  assert.equal(chipProblem(ns, find(ns, 'resource-id="alert-form-p10"'), { screenW: 1080 }), null);
  assert.match(chipProblem(ns, find(ns, 'resource-id="alert-form-p25"'), { screenW: 1080 }), /runs past the chip/);
  assert.match(chipProblem(ns, find(ns, 'resource-id="alert-me-open"'), { screenW: 1080 }), /not on screen/);
  assert.match(chipProblem(ns, null, { what: 'preset 3' }), /^preset 3: not on screen/);
  assert.match(chipProblem(ns, find(ns, 'resource-id="alert-form-p10"'), { screenW: 400 }), /off the screen's side/);
  // a chip with no words in it
  const bare = parse(`<hierarchy>${node('visit-add', 'Button', '', '', [47, 936, 350, 1073])}</hierarchy>`);
  assert.match(chipProblem(bare, bare[0], {}), /no label inside it/);
});

test('labelProblem / squeezed: empty, off-screen and squeezed one-line labels are cut off; a wrapped paragraph is not', () => {
  const t = (text, b) => parse(`<hierarchy>${node('x', 'TextView', text, '', b)}</hierarchy>`)[0];
  assert.equal(labelProblem(t('Regulations day: Thu 12 Nov, marked on the chart.', [42, 784, 1038, 831]), { screenW: 1080 }), null);
  assert.match(labelProblem(t('', [42, 784, 1038, 831]), {}), /no words/);
  assert.match(labelProblem(t('Lower than 8 days in 10', [1000, 784, 1200, 831]), { screenW: 1080 }), /off the screen's side/);
  assert.match(labelProblem(t('Lower than 8 days in 10 for the date', [42, 784, 242, 831]), { screenW: 1080 }), /cut off/);
  assert.equal(squeezed(t('Lower than 8 days in 10 for the date', [42, 784, 242, 831])), true);
  // the same words wrapped over several lines in a narrow column are whole
  assert.equal(squeezed(t('Lower than 8 days in 10 for the date', [42, 784, 242, 960])), false);
  assert.match(labelProblem(null, { what: 'the trend line' }), /^the trend line: not on screen/);
});

test('blankShare: a flat box is blank, a drawn one is not (mutation: the threshold bites both ways)', () => {
  const img = (fn) => { const w = 80, h = 80, ch = 4, px = Buffer.alloc(w * h * ch); for (let y = 0; y < h; y++) for (let x = 0; x < w; x++) { const [r, g, b] = fn(x, y); px.set([r, g, b, 255], (y * w + x) * ch); } return { w, h, ch, px }; };
  const box = { x0: 0, y0: 0, x1: 80, y1: 80 };
  const flat = blankShare(img(() => [20, 60, 40]), box);
  assert.ok(flat.share >= BLANK_AT, `flat ${flat.share}`);
  // a flat fill with a little sensor noise is still blank
  assert.ok(blankShare(img((x, y) => [20 + ((x + y) % 3), 60, 40]), box).share >= BLANK_AT);
  // water, land and a line: not blank
  const drawn = blankShare(img((x, y) => (x < 30 ? [128, 216, 228] : y % 16 < 2 ? [255, 255, 255] : [222, 220, 215])), box);
  assert.ok(drawn.share < 0.8, `drawn ${drawn.share}`);
  // 3 % of other pixels (a label on a flat map) is not "drawn" by much, but it is under the line: the line is 98.5 %
  assert.ok(blankShare(img((x, y) => (x < 4 && y < 48 ? [255, 255, 255] : [20, 60, 40])), box).share < BLANK_AT);
});

test('bcDay: B.C.\'s day at UTC−7 all year, named as the app names it', () => {
  // 2026-11-12 05:30 UTC is still Wed 11 Nov in B.C. (22:30)
  const now = Date.UTC(2026, 10, 12, 5, 30);
  assert.equal(bcDay(0, now), 'Wed 11 Nov');
  assert.equal(bcDay(1, now), 'Thu 12 Nov');
  assert.equal(bcDay(11, now), 'Sun 22 Nov');
  // 07:00 UTC is midnight: the day turns
  assert.equal(bcDay(0, Date.UTC(2026, 10, 12, 7, 0)), 'Thu 12 Nov');
  assert.equal(bcDay(0, Date.UTC(2026, 11, 31, 23, 0)), 'Thu 31 Dec');
});

test('ends: an id the app builds from a variable is named by its fixed part and its tail', () => {
  const ns = parse(XML);
  assert.equal(find(ns, 'resource-id="alert-form-"', { ends: 'p25' }).desc, '8 in 10 days higher');
  assert.equal(find(ns, 'resource-id="alert-form-"', { ends: 'p99' }), null);
  assert.equal(all(ns, 'resource-id="visit"', { ends: '-add' }).length, 1);
});
