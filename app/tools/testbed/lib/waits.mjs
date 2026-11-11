// (T542) WAITS THAT END ON A CONDITION. A fixed sleep after a tap is either too long (every run pays it) or too short (the
// step flakes); the scenarios wait for what they need instead, with a time limit that names what never came:
//
//   until(cond, { ms, what, soft })  poll `cond()` (sync or async) every 500 ms until truthy; returns its value. At the limit:
//                                    throws "no <what> in N s", or with `soft` returns null so the assertion that FOLLOWS
//                                    reports the real state (never a silent pass: a soft wait is always followed by a check).
//   settle({ ms, frame })            the screen has stopped changing (two equal looks in a row: its words, or with `frame` its
//                                    pixels — a map at rest). Never later than `ms`, never throws: a screen that never rests
//                                    (a spinner) is what the next step then reads.
//   timer(ms, why)                   a fixed wait that is the point of the step (the 60 s foreground timer, a crash that
//                                    arrives a beat late). `why` is required: a bare sleep is refused by the static check
//                                    (app/tools/testbed-static.test.ts).
//   hasId(id)                        a testID is on screen (uiautomator).
export function makeWaits(device) {
  const until = async (cond, { ms = 15_000, every = 500, what = 'the condition', soft = false } = {}) => {
    const end = Date.now() + ms;
    for (;;) {
      const v = await cond();
      if (v) return v;
      if (Date.now() + every > end) break;
      await device.sleep(every);   // poll interval
    }
    if (soft) return null;
    throw new Error(`no ${what} in ${ms / 1000} s`);
  };
  const look = (frame) => (frame ? device.adb(['exec-out', 'screencap', '-p']).toString('base64') : device.screenText().join('\u0001'));
  const settle = async ({ ms = 3000, every = 700, frame = false } = {}) => {
    const end = Date.now() + ms;
    let prev = look(frame);
    while (Date.now() + every <= end) {
      await device.sleep(every);   // poll interval
      const cur = look(frame);
      if (cur === prev) return true;
      prev = cur;
    }
    return false;
  };
  const timer = async (ms, why) => {
    if (!String(why ?? '').trim()) throw new Error('timer(ms, why): a fixed wait says why it is fixed');
    await device.sleep(ms);   // timer: documented by `why`
  };
  const hasId = (id) => {
    device.sh('uiautomator dump /sdcard/tb-id.xml >/dev/null 2>&1');
    return device.sh('cat /sdcard/tb-id.xml').includes(`resource-id="${id}"`);
  };
  return { until, settle, timer, hasId };
}
