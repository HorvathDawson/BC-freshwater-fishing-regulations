// (T760) PURE READERS for the bench: the nodes of a `uiautomator dump` (text + where to tap) and the system downloader's
// log lines (which jobs started and finished since a given moment). No device here: device.mjs and the scenarios call these.

const unxml = (s) => s.replace(/&amp;/g, '&').replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>')
  .replace(/&#10;/g, '\n');

/** Every node of a uiautomator dump that has a text or content-description and bounds: `{ text, x1, y1, x2, y2, cx, cy }`
 *  (cx, cy = the middle of the bounds, where a tap goes). */
export function parseNodes(xml) {
  const out = [];
  for (const m of xml.matchAll(/<node\b[^>]*>/g)) {
    const tag = m[0];
    const attr = (k) => new RegExp(`\\s${k}="([^"]*)"`).exec(tag)?.[1] ?? '';
    const text = unxml(attr('text') || attr('content-desc'));
    const b = /^\[(\d+),(\d+)\]\[(\d+),(\d+)\]$/.exec(attr('bounds'));
    if (!text || !b) continue;
    const [x1, y1, x2, y2] = b.slice(1).map(Number);
    out.push({ text, x1, y1, x2, y2, cx: Math.round((x1 + x2) / 2), cy: Math.round((y1 + y2) / 2) });
  }
  return out;
}

/** The first node whose text matches `re`, or null. */
export const findNode = (nodes, re) => nodes.find((n) => re.test(n.text)) ?? null;

/**
 * The system downloader's jobs since `since` ("MM-DD HH:MM:SS", the device's clock, or '' for all): from its log lines
 * (`11-16 11:22:01.512  1673  7298 D DownloadManager: [587] Starting` / `[587] Finished with status SUCCESS`).
 * `started` / `finished` are job ids, `startedAt` / `finishedAt` the "HH:MM:SS" of the first start and the last finish.
 * `running` = started and not finished: when it is empty and something started, no notification can exist any more.
 */
export function downloadJobs(lines, since = '') {
  const started = [], finished = [];
  let startedAt = '', finishedAt = '';
  for (const l of lines) {
    const m = /^(\d\d-\d\d \d\d:\d\d:\d\d)\.\d+ .*DownloadManager: \[(\d+)\] (Starting|Finished)/.exec(l);
    if (!m || m[1] < since) continue;
    const t = m[1].slice(6);
    if (m[3] === 'Starting') { if (!started.includes(m[2])) started.push(m[2]); startedAt ||= t; }
    else { if (!finished.includes(m[2])) finished.push(m[2]); finishedAt = t; }
  }
  return { started, finished, running: started.filter((i) => !finished.includes(i)), startedAt, finishedAt };
}
