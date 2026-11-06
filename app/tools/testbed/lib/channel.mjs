// (T442) IS THE TEST CHANNEL THE LIVE RELEASE? The test-bed APK reads staging's `test/release/manifest.json`, but every
// file it names is fetched from the LIVE `release/` prefix. A staging upload clears every release but its own, so a
// test/ manifest left naming an older release sends a fresh install to 404s ("The map data could not be loaded").
// `run.mjs` resets test/ before `install`; with `--no-r2` it cannot, so it checks (read only) with this instead.
const ORIGIN = 'https://data-staging.canifishthis.ca';

/** null when test/ holds the live release; else why a first install from it would fail. */
export function channelProblem(test, live) {
  if (!live?.data_release) return 'the live manifest (release/manifest.json) could not be read';
  if (!test?.data_release) return 'test/release/manifest.json could not be read';
  if (test.data_release !== live.data_release)
    return `test/ names ${test.data_release}, the live release is ${live.data_release}: ${test.data_release}'s files are ` +
      'cleared by the staging upload, so a fresh install 404s. Run without --no-r2 (the install step resets test/) ' +
      'or `node deploy/staging/test-updates.mjs reset`';
  return null;
}

/** Read both manifests from staging (no writes) and say whether test/ is the live release. */
export async function checkChannel(fetchFn = fetch) {
  const get = async (key) => {
    try { const r = await fetchFn(`${ORIGIN}/${key}`, { cache: 'no-store' }); return r.ok ? await r.json() : null; }
    catch { return null; }
  };
  const [test, live] = await Promise.all([get('test/release/manifest.json'), get('release/manifest.json')]);
  const problem = channelProblem(test, live);
  if (problem) throw new Error(problem);
  return { release: { data_release: test.data_release }, checked: 'read only (--no-r2)' };
}
