// T736 — the staging smoke's rule-book step, apart from smoke-staging.mjs so it can be tested without a browser.
//
// On a slow line (the app measured under 1 Mbit/s over its own downloads: "Slow connection — showing what we have") the
// app does not fetch the 11.7 MB rule book on a tap: it ASKS first ("The rule book is 11.7 MB. Download it / Not now",
// testid synopsis-offer). The step used to wait for the PDF to open and never answer, so on a loaded machine (headless
// Chrome's downloads read under the threshold) it failed as a broken rule book. A user would press "Download it", so the
// step does, and SAYS so in its line ("asked first (slow-connection mode)"): the slow path is named, never a silent pass.
export const OFFER_Q = `!!document.querySelector('[data-testid="synopsis-offer"]')`;
export const PDF_Q = `typeof window.__pdf === 'string' && window.__pdf.startsWith('blob:') && window.__pdf.includes('#page=')`;
export const ASKED_FIRST = 'asked first (slow-connection mode)';

/**
 * Wait for the rule book PDF to open after the "Rule book page" tap, answering the size prompt the way a user would.
 * `b.evaluate(expr)` is the page; `press(b, label)` taps a labelled control (smoke-staging's own); `sleep(ms)` waits.
 * Returns '' when the PDF opened with no prompt, ASKED_FIRST when the prompt was met and answered. Throws, saying which
 * of three things happened, when it did not open in `ms`: the prompt could not be pressed, the prompt was pressed but
 * nothing came, or no prompt and no PDF.
 */
export async function waitForRuleBook(b, { press, sleep, ms = 60000, every = 200 }) {
  let asked = false, pressed = false;
  for (let t = 0; t < ms; t += every) {
    if (await b.evaluate(PDF_Q).catch(() => false)) return asked ? ASKED_FIRST : '';
    if (!pressed && await b.evaluate(OFFER_Q).catch(() => false)) {
      asked = true;
      pressed = await press(b, 'Download it');
    }
    await sleep(every);
  }
  if (asked) throw new Error(`the app chose its slow-connection path and asked before downloading the rule book; ${pressed ? '"Download it" was pressed but' : '"Download it" could not be pressed and'} the PDF did not open (${ms / 1000} s)`);
  throw new Error(`timed out (${ms / 1000} s) waiting for the PDF to open (a blob URL at #page=N); no slow-connection prompt was showing`);
}
