async (page) => {
  // T44 swipe recording: a mouse drag right-to-left across the view's body (Regulations → Conditions), a frame every
  // step; each frame records where the back, the strip and the name row are. Run on the water screen at Regulations.
  const OUT = '/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/.claude/worktrees/wf_866f7c43-707-1/handoff/PLAN/app-integration/T44-shots/after/swipe/';
  const at = () => page.evaluate(() => {
    const vis = (e) => { const r = e.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
    const R = (e) => { if (!e) return null; const r = e.getBoundingClientRect(); return [+r.left.toFixed(1), +r.top.toFixed(1)]; };
    const strip = [...document.querySelectorAll('[role=tablist][aria-label="Views of this water"]')].find(vis);
    const sel = strip && [...strip.querySelectorAll('[role=tab]')].find((t) => t.getAttribute('aria-selected') === 'true');
    return { view: sel?.getAttribute('aria-label'), back: R([...document.querySelectorAll('[aria-label="Back"]')].find(vis)),
             strip: R(strip), title: R([...document.querySelectorAll('[data-testid=water-title]')].find(vis)) };
  });
  const res = [];
  for (const [w, h] of [[390, 844], [1440, 900]]) {
    await page.setViewportSize({ width: w, height: h }); await page.waitForTimeout(1500);
    const tab = page.getByRole('tab', { name: 'Regulations', exact: true }).first();
    if (await tab.getAttribute('aria-selected') !== 'true') { await tab.click(); await page.waitForTimeout(3000); }
    const y = 420, x0 = Math.round(w * 0.7);
    let i = 0;
    const shot = async (label) => { res.push({ size: `${w}x${h}`, frame: i, label, ...(await at()) });
      await page.screenshot({ path: `${OUT}${w}x${h}-${String(i).padStart(2, '0')}-${label}.png` }); i++; };
    await shot('start');
    await page.mouse.move(x0, y); await page.mouse.down();
    for (const dx of [-20, -60, -100, -140, -180]) { await page.mouse.move(x0 + dx, y, { steps: 4 }); await page.waitForTimeout(60); await shot(`drag${-dx}`); }
    await page.mouse.up(); await page.waitForTimeout(250); await shot('released');
    await page.waitForTimeout(3000); await shot('settled');
  }
  return JSON.stringify(res);
}
