async (page) => {
  const PHASE = 'after';   // 'before' for the shots taken on the code before T44
  const OUT = '/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/.claude/worktrees/wf_866f7c43-707-1/handoff/PLAN/app-integration/T44-shots/' + PHASE + '/';
  const measure = () => page.evaluate(() => {
    const vis = (e) => { const r = e.getBoundingClientRect(); return r.width > 0 && r.height > 0 && r.right > 0 && r.left < innerWidth; };
    const R = (e) => { if (!e) return null; const r = e.getBoundingClientRect(); return { x: +r.left.toFixed(2), y: +r.top.toFixed(2), w: +r.width.toFixed(2), h: +r.height.toFixed(2) }; };
    const back = [...document.querySelectorAll('[aria-label="Back"]')].find(vis);
    const strip = [...document.querySelectorAll('[role=tablist]')].find((e) => vis(e) && /Regulations/.test(e.textContent) && /Conditions/.test(e.textContent));
    const sel = strip && [...strip.querySelectorAll('[role=tab]')].find((t) => t.getAttribute('aria-selected') === 'true');
    const sb = strip ? strip.getBoundingClientRect().bottom : 0;
    const leaves = [...document.querySelectorAll('div,span,h1,h2')].filter((e) => vis(e) && e.children.length === 0 && e.textContent.trim() === 'Vedder River' && e.getBoundingClientRect().top >= sb - 1 && e.getAttribute('aria-hidden') !== 'true');
    const title = leaves[0];
    const tb = title ? title.getBoundingClientRect().bottom : sb;
    let left = Infinity; const w = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    for (let n; (n = w.nextNode());) {
      if (!n.textContent.trim()) continue;
      const el = n.parentElement; if (!el || !vis(el) || el.closest('[aria-hidden=true]')) continue;
      const rg = document.createRange(); rg.selectNodeContents(n);
      for (const r of rg.getClientRects()) if (r.width > 0 && r.top > tb + 2 && r.bottom < innerHeight - 90 && r.left >= 0) left = Math.min(left, r.left);
    }
    return { view: sel?.getAttribute('aria-label'), back: R(back), strip: R(strip), title: R(title),
             titleFont: title ? getComputedStyle(title).fontSize + ' ' + getComputedStyle(title).fontFamily.split(',')[0] : null,
             contentLeft: left === Infinity ? null : +left.toFixed(2) };
  });
  const sizes = [[390, 844], [768, 1024], [1440, 900]];
  const res = {};
  for (const [w, h] of sizes) {
    await page.setViewportSize({ width: w, height: h }); await page.waitForTimeout(1500);
    for (const v of ['Regulations', 'Conditions', 'Map']) {
      const tab = page.getByRole('tab', { name: v, exact: true }).first();
      if (await tab.getAttribute('aria-selected') !== 'true') await tab.click();
      await page.waitForTimeout(v === 'Map' ? 5000 : 3500);
      (res[`${w}x${h}`] ||= []).push(await measure());
      await page.screenshot({ path: `${OUT}${w}x${h}-${v.toLowerCase()}.png` });
    }
    const tab = page.getByRole('tab', { name: 'Regulations', exact: true }).first();
    await tab.click(); await page.waitForTimeout(3000);
  }
  return JSON.stringify(res);
}
