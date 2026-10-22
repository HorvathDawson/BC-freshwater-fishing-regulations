// T45 web shots (Playwright, `page` already on a water's Regulations view at the day wanted). Set STATE before running.
// For each size: the top (Keep), each section via its tab (side list on a desktop, the tab row on a tablet), then one
// sheet (the first "How to tell ›" / ⓘ on the page), closed again with Escape.
async (page) => {
  const STATE = 'klwa_1010';   // set per water before running
  const OUT = '/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations/.claude/worktrees/wf_d29a2e68-854-1/handoff/PLAN/app-integration/T45-shots/';
  const res = {};
  for (const [w, h] of [[1440, 900], [1024, 768]]) {
    await page.setViewportSize({ width: w, height: h }); await page.waitForTimeout(1500);
    for (const [i, t] of ['What you can keep', 'Gear', 'Methods', 'Licence'].entries()) {
      await page.getByRole('tablist', { name: 'Section' }).getByRole('tab', { name: t, exact: true }).click();
      await page.waitForTimeout(1200);
      await page.screenshot({ path: `${OUT}${STATE}-${w}x${h}-${i + 1}-${t.split(' ')[0].toLowerCase()}.png` });
    }
    await page.getByRole('tablist', { name: 'Section' }).getByRole('tab', { name: 'What you can keep', exact: true }).click();
    await page.waitForTimeout(800);
    const opener = page.getByRole('button', { name: 'What these words mean' }).first();
    await opener.click(); await page.waitForTimeout(1200);
    const dlg = page.getByTestId('sheet-dialog');
    const box = await dlg.boundingBox();
    res[`${w}x${h}`] = { sheet: box && { x: Math.round(box.x), y: Math.round(box.y), w: Math.round(box.width), h: Math.round(box.height) },
                         title: await page.getByRole('dialog').getByRole('heading').first().textContent() };
    await page.screenshot({ path: `${OUT}${STATE}-${w}x${h}-5-sheet.png` });
    await page.keyboard.press('Escape'); await page.waitForTimeout(800);
    res[`${w}x${h}`].closedByEscape = (await page.getByTestId('sheet-dialog').count()) === 0;
    res[`${w}x${h}`].stillOnWater = (await page.getByRole('tablist', { name: 'Views of this water' }).count()) === 1;
  }
  return JSON.stringify(res);
}
