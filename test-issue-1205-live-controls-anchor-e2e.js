/**
 * E2E regression for #1205:
 *   Live Map settings toggle row (Heat / Ghosts / Realistic / Color by hash /
 *   Matrix / Rain / …) must be DOM-anchored inside the legend / settings
 *   panel container (#liveLegend), NOT a free-floating sibling of <body>
 *   or a default-positioned .live-overlay parked elsewhere on the map.
 *
 *   Acceptance criterion (from issue body):
 *     "E2E DOM assertion: the toggle row's parent is the expected panel
 *      container element (by class/id), not body or .map-overlay"
 *
 * Run: BASE_URL=http://localhost:13581 node test-issue-1205-live-controls-anchor-e2e.js
 */
'use strict';
const { chromium } = require('playwright');

const BASE = process.env.BASE_URL || 'http://localhost:13581';

let passed = 0, failed = 0;
async function step(name, fn) {
  try { await fn(); passed++; console.log('  ✓ ' + name); }
  catch (e) { failed++; console.error('  ✗ ' + name + ': ' + e.message); }
}
function assert(c, m) { if (!c) throw new Error(m || 'assertion failed'); }

async function gotoLive(page) {
  await page.goto(BASE + '/#/live', { waitUntil: 'domcontentloaded' });
  await page.waitForSelector('#liveLegend, .live-legend', { timeout: 8000 });
  await page.waitForTimeout(400);
}

(async () => {
  const browser = await chromium.launch({
    headless: true,
    executablePath: process.env.CHROMIUM_PATH || undefined,
    args: ['--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage'],
  });
  console.log(`\n=== #1205 live-controls DOM anchor E2E against ${BASE} ===`);

  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const page = await ctx.newPage();
  await gotoLive(page);

  await step('#liveControls exists', async () => {
    const present = await page.locator('#liveControls').count();
    assert(present === 1, 'expected exactly one #liveControls element');
  });

  await step('#liveControls is a descendant of #liveLegend', async () => {
    const inside = await page.evaluate(() => {
      const ctrl = document.getElementById('liveControls');
      const legend = document.getElementById('liveLegend');
      if (!ctrl || !legend) return false;
      return legend.contains(ctrl);
    });
    assert(inside,
      '#liveControls must be a descendant of #liveLegend — got a free-floating overlay (issue #1205)');
  });

  await step('#liveControls parent is the legend panel (not body / not .live-page)', async () => {
    const parentInfo = await page.evaluate(() => {
      const ctrl = document.getElementById('liveControls');
      if (!ctrl || !ctrl.parentElement) return { tag: null, id: null, cls: null };
      const p = ctrl.parentElement;
      return { tag: p.tagName, id: p.id, cls: p.className };
    });
    assert(parentInfo.tag !== 'BODY',
      `#liveControls parent is <body> — it has detached from the legend panel`);
    assert(!(parentInfo.cls || '').split(/\s+/).includes('live-page'),
      `#liveControls parent is .live-page (free-floating overlay), not anchored to legend`);
    // Parent must be the legend or one of its inner wrappers (panel-content).
    const ok = parentInfo.id === 'liveLegend' ||
               (parentInfo.cls || '').split(/\s+/).some(c => /panel-content|live-legend/.test(c));
    assert(ok,
      `#liveControls parent must be #liveLegend or its .panel-content — got id=${parentInfo.id} cls=${parentInfo.cls}`);
  });

  await step('#liveControls is visually inside the legend bounding box', async () => {
    const fits = await page.evaluate(() => {
      const c = document.getElementById('liveControls').getBoundingClientRect();
      const l = document.getElementById('liveLegend').getBoundingClientRect();
      // 2px slack for borders/blur.
      return c.left >= l.left - 2 && c.right <= l.right + 2 &&
             c.top  >= l.top  - 2 && c.bottom <= l.bottom + 2;
    });
    assert(fits, '#liveControls bounding rect must lie inside #liveLegend bounding rect');
  });

  await browser.close();
  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
})().catch(e => { console.error(e); process.exit(2); });
