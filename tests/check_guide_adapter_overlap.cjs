const fs = require('fs');
const path = require('path');
const assert = require('assert');
const crypto = require('crypto');

const bundle = path.resolve(process.argv[2]);
const workspace = path.resolve(process.argv[3]);
const { chromium } = require(path.join(workspace, '.scratch/tmp/interactive-guide/browser-runtime/node_modules/playwright'));
const guide = path.join(bundle, 'source/docs/interactive/index.html');
const evidence = path.join(bundle, 'evidence');

(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  const errors = [], requests = [], visited = [];
  page.on('pageerror', error => errors.push(error.message));
  page.on('request', request => { if (/^https?:/.test(request.url())) requests.push(request.url()); });
  try {
    await page.goto('file://' + guide);
    for (const module of ['process', 'demux', 'dedup', 'report', 'resources']) {
      await page.locator(`[data-route="${module}"]`).click();
      if (module === 'process') await page.locator('#layout').selectOption('pe');
      const ids = await page.locator('#flow button:not(.off)').evaluateAll(nodes => nodes.map(node => node.dataset.id));
      for (const id of ids) {
        await page.locator(`#flow [data-id="${id}"]`).click();
        assert((await page.locator('#main').innerText()).length > 80, `${module}/${id}`);
        visited.push(`${module}/${id}`);
      }
    }
    await page.locator('[data-route="process"]').click();
    await page.locator('#layout').selectOption('se');
    await page.locator('#flow [data-id="adapters"]').click();
    assert((await page.locator('#main').innerText()).includes('requires paired-end input'));
    await page.locator('#layout').selectOption('pe');
    await page.locator('#flow [data-id="adapters"]').click();
    assert((await page.locator('#main').innerText()).includes('Both single-base tails remain'));
    await page.locator('[data-setting="adapterOverlap"]').selectOption('yes');
    assert((await page.locator('#main').innerText()).includes('Both clean reads end at the insert'));
    assert.equal(await page.locator('#main .adapterOverlap-pair .cut-base').count(), 2);
    assert.equal(await page.locator('#main .adapterOverlap-pair').count(), 2);
    await page.screenshot({ path: path.join(evidence, 'guide-adapter-overlap-desktop.png'), fullPage: true });
    await page.setViewportSize({ width: 390, height: 844 });
    await page.screenshot({ path: path.join(evidence, 'guide-adapter-overlap-mobile.png'), fullPage: true });
    const mobile = await page.locator('#main .adapterOverlap-pair').first().evaluate(node => ({ width: node.clientWidth, columns: getComputedStyle(node).gridTemplateColumns }));
    assert(!mobile.columns.includes(' '), `Adapter-overlap mate cards did not stack on mobile: ${mobile.columns}`);
    assert.deepEqual(errors, []);
    assert.deepEqual(requests, []);
    fs.writeFileSync(path.join(evidence, 'guide-adapter-overlap-browser.json'), JSON.stringify({ status: 'PASS', guide_sha256: crypto.createHash('sha256').update(fs.readFileSync(guide)).digest('hex'), visited, adapter_overlap_states: ['SE unavailable', 'PE off', 'PE on'], mobile, console_errors: errors, network_requests: requests }, null, 2) + '\n');
    console.log('PASS', visited.length, 'operation panels and Adapter-overlap browser states');
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exit(1); });
