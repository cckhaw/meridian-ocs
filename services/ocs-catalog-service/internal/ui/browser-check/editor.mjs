// Drives the catalog editor in a real browser. Not part of CI (needs Playwright
// and Chromium); run it by hand against a served editor:
//
//   OCS_UI_DEV_ADDR=127.0.0.1:18444 go test -run ServeEditorForManualTesting -timeout 15m ./services/ocs-catalog-service &
//   EDITOR_URL=http://127.0.0.1:18444 SHOT_DIR=/tmp node editor.mjs
import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');

const base = process.env.EDITOR_URL || 'http://127.0.0.1:8444';
const shots = process.env.SHOT_DIR || '.';
const failures = [];
const check = (cond, what) => { console.log(`${cond ? 'PASS' : 'FAIL'}  ${what}`); if (!cond) failures.push(what); };

const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1100, height: 900 } });
const problems = [];
const notOK = [];
page.on('console', (m) => { if ((m.type() === 'error' || m.type() === 'warning') && !m.text().startsWith('Failed to load resource')) problems.push(m.text()); });
page.on('response', (r) => { if (r.status() >= 400) notOK.push(`${r.status()} ${new URL(r.url()).pathname}`); });
page.on('pageerror', (e) => problems.push(String(e)));
let published = 0;
page.on('request', (r) => { if (r.method() === 'POST' && new URL(r.url()).pathname === '/v1/catalog/versions') published++; });

await page.goto(base + '/ui/');
await page.waitForFunction(() => document.querySelector('#who').textContent.includes('Signed in as'));
check((await page.textContent('#who')).includes('version 1 in effect'), 'header shows the version in effect');
check((await page.textContent('#offering-table')).includes('gold'), 'the published offering is listed');
check(await page.locator('#draft-banner').isHidden(), 'no draft banner before any edit');

// ---- opening and saving an unchanged plan does not count as a change
await page.click('#tab-plans');
await page.click('button[aria-label="Edit plan std"]');
await page.click('#p-apply');
check(await page.locator('#draft-banner').isHidden(), 'applying an unchanged plan leaves no pending change');

// ---- new plan
await page.click('#new-plan');
await page.fill('#p-id', 'night');
await page.fill('#plan-form label:has-text("Name") input >> nth=0', 'Night saver');
await page.locator('#rule-list label:has-text("numerator") input').fill('3');
await page.locator('#rule-list label:has-text("Applies when") input').fill('hour >= 22 || hour < 6');
await page.click('#add-filter');
await page.locator('#filter-list label:has-text("Filter id") input').fill('no-roam');
await page.locator('#filter-list label:has-text("Bar when") input').fill('rat == "ROAM"');
await page.click('#p-apply');
check((await page.textContent('#plan-table')).includes('night') && (await page.textContent('#plan-table')).includes('new'), 'new plan is listed and tagged new');
check((await page.textContent('#draft-banner')).includes('1'), 'banner counts the pending change');

// a duplicate id is refused before anything else
await page.click('#new-plan');
await page.fill('#p-id', 'night');
await page.click('#p-apply');
check((await page.textContent('#p-error')).includes('already exists'), 'duplicate plan id is refused');
await page.click('#plan-form button:has-text("Cancel")');

// ---- new offering with markup in its name
await page.click('#tab-offerings');
await page.click('#new-offering');
await page.fill('#o-id', 'silver');
const evil = '<i id=pwn>Silver</i>';
await page.fill('#o-name', evil);
await page.selectOption('#o-plan', 'night');
await page.click('#add-allowance');
await page.locator('#allowance-list label:has-text("Bucket id") input').fill('data1g');
await page.locator('#allowance-list label:has-text("Amount") input').fill(String(1 << 30));
check((await page.textContent('#allowance-list')).includes('1.00 GiB'), 'the allowance amount is shown in readable units');
await page.click('#o-apply');
const row = await page.textContent('#offering-table');
check(row.includes(evil) && (await page.locator('#pwn').count()) === 0, 'markup in an offering name is shown literally, not interpreted');
check(row.includes('night'), 'the offering shows its plan');

// ---- review: nothing can be published before validation
await page.click('#tab-publish');
const changes = await page.textContent('#changes');
check(changes.includes('offering') && changes.includes('silver') && changes.includes('night'), 'the review lists the added plan and offering');
check(await page.locator('#publish').isDisabled(), 'publish is disabled before validation');
await page.screenshot({ path: `${shots}/editor-review.png` });

// ---- an invalid draft reports the problem with its path and cannot be published
await page.click('#tab-plans');
await page.click('button[aria-label="Edit plan night"]');
await page.locator('#rule-list label:has-text("Applies when") input').fill('bogus ==');
await page.click('#p-apply');
await page.click('#tab-publish');
await page.click('#validate');
await page.waitForSelector('#validation .problems');
const v = await page.textContent('#validation');
check(v.includes('problem') && v.includes('plans[1].rules[0].when') && v.includes('bogus'), 'validation names the path and the cause: ' + v.slice(0, 120));
await page.check('#confirm-publish');
check(await page.locator('#publish').isDisabled(), 'an invalid draft cannot be published even when confirmed');
await page.uncheck('#confirm-publish');

// fix it
await page.click('#tab-plans');
await page.click('button[aria-label="Edit plan night"]');
await page.locator('#rule-list label:has-text("Applies when") input').fill('hour >= 22 || hour < 6');
await page.click('#p-apply');
await page.click('#tab-publish');
check((await page.textContent('#validation')).trim() === '', 'an old validation verdict is cleared when the draft changes');

// ---- compare prices
await page.click('#tab-publish');
await page.click('#dry-run-open');
await page.locator('#cases select').first().selectOption('night');
await page.click('#run-cases');
await page.waitForSelector('#dry-run-table');
const dr = await page.textContent('#dry-run-table');
check(dr.includes('unknown_plan') && dr.includes('cash 3/1'), 'dry-run shows the unknown plan live and the draft price proposed: ' + dr.replace(/\s+/g, ' ').slice(0, 160));
await page.screenshot({ path: `${shots}/editor-dryrun.png` });

// ---- publish
await page.fill('#note', 'night saver');
await page.click('#validate');
await page.waitForSelector('#validation .ok-text');
check(await page.locator('#publish').isDisabled(), 'a valid draft still needs the confirmation tick');
await page.check('#confirm-publish');
check(await page.locator('#publish').isEnabled(), 'valid + confirmed enables publish');
await page.click('#publish');
await page.waitForSelector('#publish-ok:not([hidden])');
check((await page.textContent('#publish-ok')).includes('version 2'), 'published as version 2');
check(published === 1, 'exactly one publish request was sent');
check(await page.locator('#draft-banner').isHidden(), 'the draft is clean after publishing');
check((await page.textContent('#who')).includes('version 1 in effect') || (await page.textContent('#who')).includes('version 2'), 'header still shows a version');

// ---- history
await page.click('#tab-history');
const hist = await page.textContent('#version-table');
check(hist.includes('v2') && hist.includes('v1') && hist.includes('night saver'), 'history lists both versions with the note');
check(hist.includes('scheduled'), 'the future-dated version is marked scheduled');
await page.click('button[aria-label="Start a draft from version 1"]');
await page.waitForSelector('#panel-publish:not([hidden])');
check((await page.textContent('#changes')).includes('removed'), 'starting from the older version shows the later additions as removed');
await page.click('#discard');
check(await page.locator('#draft-banner').isHidden(), 'discard returns to the newest published version');

// ---- JSON round trip
await page.click('#tab-json');
const json = JSON.parse(await page.inputValue('#json-text'));
check(json.plans.length === 2 && json.offerings.length === 2, 'JSON tab shows the draft');
json.offerings[0].name = 'Renamed';
await page.fill('#json-text', JSON.stringify(json));
await page.click('#json-load');
check((await page.textContent('#offering-table')).includes('Renamed') && !(await page.locator('#draft-banner').isHidden()), 'loading JSON replaces the draft');
await page.fill('#json-text', '{"plans": 3}');
await page.click('#json-load');
check((await page.textContent('#json-error')).includes('must be a list'), 'bad JSON is refused with a reason');
await page.click('#tab-publish');
await page.click('#discard');

// ---- a stale editor is told so
const other = await browser.newPage();
await other.goto(base + '/ui/');
await other.waitForFunction(() => document.querySelector('#who').textContent.includes('Signed in as'));
await other.click('#tab-offerings');
await other.click('#new-offering');
await other.fill('#o-id', 'late');
await other.selectOption('#o-plan', 'std');
await other.click('#o-apply');
await page.click('#tab-offerings');
await page.click('#new-offering');
await page.fill('#o-id', 'early');
await page.selectOption('#o-plan', 'std');
await page.click('#o-apply');
for (const [p, label] of [[page, 'early'], [other, 'late']]) {
  await p.click('#tab-publish');
  await p.click('#validate');
  await p.waitForSelector('#validation .ok-text');
  await p.check('#confirm-publish');
}
await page.click('#publish');
await page.waitForSelector('#publish-ok:not([hidden])');
await other.click('#publish');
await other.waitForSelector('#publish-error:not([hidden])');
check((await other.textContent('#publish-error')).includes('Someone else published'), 'a stale editor is told someone else published');
await other.close();

check(problems.length === 0, 'no console errors or warnings (including CSP violations)' + (problems.length ? ': ' + problems.join(' | ') : ''));
check(notOK.length === 0, 'no request from the first editor failed (the stale one is checked on its own page): ' + notOK.join(', '));

// ---- small screen and dark mode
await page.setViewportSize({ width: 375, height: 800 });
for (const tab of ['offerings', 'plans', 'publish', 'history']) {
  await page.click('#tab-' + tab);
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth);
  check(!overflow, `no horizontal scrolling at phone width on ${tab}`);
}
await page.click('#tab-plans');
await page.click('button[aria-label="Edit plan night"]');
check(!(await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth)), 'the plan form fits a phone');
await page.screenshot({ path: `${shots}/editor-mobile.png` });
await page.emulateMedia({ colorScheme: 'dark' });
await page.setViewportSize({ width: 1100, height: 900 });
await page.screenshot({ path: `${shots}/editor-dark.png` });

await browser.close();
console.log(failures.length ? `\n${failures.length} check(s) failed` : '\nall checks passed');
process.exit(failures.length ? 1 : 0);
