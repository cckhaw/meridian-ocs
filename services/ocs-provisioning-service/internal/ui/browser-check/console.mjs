// Drives the console in a real browser. Not part of CI (needs Playwright and
// Chromium); run it by hand against a served console:
//
//   OCS_UI_DEV_ADDR=127.0.0.1:18443 go test -run ServeConsoleForManualTesting -timeout 15m ./services/ocs-provisioning-service &
//   CONSOLE_URL=http://127.0.0.1:18443 SHOT_DIR=/tmp node console.mjs
import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');

const base = process.env.CONSOLE_URL || 'http://127.0.0.1:8443';
const shots = process.env.SHOT_DIR || '.';
const failures = [];
const check = (cond, what) => { console.log(`${cond ? 'PASS' : 'FAIL'}  ${what}`); if (!cond) failures.push(what); };

const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1000, height: 900 } });
const problems = [];
const notOK = [];
// The browser logs every non-2xx fetch as a console error; those are checked separately below.
page.on('console', (m) => { if ((m.type() === 'error' || m.type() === 'warning') && !m.text().startsWith('Failed to load resource')) problems.push(m.text()); });
page.on('response', (r) => { if (r.status() >= 400) notOK.push(`${r.status()} ${new URL(r.url()).pathname}`); });
page.on('pageerror', (e) => problems.push(String(e)));

const msisdn = '4917' + String(Math.floor(Math.random() * 1e8)).padStart(8, '0');
const imsi = '26201' + String(Math.floor(Math.random() * 1e10)).padStart(10, '0');

await page.goto(base + '/ui/');
await page.waitForFunction(() => document.querySelector('#who').textContent.includes('Signed in as'));
check((await page.textContent('#who')).includes('currency EUR'), 'header shows the principal, currency and catalog');
check((await page.locator('#c-offering option').count()) === 3, 'offering picker lists the catalog offerings');

// ---- validation before anything is sent
await page.click('#tab-create');
let sent = 0;
page.on('request', (r) => { if (r.method() === 'POST') sent++; });
await page.click('#create-submit');
check((await page.textContent('#create-error')).includes('at least one identifier'), 'empty form is refused with a reason');
await page.fill('#c-msisdn', '12ab');
await page.fill('#c-imsi', '123');
await page.click('#create-submit');
check(await page.locator('#c-msisdn-err').isVisible() && await page.locator('#c-imsi-err').isVisible(), 'bad MSISDN and IMSI are flagged next to the fields');
check((await page.getAttribute('#c-msisdn', 'aria-invalid')) === 'true', 'invalid fields carry aria-invalid');
check(sent === 0, 'nothing was sent while the form was invalid');

// ---- create with offering and top-up
await page.fill('#c-msisdn', '+' + msisdn);
await page.fill('#c-imsi', imsi);
await page.selectOption('#c-offering', 'gold-offer');
await page.fill('#c-topup', '1500');
await page.click('#create-submit');
await page.waitForFunction(() => document.querySelectorAll('#steps .badge.ok').length === 3, null, { timeout: 20000 });
check((await page.locator('#steps li').count()) === 3, 'three steps: create, attach offering, top up');
await page.screenshot({ path: `${shots}/console-created.png` });

await page.click('#view-created');
await page.waitForSelector('#find-result .card');
const card = await page.textContent('#find-result');
check(card.includes('prepaid') && card.includes('gold-offer'), 'subscriber card shows payment type and offering');
check(card.includes('MSISDN ' + msisdn) && card.includes('IMSI ' + imsi), 'subscriber card shows the identifiers that were entered');
check(card.includes('1500') && card.includes('data10g') && card.includes('10.00 GiB'), 'buckets: cash top-up and the offering allowances with readable units');
await page.screenshot({ path: `${shots}/console-subscriber.png` });

// ---- find by MSISDN (with formatting) and by IMSI
await page.click('#tab-find');
await page.fill('#find-value', '+' + msisdn.slice(0, 4) + ' ' + msisdn.slice(4));
await page.click('#find-submit');
await page.waitForSelector('#find-result .card');
check((await page.textContent('#find-result h2')).includes('sub-'), 'find by MSISDN, typed with a + and a space, returns the subscriber');
await page.selectOption('#find-type', 'imsi');
await page.fill('#find-value', imsi);
await page.click('#find-submit');
await page.waitForSelector('#find-result .card');
check((await page.textContent('#find-result')).includes('IMSI ' + imsi), 'find by IMSI');

// ---- manage the subscriber: top up, correct, change offering, lifecycle
const posts = [];
page.on('request', (r) => { if (r.method() === 'POST') posts.push(new URL(r.url()).pathname); });
const cashLine = async () => (await page.textContent('#find-result table')).replace(/\s+/g, ' ');
check(await page.locator('#manage form.action').count() === 4, 'the manage card offers top-up, offering, correction and lifecycle');
const before = posts.length;
await page.fill('#act-topup-amount', 'abc');
await page.click('#act-topup');
check((await page.textContent('#topup .action-status')).includes('whole number') && posts.length === before, 'a bad top-up amount is refused in the page and nothing is sent');
await page.fill('#act-topup-amount', '250');
await page.click('#act-topup');
await page.waitForSelector('#action-flash');
check((await page.textContent('#action-flash')).includes('Cash balance is now 1750'), 'top-up reports the new cash balance');
check((await cashLine()).includes('1750'), 'the card shows the new balance');
await page.screenshot({ path: `${shots}/console-manage.png` });

await page.fill('#act-adjust-delta', '-50');
await page.click('#act-adjust');
check((await page.textContent('#adjust .action-status')).includes('reason'), 'a correction without a reason is refused in the page');
await page.fill('#act-adjust-reason', 'goodwill reversal');
await page.click('#act-adjust');
await page.waitForFunction(() => document.querySelector('#action-flash') && document.querySelector('#action-flash').textContent.includes('1700'));
check((await cashLine()).includes('1700'), 'a correction changes the balance and is shown');

await page.selectOption('#act-offering', 'basic-offer');
await page.click('#act-offering-go');
await page.waitForFunction(() => document.querySelector('#action-flash') && document.querySelector('#action-flash').textContent.includes('Offering is now'), null, { timeout: 8000 }).catch(async () => { console.log('OFFERING STATUS:', await page.textContent('#offering .action-status')); });
check((await page.textContent('#find-result')).includes('Offering is now basic-offer'), 'changing the offering is confirmed and shown on the card');

await page.selectOption('#act-lifecycle-target', 'suspended');
await page.fill('#act-lifecycle-reason', 'fraud check');
await page.click('#act-lifecycle-go');
await page.waitForFunction(() => document.querySelector('#find-result .pill.suspended'));
check((await page.textContent('#action-flash')).includes('from active to suspended'), 'suspending is confirmed');
check((await page.locator('#act-lifecycle-target option').allTextContents()).join() === 'active,terminated', 'only the valid next states are offered from suspended');

await page.selectOption('#act-lifecycle-target', 'terminated');
check(await page.locator('#act-lifecycle-confirm').isVisible(), 'termination asks for the subscriber key');
const sentBefore = posts.length;
await page.fill('#act-lifecycle-confirm', 'wrong');
await page.click('#act-lifecycle-go');
check((await page.textContent('#lifecycle .action-status')).includes('exactly') && posts.length === sentBefore, 'termination with the wrong key sends nothing');
const key = (await page.textContent('#find-result h2')).split(' ')[0];
await page.fill('#act-lifecycle-confirm', key);
await page.click('#act-lifecycle-go');
await page.waitForFunction(() => document.querySelector('#find-result .pill.terminated'));
check((await page.textContent('#manage')).includes('terminated; nothing can be changed') && (await page.locator('#manage form').count()) === 0, 'a terminated subscriber offers no actions');
await page.screenshot({ path: `${shots}/console-terminated.png` });

// ---- not found
await page.selectOption('#find-type', 'msisdn');
await page.fill('#find-value', '49179999999999');
await page.click('#find-submit');
await page.waitForSelector('#find-error:not([hidden])');
check((await page.textContent('#find-error')).includes('No subscriber has that MSISDN'), 'unknown MSISDN gives a clear message');
check((await page.locator('#find-result .card').count()) === 0, 'no stale result is left on screen');

// ---- creating a second subscriber with the same MSISDN fails visibly, and says why
await page.click('#tab-create');
await page.click('#create-reset');
await page.fill('#c-msisdn', msisdn);
await page.click('#create-submit');
await page.waitForSelector('#steps .badge.fail', { timeout: 20000 });
const failText = await page.textContent('#steps');
check(failText.includes('identity_conflict') || failText.includes('already belongs'), 'a duplicate MSISDN is refused with the server\'s reason');
check(await page.locator('#retry').isVisible(), 'a failed step offers Retry');
check(!(await page.locator('#view-created').isVisible()), 'no "view subscriber" for a creation that failed');
await page.screenshot({ path: `${shots}/console-conflict.png` });

// ---- markup in data is shown as text, never interpreted
await page.click('#create-reset');
const evilKey = '<i/id=pwn>t</i>' + Math.floor(Math.random() * 1e6); // printable, no spaces or braces: the form accepts it
await page.fill('#c-key', evilKey);
await page.fill('#c-msisdn', '4917' + String(Math.floor(Math.random() * 1e8)).padStart(8, '0'));
await page.click('#create-submit');
await page.waitForFunction(() => document.querySelectorAll('#steps .badge.ok').length === 1, null, { timeout: 20000 });
const stepText = await page.textContent('#steps');
check(stepText.includes(evilKey), 'the progress list shows the key literally');
await page.click('#view-created');
await page.waitForSelector('#find-result .card');
check((await page.textContent('#find-result h2')).includes(evilKey), 'the subscriber card shows the key literally');
check((await page.locator('#pwn').count()) === 0 && (await page.locator('#find-result i, #steps i').count()) === 0, 'markup in a subscriber key is not interpreted as HTML');

// ---- page hygiene
check(problems.length === 0, 'no console errors or warnings (including CSP violations)' + (problems.length ? ': ' + problems.join(' | ') : ''));

check(notOK.length === 1 && notOK[0] === '404 /v1/subscribers',
  'the only failed request is the unknown-MSISDN lookup (a refused creation is a command outcome, not an HTTP error): ' + notOK.join(', '));

// ---- small screen
await page.setViewportSize({ width: 375, height: 800 });
await page.click('#tab-create');
const overflow = await page.evaluate(() => document.documentElement.scrollWidth > document.documentElement.clientWidth);
check(!overflow, 'no horizontal scrolling at phone width');
await page.screenshot({ path: `${shots}/console-mobile.png` });

// dark colour scheme renders (screenshot for a human to look at)
await page.emulateMedia({ colorScheme: 'dark' });
await page.setViewportSize({ width: 1000, height: 900 });
await page.click('#tab-find');
await page.screenshot({ path: `${shots}/console-dark.png` });

await browser.close();
console.log(failures.length ? `\n${failures.length} check(s) failed` : '\nall checks passed');
process.exit(failures.length ? 1 : 0);
