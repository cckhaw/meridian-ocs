'use strict';

// Meridian OCS console. Plain JavaScript, no dependencies. Everything shown is
// written with textContent / setAttribute, never innerHTML, so data from the API
// cannot inject markup.
(() => {
  const $ = (sel, root = document) => root.querySelector(sel);

  function el(tag, props = {}, ...kids) {
    const n = document.createElement(tag);
    for (const [k, v] of Object.entries(props)) {
      if (k === 'class') n.className = v;
      else if (k === 'text') n.textContent = v;
      else n.setAttribute(k, v);
    }
    for (const kid of kids) n.append(kid);
    return n;
  }

  // ---- API ----------------------------------------------------------------

  class ApiError extends Error {
    constructor(status, code, message) {
      super(message);
      this.status = status;
      this.code = code;
    }
  }

  function friendly(status, e) {
    switch (status) {
      case 401: return 'Not signed in: the service requires a client certificate.';
      case 403: return 'You are not allowed to do this.';
      case 429: return 'Too many requests; wait a moment and try again.';
      case 502: case 503: case 504: return 'The service is temporarily unavailable. Try again in a moment.';
    }
    return (e && e.message) || `The request failed (${status}).`;
  }

  async function api(method, path, body, idempotencyKey) {
    const headers = { Accept: 'application/json' };
    let payload;
    if (body !== undefined) {
      headers['Content-Type'] = 'application/json';
      payload = JSON.stringify(body);
    }
    if (idempotencyKey) headers['Idempotency-Key'] = idempotencyKey;
    let res;
    try {
      res = await fetch(path, { method, headers, body: payload, credentials: 'same-origin', cache: 'no-store' });
    } catch (_) {
      throw new ApiError(0, 'network', 'Cannot reach the service. Check your connection and your client certificate.');
    }
    let data = null;
    try { data = await res.json(); } catch (_) { /* no body */ }
    if (!res.ok) {
      const e = (data && data.error) || {};
      throw new ApiError(res.status, e.code || 'error', friendly(res.status, e));
    }
    return data;
  }

  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  // ---- tabs ---------------------------------------------------------------

  const tabs = [$('#tab-find'), $('#tab-create')];
  function showTab(tab) {
    for (const t of tabs) {
      const on = t === tab;
      t.setAttribute('aria-selected', String(on));
      t.tabIndex = on ? 0 : -1;
      $('#' + t.getAttribute('aria-controls')).hidden = !on;
    }
  }
  tabs.forEach((t, i) => {
    t.addEventListener('click', () => showTab(t));
    t.addEventListener('keydown', (ev) => {
      if (ev.key !== 'ArrowLeft' && ev.key !== 'ArrowRight') return;
      const next = tabs[(i + (ev.key === 'ArrowRight' ? 1 : tabs.length - 1)) % tabs.length];
      showTab(next);
      next.focus();
    });
  });

  // ---- meta (who am I, currency, offerings) -------------------------------

  let meta = { currency: '', maxTopUp: 0, offerings: [] };

  async function loadMeta() {
    try {
      meta = await api('GET', '/v1/meta');
    } catch (e) {
      $('#who').textContent = e.message;
      return;
    }
    const catalog = meta.catalogVersion ? `catalog v${meta.catalogVersion}` : 'no catalog in effect';
    $('#who').textContent = `Signed in as ${meta.principal || 'unknown'} · currency ${meta.currency} · ${catalog}`;
    $('#c-currency').textContent = `(${meta.currency}, minor units)`;
    const sel = $('#c-offering');
    for (const o of meta.offerings) {
      const label = o.name ? `${o.name} (${o.id})` : o.id;
      sel.append(el('option', { value: o.id, text: `${label}, ${o.allowances} allowance(s)` }));
    }
    $('#offering-hint').textContent = meta.offerings.length
      ? 'Choosing an offering sets the price plan and creates its allowance buckets.'
      : 'No offerings are available: no catalog version is in effect.';
    if (meta.maxTopUp) $('#c-topup').setAttribute('placeholder', `minor units, up to ${meta.maxTopUp}`);
  }

  // ---- formatting ---------------------------------------------------------

  function formatUnits(kind, n) {
    switch (kind) {
      case 'bytes': {
        const u = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
        let v = n, i = 0;
        while (Math.abs(v) >= 1024 && i < u.length - 1) { v /= 1024; i++; }
        return `${i === 0 ? v : v.toFixed(2)} ${u[i]}`;
      }
      case 'seconds': return `${n} s`;
      case 'events': return `${n} events`;
    }
    return `${n} ${meta.currency || ''} (minor units)`.trim();
  }

  const when = (s) => (s ? new Date(s).toLocaleString() : 'never');

  // ---- subscriber card ----------------------------------------------------

  function renderSubscriber(into, s) {
    into.replaceChildren();
    const card = el('div', { class: 'card' });
    const head = el('h2', { text: s.subscriberKey + ' ' });
    head.append(el('span', { class: `pill ${s.status}`, text: s.status }));
    card.append(head);

    const kv = el('dl', { class: 'kv' });
    const add = (k, v) => { kv.append(el('dt', { text: k }), el('dd', { text: v })); };
    add('Payment type', s.paymentType);
    add('Offering', s.offering || 'none (default plan)');
    add('Created', when(s.createdAt));
    add('Status since', when(s.statusAt));
    if (s.graceUntil) add('Grace until', when(s.graceUntil));
    add('Version', String(s.version));
    const ids = (s.identifiers || []).map((i) => `${i.type.toUpperCase()} ${i.value}`);
    add('Identifiers', ids.length ? ids.join(', ') : 'not recorded (created before identifiers were kept)');
    card.append(kv);

    card.append(el('h2', { text: 'Buckets' }));
    if (!s.buckets.length) {
      card.append(el('p', { class: 'hint', text: 'No buckets.' }));
    } else {
      const table = el('table');
      table.append(el('thead', {}, el('tr', {},
        el('th', { text: 'Bucket' }), el('th', { text: 'Class' }), el('th', { text: 'Unit' }),
        el('th', { text: 'Balance' }), el('th', { text: 'Expires' }))));
      const body = el('tbody');
      for (const b of s.buckets) {
        const bal = el('td', { class: 'num', text: formatUnits(b.kind, b.balance), title: String(b.balance) });
        body.append(el('tr', {}, el('td', { text: b.id }), el('td', { text: b.class }),
          el('td', { text: b.kind === 'any' ? 'money' : b.kind }), bal, el('td', { text: when(b.expiresAt) })));
      }
      table.append(body);
      card.append(table);
    }
    into.append(card);
  }

  // ---- find ---------------------------------------------------------------

  function normalizeIdentifier(type, v) {
    v = v.trim();
    return type === 'msisdn' ? v.replace(/[\s\-().]/g, '').replace(/^\+/, '') : v;
  }

  $('#find-form').addEventListener('submit', async (ev) => {
    ev.preventDefault();
    const type = $('#find-type').value;
    const value = normalizeIdentifier(type, $('#find-value').value);
    const err = $('#find-error');
    const out = $('#find-result');
    err.hidden = true;
    out.replaceChildren();
    if (!value) {
      err.textContent = 'Enter an identifier to search for.';
      err.hidden = false;
      return;
    }
    const btn = $('#find-submit');
    btn.disabled = true;
    try {
      renderSubscriber(out, await api('GET', `/v1/subscribers?${type}=${encodeURIComponent(value)}`));
    } catch (e) {
      err.textContent = e.status === 404 ? `No subscriber has that ${type.toUpperCase()}.` : e.message;
      err.hidden = false;
    } finally {
      btn.disabled = false;
    }
  });

  async function viewByKey(key) {
    showTab($('#tab-find'));
    const out = $('#find-result');
    const err = $('#find-error');
    err.hidden = true;
    try {
      renderSubscriber(out, await api('GET', `/v1/subscribers/${encodeURIComponent(key)}`));
    } catch (e) {
      out.replaceChildren();
      err.textContent = e.message;
      err.hidden = false;
    }
  }

  // ---- create -------------------------------------------------------------

  const form = $('#create-form');
  const fields = { msisdn: $('#c-msisdn'), imsi: $('#c-imsi'), nai: $('#c-nai'), key: $('#c-key'), topup: $('#c-topup') };

  function setFieldError(name, msg) {
    const input = fields[name];
    const out = $(`#c-${name}-err`);
    out.textContent = msg || '';
    out.hidden = !msg;
    if (msg) input.setAttribute('aria-invalid', 'true'); else input.removeAttribute('aria-invalid');
  }

  // validate mirrors the server's rules for fast feedback; the server decides.
  function validate() {
    const v = {
      msisdn: normalizeIdentifier('msisdn', fields.msisdn.value),
      imsi: fields.imsi.value.trim(),
      nai: fields.nai.value.trim(),
      key: fields.key.value.trim(),
      topup: fields.topup.value.trim(),
    };
    let ok = true;
    const bad = (name, msg) => { setFieldError(name, msg); ok = false; };
    for (const n of Object.keys(fields)) setFieldError(n, '');
    if (v.msisdn && !/^[0-9]{5,15}$/.test(v.msisdn)) bad('msisdn', 'Use digits only, 5 to 15 of them (a leading + is fine).');
    if (v.imsi && !/^[0-9]{14,15}$/.test(v.imsi)) bad('imsi', 'An IMSI is 14 or 15 digits.');
    if (v.nai && (v.nai.length > 128 || !v.nai.includes('@'))) bad('nai', 'An NAI looks like user@realm (at most 128 characters).');
    if (v.key && !/^[\x21-\x7a|~]{1,128}$/.test(v.key)) bad('key', 'Use 1 to 128 printable characters without spaces or braces.');
    if (v.topup) {
      if (!/^[0-9]{1,15}$/.test(v.topup) || Number(v.topup) < 1) bad('topup', 'Enter a whole number of minor units (for example cents).');
      else if (meta.maxTopUp && Number(v.topup) > meta.maxTopUp) bad('topup', `The largest single top-up is ${meta.maxTopUp}.`);
    }
    const none = !v.msisdn && !v.imsi && !v.nai;
    const e = $('#create-error');
    if (none && ok) { e.textContent = 'Give at least one identifier: MSISDN, IMSI or NAI.'; e.hidden = false; ok = false; } else e.hidden = true;
    return ok ? v : null;
  }

  const rand = (bytes) => Array.from(crypto.getRandomValues(new Uint8Array(bytes)), (b) => b.toString(16).padStart(2, '0')).join('');

  let run = null; // the submission in progress or last attempted

  form.addEventListener('input', () => { if (run && !run.busy) run = null; });

  function stepsFor(v, base, key) {
    const steps = [{
      label: 'Create subscriber ' + key,
      idem: base + '-create',
      send: (k) => api('POST', '/v1/subscribers', {
        subscriberKey: key, paymentType: $('#c-payment').value,
        identifiers: [['msisdn', v.msisdn], ['imsi', v.imsi], ['nai', v.nai]].filter(([, x]) => x).map(([type, value]) => ({ type, value })),
      }, k),
    }];
    const offering = $('#c-offering').value;
    if (offering) {
      steps.push({
        label: 'Attach offering ' + offering,
        idem: base + '-offering',
        send: (k) => api('POST', `/v1/subscribers/${encodeURIComponent(key)}/offering`, { offering }, k),
      });
    }
    if (v.topup) {
      steps.push({
        label: `Top up ${v.topup} ${meta.currency} (minor units)`,
        idem: base + '-topup',
        send: (k) => api('POST', `/v1/subscribers/${encodeURIComponent(key)}/top-ups`,
          { amountMinorUnits: Number(v.topup), currency: meta.currency }, k),
      });
    }
    return steps;
  }

  const BADGE = { pending: 'waiting', running: 'working', ok: 'done', fail: 'failed' };

  function paintSteps() {
    const list = $('#steps');
    list.replaceChildren();
    for (const s of run.steps) {
      const li = el('li', {}, el('span', { class: `badge ${s.state}`, text: BADGE[s.state] }),
        el('span', {}, el('span', { text: s.label }), ...(s.detail ? [el('div', { class: 'detail', text: s.detail })] : [])));
      list.append(li);
    }
  }

  async function waitForOutcome(commandId) {
    const deadline = Date.now() + 30000;
    let delay = 150;
    while (Date.now() < deadline) {
      const rec = await api('GET', `/v1/commands/${encodeURIComponent(commandId)}`);
      if (rec.state === 'applied' || rec.state === 'failed') return rec;
      await sleep(delay);
      delay = Math.min(delay * 1.5, 1000);
    }
    return null;
  }

  async function execute() {
    run.busy = true;
    $('#create-submit').disabled = true;
    $('#retry').hidden = true;
    $('#view-created').hidden = true;
    $('#progress-actions').hidden = true;
    let failed = false;
    for (const s of run.steps) {
      if (s.state === 'ok') continue;
      s.state = 'running';
      s.detail = '';
      paintSteps();
      try {
        const acc = await s.send(s.idem);
        const rec = await waitForOutcome(acc.commandId);
        if (!rec) {
          s.state = 'fail';
          s.detail = 'Still being processed after 30 seconds. Retry to keep waiting; the same request is not applied twice.';
        } else if (rec.state === 'failed') {
          s.state = 'fail';
          s.detail = `${rec.message || 'The request was refused.'} (${rec.outcome})`;
        } else {
          s.state = 'ok';
        }
      } catch (e) {
        s.state = 'fail';
        s.detail = e.message;
      }
      paintSteps();
      if (s.state === 'fail') { failed = true; break; }
    }
    run.busy = false;
    $('#create-submit').disabled = false;
    $('#progress-actions').hidden = false;
    $('#retry').hidden = !failed;
    $('#view-created').hidden = !run.steps[0] || run.steps[0].state !== 'ok';
    const status = $('#create-error');
    status.hidden = true;
    if (!failed) $('#view-created').focus();
  }

  form.addEventListener('submit', (ev) => {
    ev.preventDefault();
    if (run && run.busy) return;
    const v = validate();
    if (!v) return;
    const key = v.key || 'sub-' + rand(6);
    // One idempotency base per submission: a retry reuses it, so nothing is applied twice.
    const base = 'ui-' + (crypto.randomUUID ? crypto.randomUUID() : rand(16));
    run = { key, steps: stepsFor(v, base, key), busy: false };
    $('#progress-card').hidden = false;
    paintSteps();
    execute();
  });

  $('#retry').addEventListener('click', () => { if (run && !run.busy) execute(); });
  $('#view-created').addEventListener('click', () => { if (run) viewByKey(run.key); });
  $('#create-reset').addEventListener('click', () => {
    if (run && run.busy) return;
    form.reset();
    for (const n of Object.keys(fields)) setFieldError(n, '');
    $('#create-error').hidden = true;
    $('#progress-card').hidden = true;
    run = null;
  });

  loadMeta();
})();
