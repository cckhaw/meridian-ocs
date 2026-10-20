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

  const tabs = [$('#tab-find'), $('#tab-create'), $('#tab-approvals')];
  function showTab(tab) {
    for (const t of tabs) {
      const on = t === tab;
      t.setAttribute('aria-selected', String(on));
      t.tabIndex = on ? 0 : -1;
      $('#' + t.getAttribute('aria-controls')).hidden = !on;
    }
    if (tab.id === 'tab-approvals') loadApprovals();
  }
  tabs.forEach((t, i) => {
    t.addEventListener('click', () => showTab(t));
    t.addEventListener('keydown', (ev) => {
      if (ev.key !== 'ArrowLeft' && ev.key !== 'ArrowRight') return;
      const visible = tabs.filter((x) => !x.hidden);
      const at = visible.indexOf(t);
      const next = visible[(at + (ev.key === 'ArrowRight' ? 1 : visible.length - 1)) % visible.length];
      showTab(next);
      next.focus();
    });
  });

  // ---- meta (who am I, currency, offerings) -------------------------------

  let meta = { currency: '', maxTopUp: 0, maxAdjust: 0, can: {}, offerings: [], approvals: { enabled: false } };

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
    // Approvers get a tab for the requests that wait for a second person.
    $('#tab-approvals').hidden = !(meta.approvals && meta.approvals.enabled && meta.can.approve);
    if (!$('#tab-approvals').hidden) refreshApprovalCount();
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
    renderManage(into, s);
  }

  // ---- manage -------------------------------------------------------------

  // Allowed lifecycle moves (the server enforces them; this only offers valid ones).
  const NEXT = { active: ['grace', 'suspended', 'terminated'], grace: ['active', 'suspended', 'terminated'], suspended: ['active', 'terminated'], terminated: [] };
  const attempts = new Map(); // action -> idempotency key of the attempt in progress
  let flash = null;           // one message shown after the card refreshes

  function attemptKey(name) {
    if (!attempts.has(name)) attempts.set(name, 'ui-' + (crypto.randomUUID ? crypto.randomUUID() : rand(16)));
    return attempts.get(name);
  }

  // perform sends one command and follows it to its outcome. The idempotency
  // key survives only an unknown outcome (timeout, network, 5xx), so a retry
  // cannot apply the change twice; a definite answer, or an edit to the form,
  // starts a new attempt.
  async function perform(name, form, method, path, body, describe) {
    const status = $('.action-status', form);
    const buttons = form.querySelectorAll('button');
    buttons.forEach((b) => { b.disabled = true; });
    status.className = 'action-status hint';
    status.textContent = 'Working...';
    try {
      const acc = await api(method, path, body, attemptKey(name));
      const rec = await waitForOutcome(acc.commandId);
      if (!rec) {
        status.className = 'action-status error';
        status.textContent = 'Still being processed after 30 seconds. Press the button again to keep waiting; it will not be applied twice.';
      } else if (rec.state === 'failed') {
        attempts.delete(name);
        status.className = 'action-status error';
        status.textContent = `${rec.message || 'The request was refused.'} (${rec.outcome})`;
      } else if (rec.state === 'pending_approval') {
        // Nothing has happened yet: a second person has to approve it first.
        attempts.delete(name);
        const by = rec.approval && rec.approval.expiresAt ? ` It expires ${when(rec.approval.expiresAt)}.` : '';
        flash = { cls: 'warn-text', text: `Submitted for approval, not applied yet. A different authorised person has to approve it (rule ${rec.approval ? rec.approval.rule : ''}).${by}` };
        await viewByKey(currentKey);
        refreshApprovalCount();
        return;
      } else {
        attempts.delete(name);
        flash = { cls: 'ok-text', text: describe(rec) };
        await viewByKey(currentKey);
        return;
      }
    } catch (e) {
      if (e.status >= 400 && e.status < 500) attempts.delete(name);
      status.className = 'action-status error';
      status.textContent = e.message;
    }
    buttons.forEach((b) => { b.disabled = false; });
  }

  let currentKey = '';

  function actionForm(id, title, ...kids) {
    const f = el('form', { class: 'action', id, novalidate: '' }, el('h3', { text: title }), ...kids,
      el('p', { class: 'action-status hint', role: 'status', 'aria-live': 'polite' }));
    f.addEventListener('input', () => attempts.delete(id));
    return f;
  }

  function renderManage(into, s) {
    currentKey = s.subscriberKey;
    const card = el('div', { class: 'card', id: 'manage' }, el('h2', { text: 'Manage' }));
    if (flash) {
      card.append(el('p', { class: flash.cls, id: 'action-flash', role: 'status', text: flash.text }));
      flash = null;
    }
    if (meta.approvals && meta.approvals.enabled && (meta.can.adjustBalance || meta.can.changeLifecycle)) {
      card.append(el('p', { class: 'hint', id: 'approval-hint', text: `Corrections and status changes that match the approval policy (${meta.approvals.policy}) wait for a second person before they are applied.` }));
    }
    const can = meta.can || {};
    const key = encodeURIComponent(s.subscriberKey);
    const base = `/v1/subscribers/${key}`;
    if (s.status === 'terminated') {
      card.append(el('p', { class: 'hint', text: 'This subscriber is terminated; nothing can be changed.' }));
      into.append(card);
      return;
    }
    let any = false;

    if (can.topUp) {
      any = true;
      const amount = el('input', { id: 'act-topup-amount', inputmode: 'numeric', autocomplete: 'off', placeholder: `minor units, up to ${meta.maxTopUp}` });
      const go = el('button', { class: 'primary', type: 'submit', id: 'act-topup', text: 'Top up' });
      const f = actionForm('topup', 'Top up the cash balance',
        el('div', { class: 'row' }, el('label', { class: 'field grow' }, `Amount (${meta.currency}, minor units)`, amount), go));
      f.addEventListener('submit', (ev) => {
        ev.preventDefault();
        const v = amount.value.trim();
        const st = $('.action-status', f);
        if (!/^[0-9]{1,15}$/.test(v) || Number(v) < 1 || (meta.maxTopUp && Number(v) > meta.maxTopUp)) {
          st.className = 'action-status error';
          st.textContent = `Enter a whole number between 1 and ${meta.maxTopUp}.`;
          return;
        }
        perform('topup', f, 'POST', `${base}/top-ups`, { amountMinorUnits: Number(v), currency: meta.currency },
          (rec) => `Topped up ${v} ${meta.currency} (minor units). Cash balance is now ${rec.balance}.`);
      });
      card.append(f);
    }

    if (can.attachOffering && meta.offerings.length) {
      any = true;
      const sel = el('select', { id: 'act-offering' });
      for (const o of meta.offerings) {
        sel.append(el('option', { value: o.id, text: (o.name ? `${o.name} (${o.id})` : o.id) + (o.id === s.offering ? ' - current' : '') }));
      }
      if (s.offering) sel.value = s.offering;
      const renew = el('input', { type: 'checkbox', id: 'act-offering-renew' });
      const go = el('button', { class: 'primary', type: 'submit', id: 'act-offering-go', text: 'Change offering' });
      const f = actionForm('offering', 'Change the product offering',
        el('div', { class: 'row' }, el('label', { class: 'field grow' }, 'Offering', sel), go),
        el('label', { class: 'checks' }, renew, el('span', { text: 'Top up allowances that already exist to the offering\'s amount' })),
        el('p', { class: 'hint', text: 'Sets the price plan and creates the offering\'s allowance buckets. Existing buckets are kept.' }));
      f.addEventListener('submit', (ev) => {
        ev.preventDefault();
        perform('offering', f, 'POST', `${base}/offering`, { offering: sel.value, renew: renew.checked },
          () => `Offering is now ${sel.value}.`);
      });
      card.append(f);
    }

    if (can.adjustBalance) {
      any = true;
      const bucket = el('select', { id: 'act-adjust-bucket' });
      for (const b of s.buckets) bucket.append(el('option', { value: b.id, text: `${b.id} (${b.kind === 'any' ? 'money' : b.kind})` }));
      if (s.buckets.some((b) => b.id === 'cash')) bucket.value = 'cash'; // the common correction
      const delta = el('input', { id: 'act-adjust-delta', inputmode: 'numeric', autocomplete: 'off', placeholder: 'e.g. -250 or 500' });
      const reason = el('input', { id: 'act-adjust-reason', autocomplete: 'off', maxlength: '256', placeholder: 'why this correction is needed' });
      const go = el('button', { type: 'submit', id: 'act-adjust', text: 'Apply correction' });
      const f = actionForm('adjust', 'Correct a balance',
        el('div', { class: 'row' }, el('label', { class: 'field narrow' }, 'Bucket', bucket),
          el('label', { class: 'field narrow' }, 'Change (+ adds, - removes)', delta)),
        el('div', { class: 'row' }, el('label', { class: 'field grow' }, 'Reason (kept in the audit trail)', reason), go),
        el('p', { class: 'hint', text: `Corrections of up to ${meta.maxAdjust} units. Money is in minor units.` }));
      f.addEventListener('submit', (ev) => {
        ev.preventDefault();
        const st = $('.action-status', f);
        const d = delta.value.trim();
        let msg = '';
        if (!s.buckets.length) msg = 'This subscriber has no buckets.';
        else if (!/^[+-]?[0-9]{1,15}$/.test(d) || Number(d) === 0) msg = 'Enter a non-zero whole number.';
        else if (meta.maxAdjust && Math.abs(Number(d)) > meta.maxAdjust) msg = `The largest correction is ${meta.maxAdjust}.`;
        else if (!reason.value.trim()) msg = 'A reason is required.';
        if (msg) { st.className = 'action-status error'; st.textContent = msg; return; }
        perform('adjust', f, 'POST', `${base}/adjustments`,
          { bucketId: bucket.value, delta: Number(d), currency: meta.currency, reason: reason.value.trim() },
          (rec) => `Bucket ${bucket.value} changed by ${Number(d)}; balance is now ${rec.balance}.`);
      });
      card.append(f);
    }

    if (can.changeLifecycle && NEXT[s.status] && NEXT[s.status].length) {
      any = true;
      const target = el('select', { id: 'act-lifecycle-target' });
      for (const t of NEXT[s.status]) target.append(el('option', { value: t, text: t }));
      const graceUntil = el('input', { id: 'act-lifecycle-grace', type: 'datetime-local' });
      const graceField = el('label', { class: 'field narrow', hidden: true }, 'Grace until (optional)', graceUntil);
      const reason = el('input', { id: 'act-lifecycle-reason', autocomplete: 'off', maxlength: '256' });
      const confirm = el('input', { id: 'act-lifecycle-confirm', autocomplete: 'off', placeholder: s.subscriberKey });
      const confirmField = el('label', { class: 'field grow', hidden: true }, 'Type the subscriber key to confirm termination. This cannot be undone.', confirm);
      const go = el('button', { type: 'submit', id: 'act-lifecycle-go', text: 'Change status' });
      const sync = () => {
        graceField.hidden = target.value !== 'grace';
        confirmField.hidden = target.value !== 'terminated';
        go.className = target.value === 'terminated' ? 'danger' : '';
      };
      target.addEventListener('change', sync);
      sync();
      const f = actionForm('lifecycle', `Change status (now ${s.status})`,
        el('div', { class: 'row' }, el('label', { class: 'field narrow' }, 'New status', target), graceField,
          el('label', { class: 'field grow' }, 'Reason (optional)', reason)),
        el('div', { class: 'row' }, confirmField, go),
        el('p', { class: 'hint', text: 'Suspended and terminated subscribers get no new quota; termination is final.' }));
      f.addEventListener('submit', (ev) => {
        ev.preventDefault();
        const st = $('.action-status', f);
        if (target.value === 'terminated' && confirm.value.trim() !== s.subscriberKey) {
          st.className = 'action-status error';
          st.textContent = 'Type the subscriber key exactly to confirm termination.';
          return;
        }
        const body = { target: target.value, reason: reason.value.trim() };
        if (target.value === 'grace' && graceUntil.value) body.graceUntil = new Date(graceUntil.value).toISOString();
        perform('lifecycle', f, 'POST', `${base}/lifecycle`, body, (rec) => `Status changed from ${rec.from} to ${rec.to}.`);
      });
      card.append(f);
    }

    if (!any) card.append(el('p', { class: 'hint', text: 'Your certificate allows looking subscribers up only; it does not allow changes.' }));
    into.append(card);
  }

  // ---- approvals ----------------------------------------------------------

  async function refreshApprovalCount() {
    try {
      const res = await api('GET', '/v1/approvals');
      const n = res.approvals.length;
      $('#tab-approvals').textContent = n ? `Approvals (${n})` : 'Approvals';
      return res.approvals;
    } catch (_) {
      return null;
    }
  }

  let approvalFlash = null;

  async function loadApprovals() {
    const host = $('#approvals-list');
    const err = $('#approvals-error');
    err.hidden = true;
    let list;
    try {
      list = (await api('GET', '/v1/approvals')).approvals;
    } catch (e) {
      err.textContent = e.message;
      err.hidden = false;
      return;
    }
    $('#tab-approvals').textContent = list.length ? `Approvals (${list.length})` : 'Approvals';
    host.replaceChildren();
    if (approvalFlash) {
      host.append(el('p', { class: approvalFlash.cls, id: 'approval-flash', role: 'status', text: approvalFlash.text }));
      approvalFlash = null;
    }
    if (!list.length) {
      host.append(el('p', { class: 'hint', id: 'approvals-empty', text: 'Nothing is waiting for approval.' }));
      return;
    }
    for (const a of list) host.append(approvalCard(a));
  }

  function approvalCard(a) {
    const mine = a.requestedBy === meta.principal;
    const card = el('div', { class: 'card approval', 'data-id': a.commandId });
    card.append(el('h3', { text: a.summary }));
    const kv = el('dl', { class: 'kv' });
    const add = (k, v) => { kv.append(el('dt', { text: k }), el('dd', { text: v })); };
    add('Requested by', a.requestedBy);
    add('Requested', when(a.requestedAt));
    add('Expires', when(a.expiresAt));
    add('Rule', a.rule);
    add('Request id', a.commandId);
    card.append(kv);
    const status = el('p', { class: 'action-status hint', role: 'status', 'aria-live': 'polite' });
    const approve = el('button', { class: 'primary', type: 'button', text: 'Approve', 'aria-label': `Approve: ${a.summary}` });
    const why = el('input', { autocomplete: 'off', maxlength: '256', placeholder: 'why (optional)', 'aria-label': 'Reason for rejecting' });
    const reject = el('button', { type: 'button', class: 'danger', text: mine ? 'Withdraw' : 'Reject', 'aria-label': `${mine ? 'Withdraw' : 'Reject'}: ${a.summary}` });
    if (mine && !(meta.approvals && meta.approvals.allowSelf)) {
      approve.disabled = true;
      card.append(el('p', { class: 'hint self-note', text: 'You made this request: a different person has to approve it. You can withdraw it.' }));
    }
    const run = async (fn) => {
      approve.disabled = reject.disabled = true;
      status.className = 'action-status hint';
      status.textContent = 'Working...';
      try { await fn(); } catch (e) {
        status.className = 'action-status error';
        status.textContent = e.message;
        approve.disabled = mine && !(meta.approvals && meta.approvals.allowSelf);
        reject.disabled = false;
      }
    };
    approve.addEventListener('click', () => run(async () => {
      const acc = await api('POST', `/v1/commands/${encodeURIComponent(a.commandId)}/approve`);
      const rec = await waitForOutcome(acc.commandId);
      const text = !rec ? 'Approved and queued; it is still being applied.'
        : rec.state === 'applied' ? 'Approved and applied.'
        : rec.state === 'failed' ? `Approved, but it could not be applied: ${rec.message || rec.outcome}`
        : 'Approved.';
      approvalFlash = { cls: rec && rec.state === 'failed' ? 'error' : 'ok-text', text };
      await loadApprovals();
    }));
    reject.addEventListener('click', () => run(async () => {
      await api('POST', `/v1/commands/${encodeURIComponent(a.commandId)}/reject`, { reason: why.value.trim() });
      approvalFlash = { cls: 'ok-text', text: mine ? 'Request withdrawn.' : 'Request rejected.' };
      await loadApprovals();
    }));
    card.append(el('div', { class: 'row' }, el('label', { class: 'field grow' }, 'Reason for rejecting', why), approve, reject), status);
    return card;
  }

  $('#approvals-refresh').addEventListener('click', loadApprovals);

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
      if (['applied', 'failed', 'pending_approval', 'rejected', 'expired'].includes(rec.state)) return rec;
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
