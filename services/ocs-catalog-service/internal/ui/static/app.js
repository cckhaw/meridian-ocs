'use strict';

// Meridian OCS catalog editor. Plain JavaScript, no dependencies. Everything
// shown is written with textContent / setAttribute / properties, never
// parsed as markup, so data from the API cannot inject any.
//
// The editor works on a draft: a private copy of the newest published version.
// Nothing reaches live pricing until "Publish" succeeds, and the server
// validates a publish exactly as it validates a draft.
(() => {
  const $ = (sel, root = document) => root.querySelector(sel);

  function el(tag, props = {}, ...kids) {
    const n = document.createElement(tag);
    for (const [k, v] of Object.entries(props)) {
      if (k === 'class') n.className = v;
      else if (k === 'text') n.textContent = v;
      else if (k === 'value' || k === 'checked' || k === 'disabled' || k === 'hidden') n[k] = v;
      else n.setAttribute(k, v);
    }
    for (const kid of kids) if (kid !== null && kid !== undefined) n.append(kid);
    return n;
  }

  // ---- API ----------------------------------------------------------------

  class ApiError extends Error {
    constructor(status, code, message, problems) {
      super(message);
      this.status = status;
      this.code = code;
      this.problems = problems || [];
    }
  }

  function friendly(status, e) {
    switch (status) {
      case 401: return 'Not signed in: the service requires a client certificate.';
      case 403: return (e && e.code === 'forbidden') ? 'Your certificate may look at the catalog but not publish to it.' : 'You are not allowed to do this.';
      case 502: case 503: case 504: return 'The service is temporarily unavailable. Try again in a moment.';
    }
    return (e && e.message) || `The request failed (${status}).`;
  }

  async function api(method, path, body) {
    const headers = { Accept: 'application/json' };
    let payload;
    if (body !== undefined) {
      headers['Content-Type'] = 'application/json';
      payload = JSON.stringify(body);
    }
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
      throw new ApiError(res.status, e.code || 'error', friendly(res.status, e), e.problems);
    }
    return data;
  }

  // ---- state --------------------------------------------------------------

  let schema = null;
  let me = { principal: '', canPublish: false };
  let versions = [];
  let latest = null;            // newest published document (may be future-dated)
  let draft = emptyDoc();
  let dirty = false;
  let validated = false;        // the current draft passed validation
  const STORE_KEY = 'meridian-catalog-draft';

  function emptyDoc() { return { effectiveFrom: '', note: '', plans: [], offerings: [] }; }
  const clone = (x) => JSON.parse(JSON.stringify(x));

  function normalise(doc) {
    const d = clone(doc || emptyDoc());
    d.plans = d.plans || [];
    d.offerings = d.offerings || [];
    for (const p of d.plans) { p.filters = p.filters || []; p.rules = p.rules || []; }
    for (const o of d.offerings) o.allowances = o.allowances || [];
    return d;
  }

  // The browser may refuse storage (private windows, blocked cookies); the
  // editor works without it, the draft just does not survive a reload.
  function saveDraft() {
    try {
      sessionStorage.setItem(STORE_KEY, JSON.stringify({ base: latest ? latest.version : 0, draft }));
    } catch (_) { /* no storage */ }
  }
  function loadSavedDraft() {
    try {
      const s = JSON.parse(sessionStorage.getItem(STORE_KEY) || 'null');
      if (s && s.base === (latest ? latest.version : 0) && s.draft) return normalise(s.draft);
    } catch (_) { /* ignore */ }
    return null;
  }
  function clearSavedDraft() { try { sessionStorage.removeItem(STORE_KEY); } catch (_) { /* ignore */ } }

  // canon renders a value with sorted keys and without the zero values the API
  // leaves out (omitempty), so an untouched object compares equal to its
  // published form however the editor happened to build it.
  function canon(x) {
    if (Array.isArray(x)) return '[' + x.map(canon).join(',') + ']';
    if (x && typeof x === 'object') {
      const parts = [];
      for (const k of Object.keys(x).sort()) {
        const v = x[k];
        if (v === '' || v === 0 || v === false || v === null || v === undefined) continue;
        if (Array.isArray(v) && v.length === 0) continue;
        parts.push(JSON.stringify(k) + ':' + canon(v));
      }
      return '{' + parts.join(',') + '}';
    }
    return JSON.stringify(x);
  }

  function touch() {
    dirty = canon(strip(draft)) !== canon(strip(latest || emptyDoc()));
    validated = false;
    // A verdict or price comparison belongs to the draft it was made for.
    $('#validation').replaceChildren();
    $('#dry-run-result').replaceChildren();
    saveDraft();
    renderAll();
  }

  // strip removes what is not part of the content, for comparison.
  function strip(doc) {
    const d = normalise(doc);
    return { plans: d.plans, offerings: d.offerings };
  }

  // ---- tabs ---------------------------------------------------------------

  const tabs = ['offerings', 'plans', 'publish', 'history', 'json'].map((n) => $('#tab-' + n));
  function showTab(tab) {
    for (const t of tabs) {
      const on = t === tab;
      t.setAttribute('aria-selected', String(on));
      t.tabIndex = on ? 0 : -1;
      $('#' + t.getAttribute('aria-controls')).hidden = !on;
    }
    if (tab.id === 'tab-json') resetJSON();
    if (tab.id === 'tab-publish') renderPublish();
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

  // ---- form helpers -------------------------------------------------------

  // bound input: edits obj[key] as the user types. kind: text | int | num
  function bound(obj, key, kind, attrs = {}) {
    const raw = obj[key];
    const input = el('input', Object.assign({ type: kind === 'text' ? 'text' : 'number', autocomplete: 'off' }, attrs));
    input.value = raw === undefined || raw === null ? '' : String(raw);
    input.addEventListener('input', () => {
      if (kind === 'text') obj[key] = input.value;
      else obj[key] = input.value === '' ? '' : Number(input.value);
    });
    return input;
  }

  function field(label, control, cls = '') {
    return el('label', { class: 'field ' + cls }, label, control);
  }

  function select(obj, key, options, withEmpty) {
    const s = el('select');
    if (withEmpty) s.append(el('option', { value: '', text: '(choose)' }));
    for (const o of options) s.append(el('option', { value: o.value, text: o.label }));
    s.value = obj[key] || '';
    s.addEventListener('change', () => { obj[key] = s.value; });
    return s;
  }

  function formatUnits(unit, n) {
    if (unit === 'bytes') {
      const u = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
      let v = n, i = 0;
      while (Math.abs(v) >= 1024 && i < u.length - 1) { v /= 1024; i++; }
      return `${i === 0 ? v : v.toFixed(2)} ${u[i]}`;
    }
    if (unit === 'seconds') return `${n} s`;
    if (unit === 'events') return `${n} events`;
    return String(n);
  }

  const price = (p) => (p ? `${p.num}/${p.den}` : '');

  // ---- changes vs published ----------------------------------------------

  function diffList(kind, now, was) {
    const before = new Map(was.map((x) => [x.id, canon(x)]));
    const after = new Map(now.map((x) => [x.id, canon(x)]));
    const out = [];
    for (const [id, j] of after) {
      if (!before.has(id)) out.push({ kind, id, change: 'new' });
      else if (before.get(id) !== j) out.push({ kind, id, change: 'changed' });
    }
    for (const id of before.keys()) if (!after.has(id)) out.push({ kind, id, change: 'removed' });
    return out;
  }

  function changes() {
    const was = latest ? normalise(latest) : emptyDoc();
    return [
      ...diffList('offering', normalise(draft).offerings, was.offerings),
      ...diffList('plan', normalise(draft).plans, was.plans),
    ];
  }

  function changeOf(kind, id) {
    const c = changes().find((x) => x.kind === kind && x.id === id);
    return c ? c.change : '';
  }

  // ---- offerings ----------------------------------------------------------

  let editingOffering = null; // { index, obj }

  function renderOfferings() {
    const t = $('#offering-table');
    t.replaceChildren();
    const head = el('tr');
    for (const h of ['Offering', 'Price plan', 'Allowances', '']) head.append(el('th', { text: h }));
    t.append(el('thead', {}, head));
    const body = el('tbody');
    for (const [i, o] of draft.offerings.entries()) {
      const tr = el('tr');
      const c = changeOf('offering', o.id);
      const name = el('td', { text: o.name ? `${o.name} (${o.id})` : o.id });
      if (c) name.append(' ', el('span', { class: 'tag ' + c, text: c }));
      tr.append(name, el('td', { text: o.planId || '' }),
        el('td', { text: o.allowances.map((a) => `${a.id}: ${formatUnits(a.unit, a.amount)}`).join(', ') || 'none' }));
      const act = el('td');
      const edit = el('button', { class: 'small', type: 'button', text: 'Edit', 'aria-label': `Edit offering ${o.id}` });
      edit.addEventListener('click', () => openOffering(i));
      const del = el('button', { class: 'small danger', type: 'button', text: 'Remove', 'aria-label': `Remove offering ${o.id}` });
      del.addEventListener('click', () => { draft.offerings.splice(i, 1); editingOffering = null; touch(); });
      act.append(edit, ' ', del);
      tr.append(act);
      body.append(tr);
    }
    if (!draft.offerings.length) body.append(el('tr', {}, el('td', { colspan: 4, class: 'muted', text: 'No offerings yet.' })));
    t.append(body);
    renderOfferingEditor();
  }

  function openOffering(index) {
    const obj = index === null
      ? { id: '', name: '', planId: draft.plans.length ? draft.plans[0].id : '', allowances: [] }
      : clone(draft.offerings[index]);
    editingOffering = { index, obj };
    renderOfferingEditor();
    const first = $('#o-id');
    if (first && !first.readOnly) first.focus();
  }

  function renderOfferingEditor() {
    const host = $('#offering-editor');
    host.replaceChildren();
    if (!editingOffering) return;
    const { index, obj } = editingOffering;
    const card = el('form', { class: 'card', id: 'offering-form', novalidate: '' });
    card.append(el('h2', { text: index === null ? 'New offering' : `Edit offering ${obj.id}` }));
    const row = el('div', { class: 'row' });
    const id = bound(obj, 'id', 'text', { id: 'o-id', maxlength: 64, required: '' });
    if (index !== null) { id.readOnly = true; }
    row.append(field('Identifier', id, 'narrow'), field('Name', bound(obj, 'name', 'text', { id: 'o-name' }), 'grow'));
    const plans = draft.plans.map((p) => ({ value: p.id, label: p.name ? `${p.name} (${p.id})` : p.id }));
    const planSel = select(obj, 'planId', plans, true);
    planSel.id = 'o-plan';
    row.append(field('Price plan', planSel, 'grow'));
    card.append(row);
    if (!draft.plans.length) card.append(el('p', { class: 'hint', text: 'Create a price plan first; an offering needs one.' }));

    const fs = el('fieldset');
    fs.append(el('legend', { text: 'Allowances created when the offering is attached' }));
    const list = el('div', { id: 'allowance-list' });
    const draw = () => {
      list.replaceChildren();
      obj.allowances.forEach((a, i) => {
        const r = el('div', { class: 'sub' });
        const unit = select(a, 'unit', schema.units.map((u) => ({ value: u, label: u })), true);
        const cls = select(a, 'class', schema.allowanceClasses.map((u) => ({ value: u, label: u })), true);
        const hint = el('span', { class: 'hint', text: '' });
        const amount = bound(a, 'amount', 'int', { min: 0, step: 1 });
        const show = () => { hint.textContent = a.unit && a.amount !== '' ? formatUnits(a.unit, a.amount) : ''; };
        amount.addEventListener('input', show);
        unit.addEventListener('change', show);
        show();
        const rm = el('button', { class: 'small danger', type: 'button', text: 'Remove', 'aria-label': `Remove allowance ${i + 1}` });
        rm.addEventListener('click', () => { obj.allowances.splice(i, 1); draw(); });
        r.append(field('Bucket id', bound(a, 'id', 'text', { maxlength: 64 })), field('Class', cls), field('Unit', unit),
          field('Amount', amount), el('div', {}, hint), field('Valid for (days, 0 = forever)', bound(a, 'validityDays', 'int', { min: 0, step: 1 })),
          field('Use order', bound(a, 'order', 'num', { step: 'any' })), el('div', {}, rm));
        list.append(r);
      });
      if (!obj.allowances.length) list.append(el('p', { class: 'hint', text: 'No allowances: the subscriber pays from cash only.' }));
    };
    draw();
    fs.append(list);
    const add = el('button', { type: 'button', id: 'add-allowance', text: 'Add allowance' });
    add.addEventListener('click', () => {
      obj.allowances.push({ id: '', class: 'bundle', unit: 'bytes', amount: 0, validityDays: 0, order: 0 });
      draw();
    });
    fs.append(el('div', { class: 'actions' }, add));
    card.append(fs);

    const err = el('p', { class: 'error', role: 'alert', hidden: true, id: 'o-error' });
    const apply = el('button', { class: 'primary', type: 'submit', id: 'o-apply', text: index === null ? 'Add to draft' : 'Update draft' });
    const cancel = el('button', { type: 'button', text: 'Cancel' });
    cancel.addEventListener('click', () => { editingOffering = null; renderOfferingEditor(); });
    card.append(err, el('div', { class: 'actions' }, apply, cancel));
    card.addEventListener('submit', (ev) => {
      ev.preventDefault();
      const o = normaliseOffering(obj);
      let msg = '';
      if (!o.id) msg = 'Give the offering an identifier.';
      else if (index === null && draft.offerings.some((x) => x.id === o.id)) msg = `An offering ${o.id} already exists.`;
      else if (!o.planId) msg = 'Choose a price plan.';
      if (msg) { err.textContent = msg; err.hidden = false; return; }
      if (index === null) draft.offerings.push(o); else draft.offerings[index] = o;
      editingOffering = null;
      touch();
    });
    host.append(card);
  }

  function normaliseOffering(o) {
    const out = { id: (o.id || '').trim(), name: (o.name || '').trim(), planId: o.planId || '', allowances: [] };
    for (const a of o.allowances) {
      out.allowances.push({
        id: (a.id || '').trim(), class: a.class || '', unit: a.unit || '',
        amount: Number(a.amount) || 0, validityDays: Number(a.validityDays) || 0, order: Number(a.order) || 0,
      });
    }
    return out;
  }

  // ---- price plans --------------------------------------------------------

  let editingPlan = null; // { index, obj }

  function renderPlans() {
    const t = $('#plan-table');
    t.replaceChildren();
    const head = el('tr');
    for (const h of ['Plan', 'Currency', 'Filters', 'Rules', '']) head.append(el('th', { text: h }));
    t.append(el('thead', {}, head));
    const body = el('tbody');
    for (const [i, p] of draft.plans.entries()) {
      const tr = el('tr');
      const c = changeOf('plan', p.id);
      const name = el('td', { text: p.name ? `${p.name} (${p.id})` : p.id });
      if (c) name.append(' ', el('span', { class: 'tag ' + c, text: c }));
      tr.append(name, el('td', { text: p.currency || '' }), el('td', { class: 'num', text: String(p.filters.length) }),
        el('td', { class: 'num', text: String(p.rules.length) }));
      const act = el('td');
      const edit = el('button', { class: 'small', type: 'button', text: 'Edit', 'aria-label': `Edit plan ${p.id}` });
      edit.addEventListener('click', () => openPlan(i));
      const del = el('button', { class: 'small danger', type: 'button', text: 'Remove', 'aria-label': `Remove plan ${p.id}` });
      del.addEventListener('click', () => { draft.plans.splice(i, 1); editingPlan = null; touch(); });
      act.append(edit, ' ', del);
      tr.append(act);
      body.append(tr);
    }
    if (!draft.plans.length) body.append(el('tr', {}, el('td', { colspan: 5, class: 'muted', text: 'No price plans yet.' })));
    t.append(body);
    renderPlanEditor();
  }

  function openPlan(index) {
    const obj = index === null
      ? { id: '', name: '', currency: '', timeZone: '', filters: [], rules: [{ id: 'default', priority: 0, when: '', classes: [], cash: { num: '', den: 1 } }] }
      : clone(draft.plans[index]);
    for (const r of obj.rules) {
      r.classes = r.classes || [];
      r.cash = r.cash || { num: '', den: '' };
    }
    editingPlan = { index, obj };
    renderPlanEditor();
    const first = $('#p-id');
    if (first && !first.readOnly) first.focus();
  }

  function renderPlanEditor() {
    const host = $('#plan-editor');
    host.replaceChildren();
    if (!editingPlan) return;
    const { index, obj } = editingPlan;
    const card = el('form', { class: 'card', id: 'plan-form', novalidate: '' });
    card.append(el('h2', { text: index === null ? 'New price plan' : `Edit price plan ${obj.id}` }));
    const row = el('div', { class: 'row' });
    const id = bound(obj, 'id', 'text', { id: 'p-id', maxlength: 64, required: '' });
    if (index !== null) id.readOnly = true;
    row.append(field('Identifier', id, 'narrow'), field('Name', bound(obj, 'name', 'text'), 'grow'),
      field('Currency (ISO 4217)', bound(obj, 'currency', 'text', { maxlength: 3, placeholder: 'EUR' }), 'narrow'),
      field('Time zone (IANA, empty = UTC)', bound(obj, 'timeZone', 'text', { placeholder: 'Europe/Berlin' }), 'narrow'));
    card.append(row);

    // filters
    const fsF = el('fieldset');
    fsF.append(el('legend', { text: 'Policy filters (bar usage before pricing)' }));
    const fl = el('div', { id: 'filter-list' });
    const drawF = () => {
      fl.replaceChildren();
      obj.filters.forEach((f, i) => {
        const r = el('div', { class: 'sub' });
        const rm = el('button', { class: 'small danger', type: 'button', text: 'Remove', 'aria-label': `Remove filter ${i + 1}` });
        rm.addEventListener('click', () => { obj.filters.splice(i, 1); drawF(); });
        r.append(field('Filter id', bound(f, 'id', 'text', { maxlength: 64 })),
          field('Bar when', bound(f, 'when', 'text', { placeholder: 'rat == "NR"' }), 'wide'),
          field('Action', select(f, 'action', schema.filterActions.map((a) => ({ value: a, label: a })))), el('div', {}, rm));
        fl.append(r);
      });
      if (!obj.filters.length) fl.append(el('p', { class: 'hint', text: 'No filters: nothing is barred.' }));
    };
    drawF();
    const addF = el('button', { type: 'button', id: 'add-filter', text: 'Add filter' });
    addF.addEventListener('click', () => { obj.filters.push({ id: '', when: '', action: 'deny' }); drawF(); });
    fsF.append(fl, el('div', { class: 'actions' }, addF));
    card.append(fsF);

    // rules
    const fsR = el('fieldset');
    fsR.append(el('legend', { text: 'Charge rules (highest priority first, then in order)' }));
    const rl = el('div', { id: 'rule-list' });
    const drawR = () => {
      rl.replaceChildren();
      obj.rules.forEach((r, i) => {
        const wrap = el('div', { class: 'sub' });
        const checks = el('div', { class: 'checks', role: 'group', 'aria-label': 'Bucket classes that may pay' });
        for (const c of schema.classes) {
          const cb = el('input', { type: 'checkbox', checked: r.classes.includes(c) });
          cb.addEventListener('change', () => {
            r.classes = schema.classes.filter((k) => (k === c ? cb.checked : r.classes.includes(k)));
          });
          checks.append(el('label', {}, cb, el('span', { text: c })));
        }
        const up = el('button', { class: 'small', type: 'button', text: '↑', 'aria-label': `Move rule ${r.id || i + 1} up` });
        up.addEventListener('click', () => { if (i > 0) { [obj.rules[i - 1], obj.rules[i]] = [obj.rules[i], obj.rules[i - 1]]; drawR(); } });
        const down = el('button', { class: 'small', type: 'button', text: '↓', 'aria-label': `Move rule ${r.id || i + 1} down` });
        down.addEventListener('click', () => { if (i < obj.rules.length - 1) { [obj.rules[i + 1], obj.rules[i]] = [obj.rules[i], obj.rules[i + 1]]; drawR(); } });
        const rm = el('button', { class: 'small danger', type: 'button', text: 'Remove', 'aria-label': `Remove rule ${r.id || i + 1}` });
        rm.addEventListener('click', () => { obj.rules.splice(i, 1); drawR(); });
        wrap.append(field('Rule id', bound(r, 'id', 'text', { maxlength: 64 })),
          field('Priority', bound(r, 'priority', 'int', { step: 1 })),
          field('Applies when (empty = always)', bound(r, 'when', 'text', { placeholder: 'rat in ["NR","LTE"] && hour >= 22' }), 'wide'),
          field('Classes (none = all)', checks),
          field('Cash price: numerator', bound(r.cash, 'num', 'int', { min: 0, step: 1 })),
          field('denominator', bound(r.cash, 'den', 'int', { min: 1, step: 1 })),
          el('div', {}, up, ' ', down, ' ', rm));
        rl.append(wrap);
      });
      if (!obj.rules.length) rl.append(el('p', { class: 'hint', text: 'A plan needs at least one rule.' }));
    };
    drawR();
    const addR = el('button', { type: 'button', id: 'add-rule', text: 'Add rule' });
    addR.addEventListener('click', () => {
      obj.rules.push({ id: '', priority: 0, when: '', classes: [], cash: { num: '', den: 1 } });
      drawR();
    });
    fsR.append(rl, el('div', { class: 'actions' }, addR),
      el('p', { class: 'hint', text: `Conditions use: ${schema.variables.map((v) => v.name).join(', ')}. Example: ${schema.example}` }));
    card.append(fsR);

    const err = el('p', { class: 'error', role: 'alert', hidden: true, id: 'p-error' });
    const apply = el('button', { class: 'primary', type: 'submit', id: 'p-apply', text: index === null ? 'Add to draft' : 'Update draft' });
    const cancel = el('button', { type: 'button', text: 'Cancel' });
    cancel.addEventListener('click', () => { editingPlan = null; renderPlanEditor(); });
    card.append(err, el('div', { class: 'actions' }, apply, cancel));
    card.addEventListener('submit', (ev) => {
      ev.preventDefault();
      const p = normalisePlan(obj);
      let msg = '';
      if (!p.id) msg = 'Give the plan an identifier.';
      else if (index === null && draft.plans.some((x) => x.id === p.id)) msg = `A plan ${p.id} already exists.`;
      if (msg) { err.textContent = msg; err.hidden = false; return; }
      if (index === null) draft.plans.push(p); else draft.plans[index] = p;
      editingPlan = null;
      touch();
    });
    host.append(card);
  }

  function normalisePlan(p) {
    const out = { id: (p.id || '').trim(), name: (p.name || '').trim(), currency: (p.currency || '').trim().toUpperCase(),
      timeZone: (p.timeZone || '').trim(), filters: [], rules: [] };
    for (const f of p.filters) out.filters.push({ id: (f.id || '').trim(), when: f.when || '', action: f.action || 'deny' });
    for (const r of p.rules) {
      const rule = { id: (r.id || '').trim(), priority: Number(r.priority) || 0, when: r.when || '' };
      if (r.classes.length) rule.classes = r.classes.slice();
      if (r.cash && (r.cash.num !== '' || r.cash.den !== '')) rule.cash = { num: Number(r.cash.num) || 0, den: Number(r.cash.den) || 0 };
      out.rules.push(rule);
    }
    for (const k of ['name', 'currency', 'timeZone']) if (!out[k]) delete out[k];
    for (const f of out.filters) if (!f.when) f.when = '';
    return out;
  }

  // ---- review & publish --------------------------------------------------

  function pad(n) { return String(n).padStart(2, '0'); }
  function toLocalInput(d) {
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
  }

  function suggestEffective() {
    let t = new Date(Date.now() + 3600e3);
    t.setSeconds(0, 0);
    if (latest && new Date(latest.effectiveFrom) >= t) t = new Date(new Date(latest.effectiveFrom).getTime() + 60e3);
    return toLocalInput(t);
  }

  function renderChanges() {
    const host = $('#changes');
    host.replaceChildren();
    const cs = changes();
    if (!cs.length) { host.append(el('p', { class: 'hint', text: 'No changes: the draft matches the newest published version.' })); return; }
    const ul = el('ul', { class: 'problems' });
    for (const c of cs) ul.append(el('li', {}, el('span', { class: 'tag ' + c.change, text: c.change }), ` ${c.kind} `, el('span', { class: 'mono', text: c.id })));
    host.append(ul);
  }

  function renderPublish() {
    renderChanges();
    const eff = $('#effective');
    if (!eff.value) eff.value = suggestEffective();
    const next = (latest ? latest.version : 0) + 1;
    $('#effective-hint').textContent = latest
      ? `Newest published version is ${latest.version}, effective ${new Date(latest.effectiveFrom).toLocaleString()}. Yours becomes version ${next} and must start later than that.`
      : 'No version has been published yet. Yours becomes version 1.';
    const can = me.canPublish && dirty && validated && $('#confirm-publish').checked;
    $('#publish').disabled = !can;
    $('#publish-hint').textContent = !me.canPublish
      ? 'Your certificate is not on the publisher list: you can edit and test a draft but not publish it.'
      : !dirty ? 'There is nothing to publish.'
      : !validated ? 'Validate the draft first.'
      : 'Publishing is atomic: every rating node switches at the effective time, with no restart.';
  }

  function documentToSend() {
    const d = normalise(draft);
    const v = $('#effective').value;
    return {
      effectiveFrom: v ? new Date(v).toISOString() : '',
      note: $('#note').value.trim(),
      plans: d.plans,
      offerings: d.offerings,
    };
  }

  function problemList(host, probs, cls) {
    const ul = el('ul', { class: 'problems ' + (cls || '') });
    for (const p of probs) {
      ul.append(el('li', {}, p.path ? el('span', { class: 'path', text: p.path + ': ' }) : null, p.message));
    }
    host.append(ul);
  }

  async function validate() {
    const host = $('#validation');
    host.replaceChildren();
    $('#publish-ok').hidden = true;
    if (!$('#effective').value) { host.append(el('p', { class: 'error', text: 'Choose when the version should take effect.' })); return; }
    try {
      const res = await api('POST', '/v1/catalog/validate', documentToSend());
      validated = res.valid;
      if (res.valid) host.append(el('p', { class: 'ok-text', text: 'The draft is valid.' }));
      else {
        host.append(el('p', { class: 'error', text: `${res.errors.length} problem(s) to fix:` }));
        problemList(host, res.errors, 'error');
      }
      if (res.warnings.length) {
        host.append(el('p', { class: 'hint', text: `${res.warnings.length} warning(s):` }));
        problemList(host, res.warnings);
      }
    } catch (e) {
      validated = false;
      host.append(el('p', { class: 'error', text: e.message }));
    }
    renderPublish();
  }

  // ---- dry-run ------------------------------------------------------------

  let cases = [];
  function newCase() {
    return { name: '', planId: draft.plans.length ? draft.plans[0].id : '', rat: 'LTE', unit: 'bytes', cell: '', ratingGroup: '', at: '' };
  }
  function renderCases() {
    const host = $('#cases');
    host.replaceChildren();
    cases.forEach((c, i) => {
      const r = el('div', { class: 'sub' });
      const plans = draft.plans.map((p) => ({ value: p.id, label: p.id }));
      const rm = el('button', { class: 'small danger', type: 'button', text: 'Remove', 'aria-label': `Remove case ${i + 1}` });
      rm.addEventListener('click', () => { cases.splice(i, 1); renderCases(); });
      const at = bound(c, 'at', 'text', { type: 'datetime-local' });
      r.append(field('Plan', select(c, 'planId', plans, true)), field('RAT', bound(c, 'rat', 'text', { placeholder: 'LTE' })),
        field('Unit', select(c, 'unit', schema.units.map((u) => ({ value: u, label: u })))),
        field('Cell', bound(c, 'cell', 'text')), field('Rating group', bound(c, 'ratingGroup', 'int', { min: 0 })),
        field('At (empty = now)', at), el('div', {}, rm));
      host.append(r);
    });
  }

  function outcome(o) {
    if (o.error) return o.error === 'barred' ? `barred by ${o.filterId}` : o.error;
    return (o.rates || []).map((r) => `${r.class} ${r.num}/${r.den}`).join(', ') + (o.ruleId ? ` (rule ${o.ruleId})` : '');
  }

  async function runCases() {
    const host = $('#dry-run-result');
    host.replaceChildren();
    if (!$('#effective').value) { host.append(el('p', { class: 'error', text: 'Choose the effective time above first.' })); return; }
    const payload = cases.map((c) => {
      const o = { name: c.name || undefined, planId: c.planId, rat: c.rat, unit: c.unit, cell: c.cell || undefined };
      if (c.ratingGroup !== '') o.ratingGroup = Number(c.ratingGroup);
      if (c.at) o.at = new Date(c.at).toISOString();
      return o;
    });
    try {
      const res = await api('POST', '/v1/catalog/dry-run', { document: documentToSend(), cases: payload });
      const t = el('table', { id: 'dry-run-table' });
      t.append(el('thead', {}, el('tr', {}, ...['Plan', 'Usage', 'Now', 'With your draft', ''].map((h) => el('th', { text: h })))));
      const body = el('tbody');
      for (const r of res.results) {
        const c = r.case;
        body.append(el('tr', { class: r.changed ? 'changed' : '' },
          el('td', { text: c.planId }),
          el('td', { text: [c.rat, c.unit || 'bytes', c.cell, c.ratingGroup ? `rg ${c.ratingGroup}` : ''].filter(Boolean).join(' ') }),
          el('td', { class: 'mono', text: outcome(r.live) }),
          el('td', { class: 'mono', text: outcome(r.proposed) }),
          el('td', { text: r.changed ? 'changed' : 'same' })));
      }
      t.append(body);
      host.append(el('div', { class: 'table-scroll' }, t));
      if (res.warnings && res.warnings.length) problemList(host, res.warnings);
    } catch (e) {
      host.append(el('p', { class: 'error', text: e.message }));
      if (e.problems.length) problemList(host, e.problems, 'error');
    }
  }

  async function publish() {
    const err = $('#publish-error'), ok = $('#publish-ok');
    err.hidden = true; ok.hidden = true;
    err.replaceChildren();
    $('#publish').disabled = true;
    try {
      const res = await api('POST', '/v1/catalog/versions', {
        document: documentToSend(),
        expectedVersion: (latest ? latest.version : 0) + 1,
      });
      ok.textContent = `Published as version ${res.version}, effective ${new Date(res.effectiveFrom).toLocaleString()}.`;
      ok.hidden = false;
      clearSavedDraft();
      $('#confirm-publish').checked = false;
      $('#effective').value = '';
      $('#note').value = '';
      $('#validation').replaceChildren();
      await reload(true);
    } catch (e) {
      err.replaceChildren(el('p', { text: e.code === 'version_conflict'
        ? 'Someone else published a newer version while you were editing. Copy your draft from the JSON tab, reload this page to start from their version, then load it again.'
        : e.message }));
      err.hidden = false;
      if (e.problems.length) problemList(err, e.problems, 'error');
      renderPublish();
    }
  }

  // ---- history ------------------------------------------------------------

  function renderHistory() {
    const t = $('#version-table');
    t.replaceChildren();
    t.append(el('thead', {}, el('tr', {}, ...['Version', 'Effective from', 'Note', ''].map((h) => el('th', { text: h })))));
    const body = el('tbody');
    for (const v of versions.slice().reverse()) {
      const tr = el('tr');
      const ver = el('td', { text: `v${v.version}` });
      if (v.inEffect) ver.append(' ', el('span', { class: 'tag new', text: 'in effect' }));
      else if (new Date(v.effectiveFrom) > new Date()) ver.append(' ', el('span', { class: 'tag changed', text: 'scheduled' }));
      const act = el('td');
      const b = el('button', { class: 'small', type: 'button', text: 'Start a draft from this', 'aria-label': `Start a draft from version ${v.version}` });
      b.addEventListener('click', async () => {
        try {
          draft = normalise(await api('GET', `/v1/catalog?version=${v.version}`));
          touch();
          showTab($('#tab-publish'));
        } catch (e) { $('#who').textContent = e.message; }
      });
      act.append(b);
      tr.append(ver, el('td', { text: new Date(v.effectiveFrom).toLocaleString() }), el('td', { text: v.note || '' }), act);
      body.append(tr);
    }
    if (!versions.length) body.append(el('tr', {}, el('td', { colspan: 4, class: 'muted', text: 'Nothing has been published yet.' })));
    t.append(body);
  }

  // ---- JSON ---------------------------------------------------------------

  function resetJSON() { $('#json-text').value = JSON.stringify(strip(draft), null, 2); $('#json-error').hidden = true; }

  function loadJSON() {
    const err = $('#json-error');
    err.hidden = true;
    let d;
    try { d = JSON.parse($('#json-text').value); } catch (e) { err.textContent = `Not valid JSON: ${e.message}`; err.hidden = false; return; }
    if (!d || typeof d !== 'object' || Array.isArray(d)) { err.textContent = 'Expected an object with "plans" and "offerings".'; err.hidden = false; return; }
    for (const k of ['plans', 'offerings']) {
      if (d[k] !== undefined && !Array.isArray(d[k])) { err.textContent = `"${k}" must be a list.`; err.hidden = false; return; }
    }
    draft = normalise({ plans: d.plans, offerings: d.offerings });
    editingOffering = null; editingPlan = null;
    touch();
  }

  // ---- wiring -------------------------------------------------------------

  function renderBanner() {
    const b = $('#draft-banner');
    b.hidden = !dirty;
    b.textContent = dirty ? `You have unpublished changes (${changes().length}). They affect nothing until published on the Review & publish tab.` : '';
  }

  function renderAll() {
    renderBanner();
    renderOfferings();
    renderPlans();
    renderHistory();
    if (!$('#panel-publish').hidden) renderPublish();
  }

  async function reload(keepDraftIfClean) {
    versions = await api('GET', '/v1/catalog/versions');
    latest = versions.length ? normalise(await api('GET', `/v1/catalog?version=${versions[versions.length - 1].version}`)) : null;
    if (latest) latest.version = versions[versions.length - 1].version;
    if (latest) latest.effectiveFrom = versions[versions.length - 1].effectiveFrom;
    if (keepDraftIfClean || !dirty) draft = normalise(latest || emptyDoc());
    dirty = false;
    validated = false;
    renderAll();
  }

  async function init() {
    try {
      [schema, me] = await Promise.all([api('GET', '/v1/catalog/schema'), api('GET', '/v1/catalog/me')]);
      await reload(true);
    } catch (e) {
      $('#who').textContent = e.message;
      return;
    }
    const saved = loadSavedDraft();
    if (saved) { draft = saved; touch(); }
    const cur = versions.find((v) => v.inEffect);
    $('#who').textContent = `Signed in as ${me.principal || 'unknown'} · ${me.canPublish ? 'may publish' : 'read-only (cannot publish)'} · ` +
      (cur ? `version ${cur.version} in effect` : 'no version in effect');
  }

  $('#new-offering').addEventListener('click', () => openOffering(null));
  $('#new-plan').addEventListener('click', () => openPlan(null));
  $('#validate').addEventListener('click', validate);
  $('#dry-run-open').addEventListener('click', () => {
    $('#dry-run-card').hidden = false;
    if (!cases.length) cases.push(newCase());
    renderCases();
  });
  $('#add-case').addEventListener('click', () => { cases.push(newCase()); renderCases(); });
  $('#run-cases').addEventListener('click', runCases);
  $('#confirm-publish').addEventListener('change', renderPublish);
  $('#effective').addEventListener('input', () => { validated = false; renderPublish(); });
  $('#note').addEventListener('input', () => { validated = false; renderPublish(); });
  $('#publish').addEventListener('click', publish);
  $('#discard').addEventListener('click', () => {
    draft = normalise(latest || emptyDoc());
    editingOffering = null; editingPlan = null;
    clearSavedDraft();
    dirty = false; validated = false;
    $('#validation').replaceChildren();
    renderAll();
  });
  $('#json-load').addEventListener('click', loadJSON);
  $('#json-reset').addEventListener('click', resetJSON);

  init();
})();
