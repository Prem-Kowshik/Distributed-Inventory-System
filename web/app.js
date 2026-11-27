/* =============================================================================
   Distributed Inventory DBMS - live demo console
   Everything on screen comes from the real cluster:
     /api/state  -> router STATUS + which OS processes are alive  (polled)
     /api/logs   -> incremental tail of logs/<id>.log              (polled)
     /api/cmd    -> any client command, forwarded to the router
     /api/proc   -> start / kill -9 / corrupt-WAL a process
     /api/bench  -> N concurrent requests (server side)
   Packet animations are driven by observed LSN / read-counter deltas, so they show
   real traffic (also from loadgen and benchmarks), not a canned animation.
   ============================================================================= */
'use strict';

const WH = ['MUMBAI', 'PUNE', 'DELHI'];
const NODES = { MUMBAI: ['m1', 'm2', 'm3'], PUNE: ['p1', 'p2', 'p3'], DELHI: ['d1', 'd2', 'd3'] };
const ALL = [...NODES.MUMBAI, ...NODES.PUNE, ...NODES.DELHI];
const WH_OF = {}; WH.forEach(w => NODES[w].forEach(n => WH_OF[n] = w));
const COLOR = { MUMBAI: '#4cc9f0', PUNE: '#6ee7a8', DELHI: '#ffc15e', router: '#b693ff' };
const C = { ok: '#34d399', bad: '#fb6f7f', warn: '#fbbf24', info: '#7dd3fc', tpc: '#b693ff', gray: '#5b6680' };

const $ = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => [...r.querySelectorAll(s)];
const sleep = ms => new Promise(r => setTimeout(r, ms));
const esc = s => String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

function h(tag, attrs = {}, ...kids) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v == null || v === false) continue;
    if (k === 'class') el.className = v;
    else if (k === 'html') el.innerHTML = v;
    else if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
    else if (k === 'style' && typeof v === 'object') Object.assign(el.style, v);
    else el.setAttribute(k, v);
  }
  for (const k of kids.flat()) if (k != null && k !== false) el.append(k.nodeType ? k : document.createTextNode(k));
  return el;
}
const svg = (tag, attrs = {}) => {
  const el = document.createElementNS('http://www.w3.org/2000/svg', tag);
  for (const [k, v] of Object.entries(attrs)) if (v != null) el.setAttribute(k, v);
  return el;
};

const ICON = {
  kill: '<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"><path d="M8 1.8v5.4"/><path d="M4.4 3.9a5.2 5.2 0 1 0 7.2 0"/></svg>',
  start: '<svg viewBox="0 0 16 16" fill="currentColor"><path d="M4.5 2.8v10.4L13 8z"/></svg>',
  more: '<svg viewBox="0 0 16 16" fill="currentColor"><circle cx="3.5" cy="8" r="1.4"/><circle cx="8" cy="8" r="1.4"/><circle cx="12.5" cy="8" r="1.4"/></svg>',
  crown: '<svg class="crown" viewBox="0 0 16 16" fill="currentColor"><path d="M1.5 4.5l3.2 2.8L8 2.5l3.3 4.8 3.2-2.8-1.3 7.5H2.8z"/></svg>',
};

// ============================================================================ API
async function api(path, opts) {
  const r = await fetch(path, opts);
  const d = await r.json();
  if (d.error) throw new Error(d.error);
  return d;
}
async function run(c) {
  const fpBefore = S.fp;
  const d = await api('/api/cmd', { method: 'POST', body: c });
  onCommandResult(c, d.resp, fpBefore);
  kick();
  return d;
}
const proc = async (action, id) => { const d = await api(`/api/proc?action=${action}&id=${id}`, { method: 'POST' }); onProc(action, id); kick(); return d.msg; };
const cluster = async action => { const d = await api(`/api/cluster?action=${action}`, { method: 'POST' }); kick(); return d.msg; };
const bench = (n, t, c) => api(`/api/bench?n=${n}&t=${t}&cmd=${encodeURIComponent(c)}`, { method: 'POST' });
const logs = (id, from = -1) => api(`/api/logs?id=${id}&from=${from}`);

// ============================================================================ state
const S = {
  routerUp: false, status: null, mode: 'sync', lb: 'rr', fp: 'none',
  nodes: {}, primary: {}, procs: {}, prev: null,
  delay: {}, nodeFp: {}, lastKilled: {}, everUp: false,
};
ALL.forEach(id => S.nodes[id] = { id, wh: WH_OF[id], up: false, role: '?', epoch: '-', lsn: 0, follows: '-', doubt: 0, served: 0 });

function parseStatus(txt) {
  const lines = txt.split('\n');
  const m = lines[0].match(/replication=(\w+) \| lb=(\w+) \| router failpoint=(\S+)/);
  if (m) { S.mode = m[1]; S.lb = m[2]; S.fp = m[3]; }
  for (const l of lines.slice(2)) {
    const t = l.trim().split(/\s+/);
    if (!WH.includes(t[0])) continue;
    const isP = t[1].endsWith('*'), id = t[1].replace('*', '');
    const n = S.nodes[id]; if (!n) continue;
    n.up = t[2] === 'UP';
    if (n.up) { n.role = t[3]; n.epoch = +t[4]; n.lsn = +t[5]; n.follows = t[6]; n.doubt = +t[7]; }
    n.served = +t[8];
    if (isP) S.primary[t[0]] = id;
  }
}

function snapshot() {
  const o = {};
  ALL.forEach(id => { const n = S.nodes[id]; o[id] = { up: n.up, role: n.role, lsn: n.lsn, served: n.served, epoch: n.epoch, proc: S.procs[id] }; });
  o.primary = { ...S.primary }; o.router = S.routerUp;
  return o;
}

let pollTimer = null, polling = false;
async function poll() {
  if (polling) return;
  polling = true;
  try {
    const d = await api('/api/state');
    S.procs = d.procs;
    S.routerUp = d.status != null;
    if (S.routerUp) parseStatus(d.status);
    if (S.routerUp || Object.values(d.procs).some(Boolean)) S.everUp = true;
    const prev = S.prev, cur = snapshot();
    render(prev, cur);
    S.prev = cur;
    TOPIC_TICK.forEach(f => { try { f(); } catch (e) { console.error(e); } });
  } catch (e) {
    setRouterSub('web console not reachable');
  } finally {
    polling = false;
    clearTimeout(pollTimer);
    pollTimer = setTimeout(poll, 650);
  }
}
const kick = () => { clearTimeout(pollTimer); pollTimer = setTimeout(poll, 60); };
const TOPIC_TICK = new Set();

// ============================================================================ topology
const stage = $('#stage');
function buildTopology() {
  const shards = $('#shards');
  for (const wh of WH) {
    const col = h('div', { class: 'shard', 'data-wh': wh, id: 'sh-' + wh },
      h('div', { class: 'shard-head' }, h('span', { class: 'shard-name' }, wh), h('span', { class: 'shard-meta', id: 'shm-' + wh }, '')));
    for (const id of NODES[wh]) {
      col.append(h('div', { class: 'node', id: 'n-' + id, 'data-id': id },
        h('div', { class: 'node-top' },
          h('span', { class: 'status-dot' }),
          h('span', { html: ICON.crown }),
          h('span', { class: 'node-id' }, id),
          h('span', { class: 'role' }, '…'),
          h('div', { class: 'node-acts' },
            h('button', { class: 'icon-btn kill', title: 'kill -9', 'data-act': 'toggle', html: ICON.kill, onclick: e => { e.stopPropagation(); toggleProc(id); } }),
            h('button', { class: 'icon-btn', title: 'more', html: ICON.more, onclick: e => { e.stopPropagation(); nodeMenu(id, e.currentTarget); } }))),
        h('div', { class: 'node-stats' },
          h('span', {}, 'epoch ', h('b', { 'data-k': 'epoch' }, '-')),
          h('span', {}, 'lsn ', h('b', { 'data-k': 'lsn' }, '-')),
          h('span', { style: { textAlign: 'right' } }, 'reads ', h('b', { 'data-k': 'served' }, '0')),
          h('div', { class: 'lsnbar' }, h('i'))),
        h('div', { class: 'node-flags' })));
    }
    shards.append(col);
  }
  // crown inherits shard colour
  $$('.node').forEach(n => n.querySelector('.crown').parentElement.style.display = 'contents');
}

function setRouterSub(t) { $('#router-sub').textContent = t; }

function render(prev, cur) {
  // ---------------------------------------------------------------- router card
  const r = $('#router');
  r.classList.toggle('up', S.routerUp);
  r.classList.toggle('down', !S.routerUp && S.everUp);
  const ra = $('#router-actions');
  const rProc = S.procs.router;
  ra.innerHTML = '';
  ra.append(
    rProc ? h('button', { class: 'icon-btn kill', title: 'kill -9 router', html: ICON.kill, onclick: () => toggleProc('router') })
          : h('button', { class: 'icon-btn start', title: 'start router', html: ICON.start, onclick: () => toggleProc('router') }),
    h('button', { class: 'icon-btn', title: 'more', html: ICON.more, onclick: e => routerMenu(e.currentTarget) }));
  if (S.routerUp) {
    const upN = ALL.filter(id => S.nodes[id].up).length;
    setRouterSub(`:7070 · heartbeat 500ms ×3 · ${upN}/9 nodes up`);
  } else setRouterSub(rProc ? 'starting / recovering coordinator log…' : 'DOWN — no failover, no new transactions');
  $('#tag-mode').textContent = S.mode;
  $('#tag-lb').textContent = 'lb=' + S.lb;
  const fp = $('#tag-fp');
  fp.classList.toggle('hidden', S.fp === 'none' || !S.routerUp);
  fp.textContent = 'FAILPOINT ' + S.fp;
  $$('#seg-mode button').forEach(b => b.classList.toggle('on', b.dataset.v === S.mode));
  $$('#seg-lb button').forEach(b => b.classList.toggle('on', b.dataset.v === S.lb));

  // ---------------------------------------------------------------- empty state
  const anyProc = Object.values(S.procs).some(Boolean);
  $('#empty').classList.toggle('hidden', anyProc || S.routerUp);

  // ---------------------------------------------------------------- nodes
  for (const wh of WH) {
    const p = S.primary[wh];
    const pl = p ? S.nodes[p].lsn : 0;
    const liveN = NODES[wh].filter(id => S.nodes[id].up && S.procs[id]).length;
    $('#shm-' + wh).textContent = S.routerUp ? (liveN ? `epoch ${p ? S.nodes[p].epoch : '-'} · ${liveN}/3 live` : '0/3 live') : `${NODES[wh].filter(id => S.procs[id]).length}/3 processes`;
    $('#sh-' + wh).classList.toggle('unavailable', S.routerUp && S.everUp && NODES[wh].every(id => !S.nodes[id].up));
    const maxL = Math.max(1, ...NODES[wh].map(id => S.nodes[id].lsn || 0));
    for (const id of NODES[wh]) {
      const n = S.nodes[id], el = $('#n-' + id), alive = !!S.procs[id];
      const isP = S.routerUp ? (n.up && n.role === 'PRIMARY' && p === id) : (alive && n.role === 'PRIMARY');
      el.classList.toggle('primary', isP && alive);
      const down = !alive && (!n.up || !S.routerUp);
      const suspect = !alive && n.up && S.routerUp;          // process dead, detector has not noticed yet
      const joining = alive && S.routerUp && !n.up;          // process started, not yet seen by router
      el.classList.toggle('down', down);
      el.classList.toggle('suspect', suspect || joining);
      el.querySelector('.role').textContent = down ? 'DOWN' : suspect ? 'NO HEARTBEAT' : joining ? 'JOINING' : (n.role === '?' ? '…' : n.role);
      setStat(el, 'epoch', n.up ? n.epoch : '-');
      setStat(el, 'lsn', n.up || !S.routerUp ? n.lsn : '-');
      setStat(el, 'served', n.served);
      el.querySelector('.lsnbar i').style.width = (n.up ? Math.round(100 * n.lsn / maxL) : 0) + '%';
      const tb = el.querySelector('[data-act="toggle"]');
      tb.className = 'icon-btn ' + (alive ? 'kill' : 'start');
      tb.title = alive ? 'kill -9 (power failure)' : 'start / restart';
      tb.innerHTML = alive ? ICON.kill : ICON.start;
      const flags = [];
      if (n.up && n.doubt > 0) flags.push(h('span', { class: 'flag doubt' }, `IN-DOUBT ${n.doubt}`));
      if (n.up && p && id !== p && pl > n.lsn) flags.push(h('span', { class: 'flag lag' }, `lag ${pl - n.lsn}`));
      if (S.delay[id] && alive) flags.push(h('span', { class: 'flag slow' }, `slow +${S.delay[id]}ms`));
      if (S.nodeFp[id] && S.nodeFp[id] !== 'none' && alive) flags.push(h('span', { class: 'flag fp' }, `FAIL ${S.nodeFp[id]}`));
      const fe = el.querySelector('.node-flags'); fe.innerHTML = ''; fe.append(...flags);

      if (prev) {
        const a = prev[id];
        if (a.proc && !alive) { el.classList.remove('shake'); void el.offsetWidth; el.classList.add('shake'); }
        if (a.role !== 'PRIMARY' && n.role === 'PRIMARY' && n.up && prev.router && S.routerUp) {
          el.classList.remove('promoted'); void el.offsetWidth; el.classList.add('promoted');
        }
      }
    }
  }
  drawWires();
  if (prev && S.routerUp && prev.router) trafficFromDeltas(prev, cur);
}

function setStat(el, k, v) {
  const b = el.querySelector(`[data-k="${k}"]`);
  if (b.textContent !== String(v)) {
    b.textContent = v;
    b.classList.remove('bump'); void b.offsetWidth; b.classList.add('bump');
  }
}

// ---------------------------------------------------------------- geometry
function rel(el) {
  const s = stage.getBoundingClientRect(), r = el.getBoundingClientRect();
  return { x: r.left - s.left, y: r.top - s.top, w: r.width, h: r.height, cx: r.left - s.left + r.width / 2, cy: r.top - s.top + r.height / 2 };
}
const P = {
  router() { const r = rel($('#router')); return [r.cx, r.y + r.h]; },
  head(wh) { const r = rel($('#sh-' + wh + ' .shard-head')); return [r.cx, r.y + r.h]; },
  bus(wh) { return rel($('#sh-' + wh)).x - 11; },
  node(id) { return rel($('#n-' + id)); },
  spine() { const R = P.router(), H = rel($('#sh-MUMBAI .shard-head')); return (R[1] + H.y) / 2; },
  toNode(id) {
    const wh = WH_OF[id], R = P.router(), Y = P.spine(), bx = P.bus(wh), n = P.node(id);
    return [R, [R[0], Y], [bx, Y], [bx, n.cy], [n.x, n.cy]];
  },
  repl(from, to) {
    const a = P.node(from), b = P.node(to);
    const x0 = a.x + a.w, y0 = a.cy, x1 = b.x + b.w, y1 = b.cy, bulge = 9 + Math.abs(y1 - y0) * .03;
    const pts = [];
    for (let i = 0; i <= 16; i++) {
      const t = i / 16, u = 1 - t;
      pts.push([u ** 3 * x0 + 3 * u * u * t * (x0 + bulge) + 3 * u * t * t * (x1 + bulge) + t ** 3 * x1,
                u ** 3 * y0 + 3 * u * u * t * y0 + 3 * u * t * t * y1 + t ** 3 * y1]);
    }
    return { pts, d: `M${x0},${y0} C${x0 + bulge},${y0} ${x1 + bulge},${y1} ${x1},${y1}` };
  },
};

function drawWires() {
  const w = $('#wires');
  w.innerHTML = '';
  const R = P.router(), Y = P.spine();
  const xs = WH.map(wh => P.bus(wh));
  w.append(svg('path', { class: 'w-router' + (S.routerUp ? ' primary' : ' down'), d: `M${R[0]},${R[1]} L${R[0]},${Y} M${Math.min(...xs)},${Y} L${Math.max(...xs)},${Y}` }));
  for (const wh of WH) {
    const bx = P.bus(wh);
    const nodes = NODES[wh].map(id => P.node(id));
    const anyUp = NODES[wh].some(id => S.procs[id]);
    w.append(svg('path', { class: 'w-router' + (S.routerUp && anyUp ? '' : ' down'), d: `M${bx},${Y} L${bx},${nodes[2].cy}` }));
    NODES[wh].forEach((id, i) => {
      const n = nodes[i], alive = S.procs[id] && S.nodes[id].up;
      w.append(svg('path', { class: 'w-router' + (alive ? '' : ' down') + (S.primary[wh] === id ? ' primary' : ''), d: `M${bx},${n.cy} L${n.x},${n.cy}` }));
    });
    const p = S.primary[wh];
    if (p && S.procs[p]) {
      for (const id of NODES[wh]) {
        if (id === p) continue;
        const alive = S.procs[id] && S.nodes[id].up && S.nodes[id].follows === p;
        w.append(svg('path', { class: 'w-repl' + (alive ? '' : ' down'), d: P.repl(p, id).d, stroke: COLOR[wh] }));
      }
    }
  }
}

// ---------------------------------------------------------------- packets
function fly(points, { cls = '', color, dur = 800, delay = 0, label, back = false } = {}) {
  if (!points || points.length < 2 || document.hidden) return;
  const pts = back ? points.concat(points.slice(0, -1).reverse()) : points;
  const seg = [0];
  for (let i = 1; i < pts.length; i++) seg.push(seg[i - 1] + Math.hypot(pts[i][0] - pts[i - 1][0], pts[i][1] - pts[i - 1][1]));
  const total = seg[seg.length - 1] || 1;
  const el = h('div', { class: 'pkt ' + cls });
  if (color) { el.style.color = color; if (!cls || cls === 'write') el.style.background = color; }
  if (label) el.append(h('span', { class: 'pkt-label', style: { left: '50%' } }, label));
  $('#packets').append(el);
  const frames = pts.map((p, i) => ({ transform: `translate(${p[0]}px,${p[1]}px)`, offset: seg[i] / total }));
  const a = el.animate(frames, { duration: dur, delay, easing: 'cubic-bezier(.35,.1,.25,1)', fill: 'both' });
  el.animate([{ opacity: 0 }, { opacity: 1, offset: .06 }, { opacity: 1, offset: .92 }, { opacity: 0 }], { duration: dur, delay, fill: 'both' });
  a.onfinish = () => el.remove();
  return dur + delay;
}
function flashNode(id) { const el = $('#n-' + id); if (!el) return; el.classList.remove('flash'); void el.offsetWidth; el.classList.add('flash'); }

function trafficFromDeltas(prev, cur) {
  for (const wh of WH) {
    const p = S.primary[wh];
    for (const id of NODES[wh]) {
      const a = prev[id], b = cur[id];
      if (!a.up || !b.up) continue;
      const dServed = b.served - a.served;
      for (let i = 0; i < Math.min(dServed, 3); i++) fly(P.toNode(id), { cls: 'read', dur: 900, delay: i * 140, back: true });
      const dLsn = b.lsn - a.lsn;
      if (dLsn > 0 && id === p && b.role === 'PRIMARY') {
        for (let i = 0; i < Math.min(dLsn, 3); i++) fly(P.toNode(id), { cls: 'write', color: '#ffffff', dur: 650, delay: i * 160 });
        setTimeout(() => flashNode(id), 600);
      } else if (dLsn > 0 && p && id !== p && S.procs[p]) {
        for (let i = 0; i < Math.min(dLsn, 3); i++) fly(P.repl(p, id).pts, { cls: 'repl', color: COLOR[wh], dur: 600, delay: 450 + i * 150 });
      }
    }
  }
}

// ---------------------------------------------------------------- toasts
function toast(msg, kind = 'info', ms = 4200) {
  const t = h('div', { class: 'toast ' + kind }, msg);
  const box = $('#toasts');
  box.append(t);
  while (box.children.length > 3) box.firstChild.remove();
  setTimeout(() => { t.classList.add('out'); setTimeout(() => t.remove(), 400); }, ms);
}

// ---------------------------------------------------------------- process control
function onProc(action, id) {
  if (action === 'kill') { S.lastKilled[WH_OF[id] || 'router'] = id; S.killedAt = { id, t: performance.now() }; delete S.delay[id]; delete S.nodeFp[id]; }
  if (action === 'start') { delete S.delay[id]; delete S.nodeFp[id]; }
}
async function toggleProc(id) {
  try {
    const alive = S.procs[id];
    const m = await proc(alive ? 'kill' : 'start', id);
    toast(m, alive ? 'bad' : 'ok', 2500);
  } catch (e) { toast(e.message, 'bad'); }
}

function openMenu(anchor, items) {
  const m = $('#menu');
  m.innerHTML = '';
  for (const it of items) {
    if (it === '-') { m.append(h('hr')); continue; }
    if (it.h) { m.append(h('div', { class: 'mh' }, it.h)); continue; }
    m.append(h('button', { disabled: it.disabled, onclick: async () => { closeMenu(); try { await it.run(); } catch (e) { toast(e.message, 'bad'); } } },
      it.label, it.hint ? h('span', { class: 'hint' }, it.hint) : null));
  }
  m.classList.remove('hidden');
  const r = anchor.getBoundingClientRect();
  const mw = 240;
  m.style.left = Math.min(window.innerWidth - mw - 10, r.right - mw + 20) + 'px';
  m.style.top = Math.min(window.innerHeight - m.offsetHeight - 10, r.bottom + 6) + 'px';
}
function closeMenu() { $('#menu').classList.add('hidden'); }
document.addEventListener('click', e => { if (!e.target.closest('#menu')) closeMenu(); });

async function nodeCmd(id, c) { const d = await run(`NODE ${id} ${c}`); toast(`${id}: ${d.resp.split('\n')[0]}`, d.resp.startsWith('OK') ? 'info' : 'bad', 2500); return d; }
function nodeMenu(id, anchor) {
  const alive = S.procs[id];
  openMenu(anchor, [
    { h: `node ${id} · ${WH_OF[id]}` },
    alive ? { label: 'kill -9 (power failure)', hint: 'SIGKILL', run: () => toggleProc(id) } : { label: 'start / restart', hint: 'recovers from WAL', run: () => toggleProc(id) },
    { label: 'crash via RPC', hint: 'NODE CRASH', disabled: !alive, run: () => nodeCmd(id, 'CRASH') },
    '-',
    { label: 'slow replica: +300 ms reads', hint: 'DELAY 300', disabled: !alive, run: async () => { await nodeCmd(id, 'DELAY 300'); S.delay[id] = 300; } },
    { label: 'clear read delay', hint: 'DELAY 0', disabled: !alive, run: async () => { await nodeCmd(id, 'DELAY 0'); delete S.delay[id]; } },
    { label: 'next PREPARE votes NO', hint: 'FAIL vote_no', disabled: !alive, run: async () => { await nodeCmd(id, 'FAIL vote_no'); S.nodeFp[id] = 'vote_no'; } },
    { label: 'crash right after PREPARE', hint: 'FAIL crash_after_prepare', disabled: !alive, run: async () => { await nodeCmd(id, 'FAIL crash_after_prepare'); S.nodeFp[id] = 'crash_after_prepare'; } },
    { label: 'clear failpoint', hint: 'FAIL none', disabled: !alive, run: async () => { await nodeCmd(id, 'FAIL none'); delete S.nodeFp[id]; } },
    '-',
    { label: 'corrupt WAL tail (torn write)', hint: 'node must be down', disabled: alive, run: async () => toast(await proc('corrupt', id), 'warn') },
    { label: 'show recovery report', run: () => { selectTopic('recovery'); showRecovery(id); } },
    { label: 'stream this node’s log', run: () => { $('#ev-source').value = id; switchSource(); } },
  ]);
}
function routerMenu(anchor) {
  const alive = S.procs.router;
  const fp = async v => { await run('FAILPOINT ' + v); toast('router failpoint = ' + v, v === 'none' ? 'info' : 'warn'); };
  openMenu(anchor, [
    { h: 'router / coordinator' },
    alive ? { label: 'kill -9 router', run: () => toggleProc('router') } : { label: 'start router', hint: 'replays coordinator.log', run: () => toggleProc('router') },
    '-',
    { label: 'crash before DECISION', hint: 'crash_after_prepare', disabled: !S.routerUp, run: () => fp('crash_after_prepare') },
    { label: 'crash after DECISION', hint: 'crash_after_decision', disabled: !S.routerUp, run: () => fp('crash_after_decision') },
    { label: 'clear failpoint', disabled: !S.routerUp, run: () => fp('none') },
    '-',
    { label: 'show coordinator recovery', run: () => { selectTopic('recovery'); showRecovery('router'); } },
  ]);
}

// ============================================================================ event stream
const EV = { source: 'router', off: {}, paused: false, filter: 'all', first: {}, follow: true };
const EV_RX = {
  fault: /DOWN|UP again|FAILOVER|ELECTION|PROMOTED|FENCED|stepping down|role =|adopted|UNAVAILABLE|discovered|demoted/,
  tpc: /2PC|DECISION|PREPARE|in-doubt|IN-DOUBT|presumed|re-driv|recovery|FAILPOINT|coordinator/,
  warn: / (WARN|!!) /,
};
const LOG_LISTENERS = new Set();
function classify(l) {
  if (/ !! /.test(l)) return 'bad';
  if (/ WARN /.test(l)) return 'warn';
  if (/UP again|finally delivered|discovered|PREPARED|applied$/.test(l)) return 'good';
  if (/ >> /.test(l)) return 'event';
  return '';
}
function evLine(l) {
  const m = l.match(/^(\S+) \[([^\]]+)\] (.*)$/);
  const cls = 'ev ' + classify(l);
  if (!m) return h('div', { class: cls }, l);
  return h('div', { class: cls }, h('span', { class: 'ts' }, m[1]), h('span', { class: 'src' }, m[2]), m[3].replace(/^(>> |!! |WARN )/, ''));
}
function passes(l) { return EV.filter === 'all' || EV_RX[EV.filter].test(l); }
function appendEvents(lines) {
  if (EV.paused) return;
  const box = $('#events');
  for (const l of lines) if (passes(l)) box.append(evLine(l));
  while (box.children.length > 400) box.firstChild.remove();
  if (EV.follow) box.scrollTop = box.scrollHeight;
}
async function tailOnce(id) {
  const first = EV.off[id] == null;
  const d = await logs(id, first ? -1 : EV.off[id]);
  if (d.next < (EV.off[id] || 0)) EV.off[id] = 0;
  EV.off[id] = d.next;
  return { first, lines: d.lines };
}
async function tailLoop() {
  try {
    const r = await tailOnce('router');
    if (EV.source === 'router') appendEvents(r.first ? r.lines.slice(-120) : r.lines);
    if (!r.first) r.lines.forEach(l => { routerToast(l); LOG_LISTENERS.forEach(f => f('router', l)); });
    if (EV.source !== 'router') {
      const s = await tailOnce(EV.source);
      appendEvents(s.first ? s.lines.slice(-120) : s.lines);
    }
  } catch (e) { /* ignore */ }
  setTimeout(tailLoop, 700);
}
function routerToast(l) {
  const msg = l.replace(/^\S+ \[[^\]]+\] (>> |!! |WARN )?/, '');
  if (/FAILOVER/.test(l)) toast(msg, 'warn', 6000);
  else if (/is DOWN/.test(l)) toast(msg, 'bad');
  else if (/UP again/.test(l)) toast(msg, 'ok');
  else if (/ELECTION/.test(l)) toast(msg, 'info');
  else if (/presumed ABORT|re-driving|coordinator log recovered|FAILPOINT/.test(l)) toast(msg, 'warn', 6000);
  else if (/finally delivered/.test(l)) toast(msg, 'ok', 6000);
  else if (/UNAVAILABLE/.test(l)) toast(msg, 'bad', 6000);
}
function switchSource() {
  EV.source = $('#ev-source').value;
  delete EV.off[EV.source === 'router' ? '__' : EV.source];
  if (EV.source === 'router') delete EV.off.router;
  $('#events').innerHTML = '';
}
function setupEvents() {
  const sel = $('#ev-source');
  ['router', ...ALL].forEach(id => sel.append(h('option', { value: id }, id === 'router' ? 'router log' : id + ' log')));
  sel.onchange = switchSource;
  const box = $('#events');
  box.addEventListener('wheel', () => setTimeout(() => EV.follow = box.scrollTop + box.clientHeight >= box.scrollHeight - 40, 50), { passive: true });
  box.addEventListener('scroll', () => { if (box.scrollTop + box.clientHeight >= box.scrollHeight - 40) EV.follow = true; });
  new ResizeObserver(() => { if (EV.follow) box.scrollTop = box.scrollHeight; }).observe(box);
  $$('#ev-filter .chip').forEach(c => c.onclick = () => {
    $$('#ev-filter .chip').forEach(x => x.classList.remove('on')); c.classList.add('on');
    EV.filter = c.dataset.f; $('#events').innerHTML = ''; delete EV.off[EV.source];
  });
  $('#ev-pause').onclick = e => { EV.paused = !EV.paused; e.target.classList.toggle('on', EV.paused); e.target.textContent = EV.paused ? 'resume' : 'pause'; };
}

/** Mark the end of some logs now; later collect the new lines that match. */
async function markLogs(ids) { const m = {}; for (const id of ids) m[id] = (await logs(id, -1)).next; return m; }
async function collectLogs(mark, re) {
  const out = [];
  for (const id of Object.keys(mark)) {
    const d = await logs(id, mark[id]);
    d.lines.filter(l => re.test(l)).forEach(l => out.push(l));
  }
  return out;
}

// ============================================================================ response rendering
const TOK = /\b(OK|IDENTICAL|CONSISTENT|COMMIT|YES|UP|PRIMARY|BACKUP|ERR|DIVERGED|INCONSISTENT|DOWN|UNAVAILABLE|NO|NO_QUORUM|VIOLATION|ABORTED|ABORT|MUMBAI|PUNE|DELHI|[mpd][123]\*?)\b/g;
function highlight(text) {
  return esc(text).replace(TOK, t => {
    if (/^(OK|IDENTICAL|CONSISTENT|COMMIT|YES|UP)$/.test(t)) return `<span class="t-ok">${t}</span>`;
    if (/^(ERR|DIVERGED|INCONSISTENT|DOWN|UNAVAILABLE|NO|NO_QUORUM|VIOLATION)$/.test(t)) return `<span class="t-bad">${t}</span>`;
    if (/^(ABORT|ABORTED)$/.test(t)) return `<span class="t-warn">${t}</span>`;
    if (t === 'MUMBAI') return `<span class="t-mum">${t}</span>`;
    if (t === 'PUNE') return `<span class="t-pun">${t}</span>`;
    if (t === 'DELHI') return `<span class="t-del">${t}</span>`;
    if (/^[mpd][123]\*?$/.test(t)) return `<span class="t-node">${t}</span>`;
    return t;
  });
}
function badgeFor(resp) {
  if (resp.startsWith('OK')) return h('span', { class: 'badge ok' }, 'OK');
  if (resp.startsWith('ABORTED')) return h('span', { class: 'badge warn' }, 'ABORTED');
  if (resp.startsWith('ERR')) return h('span', { class: 'badge bad' }, 'ERR');
  return h('span', { class: 'badge info' }, 'INFO');
}
function nodeChip(id) { const wh = WH_OF[id.replace('*', '')]; return h('span', { class: 'nodechip', style: { color: wh ? COLOR[wh] : '#fff' } }, id); }
function hashColor(hs) { let x = 0; for (const c of hs) x = (x * 31 + c.charCodeAt(0)) >>> 0; return `hsl(${x % 360} 75% 60%)`; }

function renderResp(c, resp, ms) {
  const verb = c.trim().split(/\s+/)[0].toUpperCase();
  const box = h('div', { class: 'resp' },
    h('div', { class: 'resp-head' }, badgeFor(resp), h('span', { class: 'cmd' }, c), ms != null ? h('span', { class: 'ms' }, ms + ' ms') : null));
  let body = null;
  try {
    if (verb === 'STOCK' && resp.startsWith('OK')) body = renderStock(resp);
    else if (verb === 'CHECK' && resp.startsWith('OK')) body = renderCheck(resp);
  } catch (e) { body = null; }
  box.append(body || h('pre', { html: highlight(resp) }));
  return box;
}
function renderStock(resp) {
  const lines = resp.split('\n');
  const tb = h('table', { class: 'tbl' }, h('tr', {}, ...['Warehouse', 'Served by', 'Role', 'Qty', 'Reserved', 'Available'].map((x, i) => h('th', { class: i > 2 ? 'r' : '' }, x))));
  let k = 0;
  for (const l of lines.slice(2)) {
    const t = l.trim().split(/\s+/);
    if (WH.includes(t[0])) {
      const tr = h('tr', { style: { animationDelay: (k++ * 70) + 'ms' } }, h('td', { style: { color: COLOR[t[0]], fontWeight: 700 } }, t[0]));
      if (t[2] === 'UNAVAILABLE') tr.append(h('td', { colspan: 5, class: 't-bad' }, 'UNAVAILABLE — all replicas down'));
      else if (t[3] === 'not') tr.append(h('td', {}, nodeChip(t[1])), h('td', {}, t[2]), h('td', { colspan: 3, class: 'muted' }, 'not stocked'));
      else tr.append(h('td', {}, nodeChip(t[1])), h('td', { class: t[2] === 'PRIMARY' ? '' : 'muted' }, t[2]), h('td', { class: 'r' }, t[3]),
        h('td', { class: 'r' + (+t[4] > 0 ? ' t-warn' : '') }, t[4]), h('td', { class: 'r', style: { fontWeight: 700 } }, t[5]));
      tb.append(tr);
    } else if (/TOTAL AVAILABLE/.test(l)) {
      tb.append(h('tr', {}, h('td', { colspan: 5, class: 'muted' }, 'total available'), h('td', { class: 'r', style: { fontWeight: 800 } }, l.split('=')[1].trim())));
    }
  }
  return h('div', { style: { padding: '4px 4px 6px' } }, h('div', { class: 'small muted', style: { padding: '4px 8px' } }, lines[0].replace(/^OK /, '')), tb);
}
function renderCheck(resp) {
  const lines = resp.split('\n');
  const tb = h('table', { class: 'tbl' }, h('tr', {}, h('th', {}, 'Warehouse'), h('th', {}, 'Replicas (lsn · table hash)'), h('th', {}, 'Verdict')));
  let k = 0;
  for (const l of lines.slice(1)) {
    const t = l.trim();
    const wh = t.split(/\s+/)[0];
    if (!WH.includes(wh)) continue;
    const cells = [...t.matchAll(/([mpd]\d\*?):(?:lsn=(\d+),hash=(\w+)|DOWN)/g)];
    const verdict = t.split('->')[1].trim();
    tb.append(h('tr', { style: { animationDelay: (k++ * 90) + 'ms' } },
      h('td', { style: { color: COLOR[wh], fontWeight: 700 } }, wh),
      h('td', {}, ...cells.map(m => m[2] ? h('span', { class: 'hash', title: m[3] }, h('i', { style: { background: hashColor(m[3]) } }), `${m[1]} ${m[2]} · ${m[3].slice(0, 6)}`)
                                         : h('span', { class: 'hash' }, h('i', { style: { background: '#333' } }), `${m[1]} DOWN`))),
      h('td', { class: /DIVERGED/.test(verdict) ? 't-bad' : /lagging/.test(verdict) ? 't-warn' : 't-ok' }, verdict)));
  }
  return h('div', { style: { padding: '4px' } }, tb,
    h('div', { class: 'small muted', style: { padding: '6px 10px' } }, 'Same colour = same table hash. Replicas at the same LSN must be byte-identical (deterministic state machine).'));
}

// ============================================================================ 2PC: parse + sequence diagram
const TPC = /^(BUY|ORDER|TRANSFER)$/i;
function parse2pc(c, resp, fpBefore) {
  const m = resp.match(/^(OK|ABORTED) (T\d+) (COMMIT|ABORT) \((.*)\) in (\d+) ms/);
  if (!m) {
    if (resp.startsWith('ERR') && /closed without a reply|not reachable|UNKNOWN/.test(resp) && fpBefore !== 'none') {
      const parts = guessParts(c);
      return { id: 'T????', desc: c, decision: fpBefore === 'crash_after_decision' ? 'COMMIT' : null, ms: null,
               crash: fpBefore === 'crash_after_decision' ? 'after' : 'before', parts };
    }
    return null;
  }
  const parts = [];
  for (const l of resp.split('\n').slice(1)) {
    const v = l.match(/^\s+(MUMBAI|PUNE|DELHI)\s+items=(\S+)\s+vote=(.*)$/);
    if (!v) continue;
    const node = (v[3].match(/\[@(\w+)/) || [])[1];
    const retried = (v[3].match(/retried after (\w+) failed/) || [])[1];
    parts.push({ wh: v[1], items: v[2], vote: v[3].replace(/\s*\[.*\]\s*$/, ''), yes: v[3].startsWith('YES'), node, retried });
  }
  const p2 = (resp.match(/phase-2: (.*)/) || [])[1] || '';
  return { id: m[2], decision: m[3], desc: m[4], ms: +m[5], parts, crash: null, phase2: p2 };
}
function guessParts(c) {
  const parts = [];
  const t = c.trim().split(/\s+/);
  if (/^ORDER$/i.test(t[0])) {
    const by = {};
    t.slice(1).forEach(s => { const [a, w] = s.split('@'); const [sku, q] = a.split(':'); (by[w.toUpperCase()] ||= []).push(`${sku.toUpperCase()}:-${q}:O`); });
    for (const w of Object.keys(by)) parts.push({ wh: w, items: by[w].join(','), vote: '?', yes: true, node: S.primary[w] });
  } else if (/^TRANSFER$/i.test(t[0])) {
    parts.push({ wh: t[3].toUpperCase(), items: `${t[1].toUpperCase()}:-${t[2]}:T`, vote: '?', yes: true, node: S.primary[t[3].toUpperCase()] });
    parts.push({ wh: t[4].toUpperCase(), items: `${t[1].toUpperCase()}:${t[2]}:T`, vote: '?', yes: true, node: S.primary[t[4].toUpperCase()] });
  }
  return parts;
}
const fmtItems = s => s.split(',').map(x => { const [sku, d] = x.split(':'); return `${sku} ${+d > 0 ? '+' : '−'}${Math.abs(+d)}`; }).join(', ');

const SEQ = { last: null, el: null, banner: null, timer: [] };
function onCommandResult(c, resp, fpBefore) {
  const verb = c.trim().split(/\s+/)[0];
  if (TPC.test(verb)) {
    const tx = parse2pc(c, resp, fpBefore);
    if (tx) { SEQ.last = tx; drawSeq(tx); animate2pcTopology(tx); }
  }
  if (/^NODE$/i.test(verb)) {
    const t = c.trim().split(/\s+/);
    if (/^DELAY$/i.test(t[2])) { +t[3] ? S.delay[t[1]] = +t[3] : delete S.delay[t[1]]; }
    if (/^FAIL$/i.test(t[2])) S.nodeFp[t[1]] = t[3];
  }
}

function animate2pcTopology(tx) {
  let t = 0;
  for (const p of tx.parts) {
    const node = p.node || S.primary[p.wh];
    if (!node || !$('#n-' + node)) continue;
    const path = P.toNode(node);
    fly(path, { cls: 'tpc', dur: 700, label: 'PREPARE' });
    if (tx.crash === 'before' && p.vote === '?') continue;
    fly(path.slice().reverse(), { cls: p.yes ? 'yes' : 'no', dur: 700, delay: 800, label: p.yes ? 'YES' : 'NO' });
    if (tx.crash !== 'after' && tx.decision) fly(path, { cls: tx.decision === 'COMMIT' ? 'yes' : 'no', dur: 700, delay: 1700, label: tx.decision });
    t++;
  }
}

function drawSeq(tx) {
  const host = SEQ.el;
  if (!host) return;
  SEQ.timer.forEach(clearTimeout); SEQ.timer = [];
  host.innerHTML = '';
  const W = 600, H = 344, cx = 74, lanes = { MUMBAI: 252, PUNE: 384, DELHI: 516 };
  const s = svg('svg', { class: 'seq', viewBox: `0 0 ${W} ${H}` });
  const defs = svg('defs');
  for (const [k, col] of Object.entries(C)) {
    const mk = svg('marker', { id: 'ah-' + k, viewBox: '0 0 10 10', refX: 9, refY: 5, markerWidth: 7, markerHeight: 7, orient: 'auto-start-reverse' });
    mk.append(svg('path', { d: 'M0,0 L10,5 L0,10 z', fill: col }));
    defs.append(mk);
  }
  s.append(defs);
  const inTx = Object.fromEntries(tx.parts.map(p => [p.wh, p]));
  // lane heads
  const head = (x, title, sub, off, col) => {
    const g = svg('g', { class: 'lane-head' + (off ? ' off' : '') });
    g.append(svg('rect', { x: x - 54, y: 6, width: 108, height: 34, rx: 9, stroke: col }));
    const t1 = svg('text', { x, y: 21, 'text-anchor': 'middle' }); t1.textContent = title; t1.setAttribute('fill', col);
    const t2 = svg('text', { x, y: 34, 'text-anchor': 'middle', class: 'sub' }); t2.textContent = sub;
    g.append(t1, t2);
    s.append(svg('line', { class: 'lane', x1: x, y1: 40, x2: x, y2: H - 8 }));
    s.append(g);
  };
  head(cx, 'COORDINATOR', 'router', false, COLOR.router);
  for (const wh of WH) {
    const p = inTx[wh];
    head(lanes[wh], wh, p ? (p.retried ? `${p.retried}✕ → ${p.node}` : (p.node || 'primary')) : 'not involved', !p, COLOR[wh]);
  }
  const steps = [];
  const G = () => { const g = svg('g', { class: 'step-g' }); s.append(g); steps.push(g); return g; };
  const text = (g, x, y, t, cls = 'msg-label', anchor = 'middle', fill) => { const e = svg('text', { x, y, class: cls, 'text-anchor': anchor }); if (fill) e.setAttribute('fill', fill); e.textContent = t; g.append(e); return e; };
  const arrow = (g, x1, y1, x2, y2, col, dash) => g.append(svg('line', { class: 'msg draw', x1, y1, x2, y2, stroke: C[col], 'marker-end': `url(#ah-${col})`, 'stroke-dasharray': dash ? '4 3' : null, style: dash ? 'stroke-dashoffset:0' : null }));
  const disk = (g, x, y, label) => {
    g.append(svg('rect', { class: 'disk', x: x - 10, y: y - 9, width: 20, height: 14, rx: 3 }));
    g.append(svg('line', { x1: x - 10, y1: y - 4, x2: x + 10, y2: y - 4, stroke: '#8b96ad' }));
    text(g, x + 16, y + 2, label, 'note', 'start');
  };

  // 1 BEGIN
  let g = G(); disk(g, cx, 58, `BEGIN ${tx.id} → coordinator.log (fsync)`);
  // 2 PREPARE
  g = G();
  for (const p of tx.parts) { arrow(g, cx, 76, lanes[p.wh] - 4, 102, 'tpc'); text(g, lanes[p.wh], 116, fmtItems(p.items), 'note'); }
  text(g, cx + 14, 72, 'PREPARE', 'msg-label', 'start', COLOR.router);
  // 3 participant logs PREP + replicates
  g = G();
  for (const p of tx.parts) {
    const ok = p.vote === '?' || p.yes;
    g.append(svg('rect', { x: lanes[p.wh] - 50, y: 124, width: 100, height: 20, rx: 6, fill: ok ? 'rgba(52,211,153,.10)' : 'rgba(251,111,127,.10)', stroke: ok ? 'rgba(52,211,153,.45)' : 'rgba(251,111,127,.5)' }));
    text(g, lanes[p.wh], 138, ok ? 'PREP → WAL + backups' : 'cannot reserve', 'note', 'middle', ok ? '#b7f5dc' : '#ffb4bd');
  }
  // 4 votes
  g = G();
  for (const p of tx.parts) {
    if (p.vote === '?') { arrow(g, lanes[p.wh] - 4, 152, cx + 4, 178, 'gray', true); text(g, lanes[p.wh] - 30, 164, 'vote ?', 'note'); continue; }
    arrow(g, lanes[p.wh] - 4, 152, cx + 4, 178, p.yes ? 'ok' : 'bad');
    text(g, lanes[p.wh] - 12, 166, p.yes ? 'YES' : 'NO', 'msg-label', 'end', p.yes ? C.ok : C.bad);
  }
  // 5 decision / crash before
  g = G();
  if (tx.crash === 'before') {
    text(g, cx, 204, '✕', 'crash');
    text(g, cx + 22, 200, 'coordinator crashed before logging a DECISION', 'note', 'start', '#ffb4bd');
    text(g, cx + 22, 214, '→ on restart: presumed ABORT, reservations released', 'note', 'start');
  } else {
    g.append(svg('rect', { class: 'band', x: 16, y: 190, width: W - 32, height: 26, rx: 7 }));
    text(g, W / 2, 207, `DECISION = ${tx.decision}  ·  forced to coordinator log  ·  COMMIT POINT`, 'band-label');
  }
  if (tx.crash !== 'before') {
    // 6 phase 2 (or crash after decision)
    g = G();
    if (tx.crash === 'after') {
      text(g, cx, 248, '✕', 'crash');
      text(g, cx + 22, 238, 'coordinator crashed after the DECISION was durable', 'note', 'start', '#ffb4bd');
      text(g, cx + 22, 252, 'participants stay prepared (IN-DOUBT) → 2PC blocking', 'note', 'start');
      text(g, cx + 22, 266, '→ on restart: decision re-driven until all ACK', 'note', 'start');
    } else {
      const col = tx.decision === 'COMMIT' ? 'ok' : 'warn';
      for (const p of tx.parts) arrow(g, cx, 230, lanes[p.wh] - 4, 252, col);
      text(g, cx + 14, 226, tx.decision, 'msg-label', 'start', C[col]);
      for (const p of tx.parts) text(g, lanes[p.wh], 266, tx.decision === 'COMMIT' ? 'qty −= items' : 'reservation released', 'note');
      g = G();
      for (const p of tx.parts) { arrow(g, lanes[p.wh] - 4, 276, cx + 4, 298, 'gray'); }
      text(g, cx + 14, 306, 'ACK', 'note', 'start');
      g = G();
      disk(g, cx, 326, `END ${tx.id}${tx.ms != null ? ` · ${tx.ms} ms` : ''}`);
    }
  }
  host.append(s);
  steps.forEach((st, i) => SEQ.timer.push(setTimeout(() => st.classList.add('show'), 120 + i * 420)));
  // banner
  const b = SEQ.banner;
  if (b) {
    b.innerHTML = '';
    const kind = tx.crash ? 'bad' : tx.decision === 'COMMIT' ? 'ok' : 'warn';
    const msg = tx.crash ? `coordinator crashed (${tx.crash === 'before' ? 'before' : 'after'} decision) — restart the router to resolve`
      : `${tx.id} ${tx.decision} · ${tx.desc} · ${tx.ms} ms${tx.phase2 && !/acknowledged/.test(tx.phase2) ? ' · ' + tx.phase2 : ''}`;
    SEQ.timer.push(setTimeout(() => b.append(h('div', { class: 'outcome-banner ' + kind }, tx.crash ? '✕' : tx.decision === 'COMMIT' ? '✓' : '↺', msg)), 120 + steps.length * 420));
  }
}

// ============================================================================ recovery report
const REC = { el: null };
function stripLog(l) { return (l.match(/^\S+ \[[^\]]+\] (?:>> |!! |WARN )?(.*)$/) || [, l])[1]; }
async function showRecovery(id) {
  const host = REC.el;
  if (!host) return;
  const sel = $('#rec-node'); if (sel) sel.value = id;
  host.innerHTML = '<div class="muted small">reading logs/' + id + '.log…</div>';
  const d = await logs(id, -1);
  const L = d.lines;
  const items = [];
  if (id === 'router') {
    let start = -1;
    for (let i = L.length - 1; i >= 0; i--) if (/router\/coordinator listening/.test(L[i])) { start = i; break; }
    if (start < 0) { host.innerHTML = '<div class="muted small">router has not started yet.</div>'; return; }
    let from = start;
    while (from > 0 && start - from < 25 && !/router\/coordinator listening/.test(L[from - 1]) && !/^\S+ \[router\] .*(2PC T\d+ finished|FAILPOINT)/.test(L[from - 1])) from--;
    const win = L.slice(from);
    const ep = win.find(l => /loaded warehouse epochs/.test(l));
    const sum = win.find(l => /coordinator log recovered/.test(l));
    items.push({ k: 'Epochs reloaded', v: ep ? stripLog(ep) : 'no persisted epochs (first start)', s: 'ok' });
    items.push({ k: 'Coordinator log', v: sum ? stripLog(sum) : 'empty: nothing to recover', s: sum ? 'warn' : 'ok' });
    win.filter(l => /presumed ABORT/.test(l)).forEach(l => items.push({ k: 'Presumed abort', v: stripLog(l), s: 'warn' }));
    win.filter(l => /re-driving/.test(l)).forEach(l => items.push({ k: 'Re-drive decision', v: stripLog(l), s: 'warn' }));
    items.push({ k: 'Listening', v: stripLog(L[start]), s: 'ok' });
    win.filter(l => /adopted existing primary/.test(l)).forEach(l => items.push({ k: 'Adopt primary', v: stripLog(l), s: 'ok' }));
    win.filter(l => /finally delivered/.test(l)).forEach(l => items.push({ k: 'Decision delivered', v: stripLog(l), s: 'ok' }));
  } else {
    let i0 = -1;
    for (let i = L.length - 1; i >= 0; i--) if (/CRASH RECOVERY|fresh start/.test(L[i])) { i0 = i; break; }
    if (i0 < 0) { host.innerHTML = `<div class="muted small">no start-up found in ${id}'s log yet.</div>`; return; }
    if (/fresh start/.test(L[i0])) {
      items.push({ k: 'Fresh start', v: stripLog(L[i0]), s: 'ok' });
    } else {
      const block = []; let j = i0 + 1;
      for (; j < L.length && block.length < 12; j++) { block.push(stripLog(L[j]).trim()); if (/recovery took/.test(L[j])) break; }
      const get = k => (block.find(b => b.startsWith(k)) || '').split(':').slice(1).join(':').trim();
      items.push({ k: 'Load checkpoint', v: get('checkpoint') || 'none', s: 'ok' });
      items.push({ k: 'WAL redo', v: get('WAL redo'), s: 'ok' });
      const tail = get('WAL tail');
      items.push({ k: 'WAL tail / CRC', v: /truncated/.test(tail) ? 'TORN WRITE detected — ' + tail : tail, s: /truncated/.test(tail) ? 'bad' : 'ok' });
      items.push({ k: 'Recovered state', v: get('recovered state'), s: 'ok' });
      const dbt = get('in-doubt 2PC tx');
      items.push({ k: 'In-doubt 2PC', v: dbt, s: /none/.test(dbt) ? 'ok' : 'warn' });
      const took = block.find(b => /recovery took/.test(b)) || '';
      items.push({ k: 'Recovery time', v: (took.match(/took ([\d.]+ ms)/) || [, '?'])[1], s: 'ok' });
      const after = L.slice(j + 1, j + 400);
      const fol = after.find(l => /role = BACKUP, following|PROMOTED/.test(l));
      items.push({ k: 'Rejoin (fencing)', v: fol ? stripLog(fol) + (/following/.test(fol) ? ' — old primary never returns as primary' : '') : 'waiting for the router…', s: fol ? 'ok' : 'warn' });
      const st = after.find(l => /FULL STATE TRANSFER/.test(l));
      const reps = after.filter(l => /replicated #/.test(l)).length;
      items.push(st ? { k: 'Catch-up', v: stripLog(st), s: 'warn' }
                    : { k: 'Catch-up', v: reps ? `incremental: ${reps} record(s) shipped by the primary` : 'already in sync with the primary', s: 'ok' });
    }
  }
  host.innerHTML = '';
  const list = h('div', { class: 'rec' });
  items.forEach((it, i) => {
    const row = h('div', { class: 'rec-item ' + it.s }, h('span', { class: 'ic' }, it.s === 'bad' ? '!' : it.s === 'warn' ? '•' : '✓'), h('span', { class: 'k' }, it.k), h('span', { class: 'v' }, it.v || '-'));
    list.append(row);
    setTimeout(() => row.classList.add('show'), 80 + i * 170);
  });
  host.append(h('div', { class: 'small muted', style: { marginBottom: '6px' } }, id === 'router' ? 'Coordinator restart (TxCoordinator.recover)' : `Storage node ${id} restart (StorageNode.recover)`), list);
}

// ============================================================================ topics
const helpers = {
  primary: wh => S.primary[wh],
  backups: wh => NODES[wh].filter(id => id !== S.primary[wh] && S.procs[id]),
  down: wh => (wh ? NODES[wh] : ALL).filter(id => !S.procs[id]),
  async waitPrimaries(ms = 15000) {
    const end = Date.now() + ms;
    while (Date.now() < end) { await poll(); if (S.routerUp && WH.every(w => S.primary[w] && S.nodes[S.primary[w]].up)) return true; await sleep(400); }
    return false;
  },
  async waitUp(ids, ms = 8000) {
    const end = Date.now() + ms;
    while (Date.now() < end) { await poll(); if (ids.every(id => S.nodes[id].up && S.nodes[id].role === 'BACKUP' || S.nodes[id].role === 'PRIMARY' && S.nodes[id].up)) return true; await sleep(400); }
    return false;
  },
  async waitRouter(up = true, ms = 10000) {
    const end = Date.now() + ms;
    while (Date.now() < end) { await poll(); if (S.routerUp === up) return true; await sleep(300); }
    return false;
  },
};

/* A step action receives ctx: ctx.cmd(c) runs + renders inline, ctx.note(text, kind), ctx.log(lines) */
function makeCtx(out) {
  return {
    async cmd(c) { const d = await run(c); out.append(renderResp(c, d.resp, d.ms)); scrollInto(out); return d.resp; },
    note(t, kind = 'info') { out.append(h('div', { class: 'resp' }, h('div', { class: 'resp-head' }, h('span', { class: 'badge ' + kind }, kind === 'bad' ? 'FAULT' : kind === 'ok' ? 'DONE' : kind === 'warn' ? 'NOTE' : 'INFO'), h('span', { class: 'cmd', style: { color: 'var(--text)' } }, t)))); scrollInto(out); },
    log(title, lines) {
      out.append(h('div', { class: 'resp' }, h('div', { class: 'resp-head' }, h('span', { class: 'badge info' }, 'LOG'), h('span', { class: 'cmd' }, title)),
        h('pre', { html: lines.length ? lines.map(l => highlight(l.replace(/^(\S+) /, ''))).join('\n') : '<span class="muted">(no matching lines yet)</span>' })));
      scrollInto(out);
    },
    clear() { out.innerHTML = ''; },
    el: out,
  };
}
function scrollInto(el) {
  const target = el && (el.lastElementChild || el); if (!target) return;
  const box = target.closest('#panel, .console-out'); if (!box) return;
  const b = box.getBoundingClientRect(), r = target.getBoundingClientRect();
  if (r.bottom > b.bottom - 8) box.scrollTo({ top: box.scrollTop + (r.bottom - b.bottom) + 16, behavior: 'smooth' });
  else if (r.top < b.top) box.scrollTo({ top: box.scrollTop - (b.top - r.top) - 16, behavior: 'smooth' });
}

async function act(btn, step, out, fn) {
  btn.classList.add('busy'); step.classList.add('running');
  try { await fn(makeCtx(out)); step.classList.add('done'); }
  catch (e) { makeCtx(out).note(e.message, 'bad'); }
  finally { btn.classList.remove('busy'); step.classList.remove('running'); }
}

const killNote = async (ctx, id, why = '') => { ctx.note(`${await proc('kill', id)}${why}`, 'bad'); };
const startNote = async (ctx, id) => { ctx.note(await proc('start', id), 'ok'); };

const TOPICS = [
  // ------------------------------------------------------------------ 1. Replication
  {
    id: 'repl', n: 1, title: 'Replication', color: '#4cc9f0',
    tag: 'Every warehouse shard lives on 3 nodes. The primary writes to its WAL (fsync), applies, then ships the record to both backups in LSN order.',
    concepts: ['primary-backup', 'WAL shipping', 'in-sync replica set', 'sync / quorum / async', 'log-matching (prevEpoch)', 'deterministic state machine'],
    where: 'Replicator.java · StorageNode.write() · StorageNode.onRepl()',
    widget(el) {
      const grid = h('div', { class: 'lag-grid' });
      for (const wh of WH) {
        const col = h('div', {}, h('div', { class: 'lag-wh', style: { color: COLOR[wh] } }, wh));
        NODES[wh].forEach(id => col.append(h('div', { class: 'lag-row', id: 'lag-' + id }, h('span', { class: 'id' }, id), h('div', { class: 'bar' }, h('i', { style: { background: COLOR[wh], width: '0%' } })), h('span', { class: 'r' }, '-'))));
        grid.append(col);
      }
      const whSel = h('select', { class: 'inp' }, ...WH.map(w => h('option', {}, w)));
      const sku = h('input', { class: 'inp', value: 'LAPTOP' });
      const qty = h('input', { class: 'inp num', value: '5', type: 'number' });
      const out = h('div', { class: 'out' });
      const btn = h('button', { class: 'btn sm accent', onclick: async () => { btn.classList.add('busy'); try { await makeCtx(out).cmd(`RESTOCK ${whSel.value} ${sku.value} ${qty.value}`); } finally { btn.classList.remove('busy'); } } }, 'Write');
      el.append(h('h4', {}, 'Replica LSNs (live)', h('span', { class: 'right muted small' }, 'bar = LSN relative to the primary')), grid,
        h('div', { style: { marginTop: '14px', display: 'flex', gap: '6px', alignItems: 'center', flexWrap: 'wrap' } },
          h('span', { class: 'small muted' }, 'RESTOCK'), whSel, sku, qty, btn), out);
      const tick = () => {
        for (const wh of WH) {
          const p = S.primary[wh], pl = Math.max(1, p ? S.nodes[p].lsn : 1);
          NODES[wh].forEach(id => {
            const n = S.nodes[id], row = $('#lag-' + id); if (!row) return;
            const up = n.up && S.procs[id];
            row.classList.toggle('down', !up);
            row.querySelector('i').style.width = (up ? Math.min(100, 100 * n.lsn / pl) : 100) + '%';
            row.querySelector('.r').textContent = up ? (id === p ? `${n.lsn} ★` : n.lsn) : 'DOWN';
          });
        }
      };
      tick(); return tick;
    },
    steps: [
      { t: 'One primary per warehouse, all replicas at the same LSN', d: '<code>STATUS</code> — the router elected a PRIMARY per shard (★ in the topology). Backups FOLLOW it in the same epoch.',
        acts: [{ l: 'STATUS', f: c => c.cmd('STATUS') }], e: 'Each warehouse shows exactly one <b>*</b> primary and equal LSNs.' },
      { t: 'A write goes to the primary and is shipped to both backups', d: 'Watch the white packet hit the primary, then the WAL-shipping rings travel to each backup.',
        acts: [{ l: 'RESTOCK MUMBAI LAPTOP 5', f: c => c.cmd('RESTOCK MUMBAI LAPTOP 5') }], e: '<b>replicas_acked=2/2</b> — the client is acknowledged only after both backups have the record (sync mode).' },
      { t: 'Replicas are byte-identical', d: '<code>CHECK</code> hashes every replica’s tables. Same LSN ⇒ same hash, because the state is a deterministic function of the log.',
        acts: [{ l: 'CHECK', f: c => c.cmd('CHECK') }], e: 'Every warehouse is <b>IDENTICAL</b>; equal colours mean equal hashes.' },
      { t: 'Backups serve reads', d: 'Run <code>STOCK LAPTOP</code> twice; the load balancer sends each read to a different replica.',
        acts: [{ l: 'STOCK LAPTOP ×2', f: async c => { await c.cmd('STOCK LAPTOP'); await c.cmd('STOCK LAPTOP'); } }], e: 'The <b>Served by</b> column changes but the quantities are the same — backups have the new value.' },
      { t: 'Replication modes', d: '<code>async</code> acks after the local fsync only (fast, may lose the tail on failover). <code>quorum</code> refuses writes without a majority (CP).',
        acts: [
          { l: 'MODE async → write', f: async c => { await c.cmd('MODE async'); await c.cmd('RESTOCK MUMBAI PHONE 1'); } },
          { l: 'MODE quorum → write', f: async c => { await c.cmd('MODE quorum'); await c.cmd('RESTOCK MUMBAI PHONE 1'); } },
          { l: 'back to sync', cls: 'good', f: c => c.cmd('MODE sync') }],
        e: 'async ⇒ <b>replicas_acked=async</b>; sync ⇒ <b>2/2</b>.' },
    ],
  },
  // ------------------------------------------------------------------ 2. Fault tolerance
  {
    id: 'fault', n: 2, title: 'Fault tolerance', color: '#fb7185',
    tag: 'The router pings every node each 500 ms. 3 missed heartbeats ⇒ DOWN. The live replica with the highest LSN is promoted with epoch+1; lower epochs are fenced.',
    concepts: ['heartbeat failure detector', 'automatic failover', 'highest-LSN election', 'epochs / terms', 'fencing (STALE)', 'no lost acknowledged writes'],
    where: 'ClusterManager.round() · reconcile() · elect() · StorageNode.onPromote()/onFollow()',
    widget(el) {
      const cells = { det: h('div', { class: 'v' }, '–'), out: h('div', { class: 'v' }, '–'), np: h('div', { class: 'v' }, '–') };
      const tl = h('div', { class: 'timeline' });
      const killBtns = h('div', { class: 'btn-row' }, ...WH.map(wh => h('button', { class: 'btn sm danger', onclick: async e => {
        const p = S.primary[wh]; if (!p) return toast(`${wh} has no primary`, 'bad');
        e.currentTarget.classList.add('busy'); try { toast(await proc('kill', p), 'bad'); } finally { e.currentTarget.classList.remove('busy'); }
      } }, `kill ${wh} primary`)));
      el.append(h('h4', {}, 'Failover stopwatch', h('span', { class: 'right muted small' }, 'from the router’s own measurements')),
        h('div', { class: 'watch' },
          h('div', { class: 'watch-cell' }, h('div', { class: 'k' }, 'Detection'), cells.det),
          h('div', { class: 'watch-cell' }, h('div', { class: 'k' }, 'Outage (detect + promote)'), cells.out),
          h('div', { class: 'watch-cell' }, h('div', { class: 'k' }, 'New primary'), cells.np)),
        h('div', { style: { marginTop: '12px' } }, killBtns),
        h('h4', { style: { marginTop: '14px' } }, 'Epoch history'), tl);
      const listener = (src, l) => {
        let m = l.match(/NODE (\w+) \((\w+)\) is DOWN: missed (\d+) heartbeats \(~(\d+) ms/);
        if (m) { cells.det.innerHTML = `${m[4]}<small> ms</small>`; cells.out.innerHTML = '<span class="muted">…</span>'; cells.np.innerHTML = '<span class="muted">electing</span>'; }
        m = l.match(/FAILOVER (\w+): primary (\w+) -> (\w+) \(epoch (\d+), lsn (\d+)\)\. Outage ~(\d+) ms/);
        if (m) {
          cells.out.innerHTML = `${m[6]}<small> ms</small>`;
          cells.np.innerHTML = `<span style="color:${COLOR[m[1]]}">${m[3]}</span><small> epoch ${m[4]}</small>`;
          tl.prepend(h('div', { class: 'tl' }, h('span', { class: 'when' }, l.slice(0, 12)), h('b', { style: { color: COLOR[m[1]] } }, m[1]),
            `${m[2]} ✕ → ${m[3]}`, h('span', { class: 'epoch-pill' }, 'epoch ' + m[4]), h('span', { class: 'muted' }, `lsn ${m[5]} · ${m[6]} ms`)));
        }
        m = l.match(/ELECTION (\w+): (\w+) is PRIMARY \(epoch (\d+)/);
        if (m) tl.prepend(h('div', { class: 'tl' }, h('span', { class: 'when' }, l.slice(0, 12)), h('b', { style: { color: COLOR[m[1]] } }, m[1]), `${m[2]} elected`, h('span', { class: 'epoch-pill' }, 'epoch ' + m[3])));
      };
      LOG_LISTENERS.add(listener);
      return null;
    },
    steps: [
      { t: 'Crash the MUMBAI primary', d: 'A real <code>kill -9</code> of the JVM (power failure: no shutdown hooks). Watch it go amber (missed heartbeats) then red, and the crown move.',
        acts: [
          { l: 'kill -9 MUMBAI primary', cls: 'danger', f: async c => {
            const p = helpers.primary('MUMBAI'); if (!p) throw new Error('MUMBAI has no primary');
            const m = await markLogs(['router']);
            await killNote(c, p);
            await sleep(3200);
            c.log('router log', await collectLogs(m, /DOWN|FAILOVER|PROMOTE/));
          } },
          { l: 'NODE … CRASH (RPC)', cls: 'danger', f: async c => {
            const p = helpers.primary('MUMBAI'); if (!p) throw new Error('MUMBAI has no primary');
            S.lastKilled.MUMBAI = p; const m = await markLogs(['router']);
            await c.cmd(`NODE ${p} CRASH`); await sleep(3200);
            c.log('router log', await collectLogs(m, /DOWN|FAILOVER|PROMOTE/));
          } }],
        e: 'Within ~1.5 s: <b>NODE m1 is DOWN</b> then <b>FAILOVER MUMBAI: primary m1 -> m2 (epoch 2…)</b>.' },
      { t: 'New primary has a higher epoch; no acknowledged write lost', d: 'The highest-LSN replica was promoted, so it holds every acknowledged write.',
        acts: [{ l: 'STATUS', f: c => c.cmd('STATUS') }, { l: 'STOCK PHONE', f: c => c.cmd('STOCK PHONE') }], e: 'The MUMBAI primary changed and its <b>EPOCH</b> went up by one.' },
      { t: 'Writes continue on the new primary', d: 'The router routes writes to whoever is primary in the current epoch.',
        acts: [{ l: 'RESTOCK MUMBAI PHONE 10', f: c => c.cmd('RESTOCK MUMBAI PHONE 10') }], e: '<b>OK</b>, served by the new primary, <b>replicas_acked=1/1</b> while the old one is down.' },
      { t: 'Restart the old primary: recovery + epoch fencing', d: 'It replays its WAL, rejoins as BACKUP and follows the new primary. A zombie primary can never overwrite the new one.',
        acts: [{ l: 'restart old primary', cls: 'good', f: async c => {
          const id = S.lastKilled.MUMBAI && !S.procs[S.lastKilled.MUMBAI] ? S.lastKilled.MUMBAI : helpers.down('MUMBAI')[0];
          if (!id) throw new Error('no MUMBAI node is down');
          await startNote(c, id); await sleep(2600);
          REC.el = c.el.appendChild(h('div', { class: 'widget', style: { marginTop: '10px' } }));
          await showRecovery(id);
        } }], e: 'Recovery block, then <b>role = BACKUP, following primary m2</b>.' },
      { t: 'All replicas consistent again', d: 'The restarted node caught up (incremental catch-up or snapshot state transfer).',
        acts: [{ l: 'CHECK', f: c => c.cmd('CHECK') }], e: 'All warehouses <b>IDENTICAL</b>.' },
    ],
  },
  // ------------------------------------------------------------------ 3. Availability
  {
    id: 'avail', n: 3, title: 'Availability', color: '#6ee7a8',
    tag: 'Clients never see the failover: writes wait (≤6 s) for the new primary and are retried only when the request was certainly not delivered. Reads are served by any replica.',
    concepts: ['ride-through failover', 'safe retries (ConnectFailed)', 'reads from backups', 'partial availability', 'CAP: quorum vs sync'],
    where: 'Router.callPrimary() · ClusterManager.awaitPrimary() · readable()',
    widget(el) {
      const k = { req: h('div', { class: 'v' }, '0'), err: h('div', { class: 'v ok' }, '0'), av: h('div', { class: 'v ok' }, '–'), lat: h('div', { class: 'v' }, '–') };
      const chart = svg('svg', { class: 'chart', viewBox: '0 0 560 150', preserveAspectRatio: 'none' });
      const dur = h('input', { class: 'inp num', value: '40', type: 'number' });
      const rate = h('input', { class: 'inp num', value: '10', type: 'number' });
      const btn = h('button', { class: 'btn sm accent' }, 'Start traffic');
      const status = h('span', { class: 'small muted' }, '70% STOCK PEN · 30% BUY PEN 1 (2PC)');
      el.append(h('h4', {}, 'Live client traffic (loadgen)', h('span', { class: 'right' }, status)),
        h('div', { class: 'kpis' },
          h('div', { class: 'kpi' }, h('div', { class: 'k' }, 'requests'), k.req),
          h('div', { class: 'kpi' }, h('div', { class: 'k' }, 'errors'), k.err),
          h('div', { class: 'kpi' }, h('div', { class: 'k' }, 'availability'), k.av),
          h('div', { class: 'kpi' }, h('div', { class: 'k' }, 'worst latency'), k.lat)),
        chart,
        h('div', { style: { display: 'flex', gap: '6px', alignItems: 'center', marginTop: '8px', flexWrap: 'wrap' } },
          h('span', { class: 'small muted' }, 'seconds'), dur, h('span', { class: 'small muted' }, 'ops/s'), rate, btn,
          h('span', { class: 'small muted', style: { marginLeft: 'auto' } }, 'bars: ok / slow (>1 s) / errors · line: max latency')));
      LG.ui = { k, chart, btn, status };
      btn.onclick = () => LG.running ? LG.stop() : LG.start(+dur.value, +rate.value);
      LG.draw();
      return null;
    },
    steps: [
      { t: 'Start continuous client traffic', d: 'A browser-side load generator: ~10 ops/s for 40 s, each a real request through the router.',
        acts: [{ l: 'start loadgen 40 s', cls: 'accent', f: async c => { LG.start(40, 10); c.note('loadgen running — now kill nodes below while watching the chart', 'info'); } }] },
      { t: 'Kill the PUNE primary while traffic runs', d: 'Failover takes ~1.5 s. Requests in that window wait for the new primary instead of failing.',
        acts: [{ l: 'kill -9 PUNE primary', cls: 'danger', f: async c => { const p = helpers.primary('PUNE'); if (!p) throw new Error('PUNE has no primary'); await killNote(c, p); } }],
        e: 'One <b>slow</b> (amber) second, <b>0 errors</b>.' },
      { t: 'Kill a second PUNE node', d: 'With sync replication the ISR shrinks; the single survivor keeps serving reads and writes.',
        acts: [{ l: 'kill another PUNE node', cls: 'danger', f: async c => {
          const id = NODES.PUNE.filter(x => S.procs[x]).sort((a, b) => (a === S.primary.PUNE) - (b === S.primary.PUNE))[0];
          if (!id || NODES.PUNE.filter(x => S.procs[x]).length < 2) throw new Error('need at least 2 live PUNE nodes');
          await killNote(c, id);
        } }], e: 'Availability stays at <b>100%</b>.' },
      { t: 'Partial outage: a whole warehouse down', d: 'Kill all DELHI replicas. DELHI is unavailable, the other shards keep working, and BUY allocates from live warehouses.',
        acts: [
          { l: 'kill all DELHI', cls: 'danger', f: async c => { for (const id of NODES.DELHI.filter(x => S.procs[x])) await killNote(c, id); } },
          { l: 'STOCK LAPTOP', f: c => c.cmd('STOCK LAPTOP') },
          { l: 'BUY LAPTOP 3', f: c => c.cmd('BUY LAPTOP 3') }],
        e: '<b>DELHI UNAVAILABLE</b>, yet <b>BUY LAPTOP 3</b> commits from MUMBAI/PUNE.' },
      { t: 'CAP trade-off (with 1 of 3 PUNE nodes alive)', d: '<code>quorum</code> refuses the write without a majority (consistency first). <code>sync</code> accepts it (availability first).',
        acts: [
          { l: 'MODE quorum → RESTOCK PUNE PEN 5', f: async c => { await c.cmd('MODE quorum'); await sleep(2000); await c.cmd('RESTOCK PUNE PEN 5'); } },
          { l: 'MODE sync → RESTOCK PUNE PEN 5', cls: 'good', f: async c => { await c.cmd('MODE sync'); await c.cmd('RESTOCK PUNE PEN 5'); } }],
        e: 'quorum ⇒ <b>ERR NO_QUORUM</b>; sync ⇒ <b>OK</b>.' },
      { t: 'Heal the cluster and verify', d: 'Restart every killed process; they recover and catch up. Then check replicas and the global stock invariant.',
        acts: [
          { l: 'restart all down', cls: 'good', f: async c => { const d = helpers.down(); if (!d.length) return c.note('nothing is down', 'ok'); for (const id of d) await startNote(c, id); await helpers.waitUp(d.filter(x => x !== 'router'), 9000); c.note('all nodes rejoined', 'ok'); } },
          { l: 'CHECK', f: c => c.cmd('CHECK') }, { l: 'AUDIT', f: c => c.cmd('AUDIT') }],
        e: '<b>IDENTICAL</b> everywhere and <b>CONSISTENT - no stock created or lost</b>.' },
    ],
  },
  // ------------------------------------------------------------------ 4. Distributed transactions
  {
    id: 'tpc', n: 4, title: 'Distributed transactions', color: '#b693ff',
    tag: 'An order that takes stock from several warehouses is atomic: Two-Phase Commit with the router as coordinator and the warehouse primaries as participants.',
    concepts: ['two-phase commit', 'escrow reservation (no 2PL deadlocks)', 'durable votes (PREP replicated)', 'presumed abort', 'coordinator log', 'stock never negative'],
    where: 'TxCoordinator.execute() · StorageNode.prepare()/decide() · InventoryState (reserved)',
    widget(el) {
      SEQ.el = h('div', {}, h('div', { class: 'muted small', style: { padding: '30px 0', textAlign: 'center' } }, 'Run a BUY / ORDER / TRANSFER — the protocol is drawn here, step by step, from the router’s real response.'));
      SEQ.banner = h('div');
      const inp = h('input', { class: 'inp wide', value: 'ORDER LAPTOP:1@MUMBAI PEN:10@PUNE' });
      const out = h('div', { class: 'out' });
      const btn = h('button', { class: 'btn sm accent', onclick: async () => { btn.classList.add('busy'); try { await makeCtx(out).cmd(inp.value); } finally { btn.classList.remove('busy'); } } }, 'Run');
      el.append(h('h4', {}, '2PC sequence', h('span', { class: 'right muted small' }, 'coordinator ⇄ warehouse primaries')), SEQ.el, SEQ.banner,
        h('div', { style: { display: 'flex', gap: '6px', marginTop: '12px', alignItems: 'center' } }, inp, btn), out);
      if (SEQ.last) drawSeq(SEQ.last);
      return null;
    },
    steps: [
      { t: 'A multi-warehouse order commits', d: 'BUY auto-splits the quantity across warehouses, most stock first. Every participant reserves (escrow) and votes YES. (README: <code>BUY LAPTOP 15</code> on fresh seed data.)',
        acts: [{ l: 'STOCK LAPTOP', f: c => c.cmd('STOCK LAPTOP') }, { l: 'BUY more LAPTOPs than one warehouse has', cls: 'accent', f: async c => {
          const r = (await api('/api/cmd', { method: 'POST', body: 'STOCK LAPTOP' })).resp;
          const av = r.split('\n').map(l => l.trim().split(/\s+/)).filter(t => WH.includes(t[0]) && /^\d+$/.test(t[5] || '')).map(t => +t[5]);
          const total = av.reduce((a, b) => a + b, 0), max = Math.max(0, ...av);
          if (total < 2) throw new Error('not enough LAPTOP stock left — RESTOCK first');
          await c.cmd(`BUY LAPTOP ${Math.min(total, max + Math.max(1, Math.min(5, total - max)))}`);
        } }],
        e: 'Two participants, both <b>YES</b>, decision <b>COMMIT</b>.' },
      { t: 'One NO vote aborts everything', d: 'MUMBAI can reserve 2 phones, DELHI cannot reserve 500 ⇒ the whole order aborts and MUMBAI’s reservation is released.',
        acts: [{ l: 'ORDER PHONE:2@MUMBAI PHONE:500@DELHI', f: c => c.cmd('ORDER PHONE:2@MUMBAI PHONE:500@DELHI') }, { l: 'STOCK PHONE', f: c => c.cmd('STOCK PHONE') }],
        e: '<b>ABORTED</b>; <b>RESERVED = 0</b> everywhere afterwards.' },
      { t: 'Atomic stock transfer', d: 'Debit one warehouse and credit another in one transaction.',
        acts: [{ l: 'TRANSFER PHONE 5 MUMBAI PUNE', f: c => c.cmd('TRANSFER PHONE 5 MUMBAI PUNE') }], e: 'Both sides commit, or neither does.' },
      { t: 'Inject a participant failure', d: 'Arm the PUNE primary to vote NO on its next PREPARE, then send a 3-warehouse order.',
        acts: [{ l: 'arm PUNE primary: vote_no', cls: 'danger', f: async c => { const p = helpers.primary('PUNE'); await c.cmd(`NODE ${p} FAIL vote_no`); } },
               { l: 'ORDER PEN:1@MUMBAI PEN:1@PUNE PEN:1@DELHI', f: c => c.cmd('ORDER PEN:1@MUMBAI PEN:1@PUNE PEN:1@DELHI') }],
        e: 'PUNE votes <b>NO</b> ⇒ global <b>ABORT</b>.' },
      { t: 'Concurrency: 25 buyers, 15 headphones', d: '25 concurrent clients each <code>BUY HEADPHONE 1</code>. Escrow reservations serialize correctly without long row locks.',
        acts: [
          { l: 'reset HEADPHONE stock to 15', f: async c => {
            const r = await c.cmd('STOCK HEADPHONE');
            const line = r.split('\n').find(l => /^\s+PUNE\s/.test(l)); const avail = line ? +line.trim().split(/\s+/)[5] : 0;
            if (avail < 15) await c.cmd(`RESTOCK PUNE HEADPHONE ${15 - avail}`); else c.note('PUNE already has ' + avail, 'ok');
          } },
          { l: 'bench 25 × BUY HEADPHONE 1', cls: 'accent', f: async c => {
            const crowd = h('div', { class: 'crowd' }, ...Array.from({ length: 25 }, (_, i) => h('div', { class: 'person wait' }, i + 1)));
            const tally = h('div', { class: 'small muted' }, '25 customers racing…');
            c.el.append(h('div', { class: 'widget', style: { marginTop: '10px' } }, h('h4', {}, '25 concurrent customers'), crowd, tally));
            const r = await bench(25, 25, 'BUY HEADPHONE 1');
            r.outcomes.forEach((o, i) => setTimeout(() => { const p = crowd.children[i]; p.className = 'person ' + (o === 'OK' ? 'ok' : o === 'ABORTED' ? 'ab' : 'er'); p.textContent = o === 'OK' ? '✓' : o === 'ABORTED' ? '–' : '!'; }, i * 55));
            setTimeout(() => tally.innerHTML = `<span class="t-ok">${r.ok} committed</span> · <span class="t-warn">${r.aborted} aborted (out of stock)</span> · <span class="t-bad">${r.err} errors</span> · avg ${r.avg} ms · p95 ${r.p95} ms`, 25 * 55 + 200);
            kick();
          } },
          { l: 'STOCK HEADPHONE', f: c => c.cmd('STOCK HEADPHONE') }],
        e: '<b>Exactly 15 commit</b>; stock never goes negative.' },
      { t: 'Verify: global invariant + coordinator log', d: '<code>AUDIT</code> checks on_hand = received − shipped for every SKU. <code>TXLOG</code> lists the coordinator’s decisions.',
        acts: [{ l: 'AUDIT', f: c => c.cmd('AUDIT') }, { l: 'TXLOG 8', f: c => c.cmd('TXLOG 8') }], e: '<b>CONSISTENT - no stock created or lost</b>.' },
    ],
  },
  // ------------------------------------------------------------------ 5. Recovery
  {
    id: 'recovery', n: 5, title: 'Recovery', color: '#ffc15e',
    tag: 'Nodes restart from checkpoint + WAL REDO with CRC checks; torn writes are truncated; laggards catch up or get a snapshot; the coordinator replays its own log.',
    concepts: ['checkpoint + REDO', 'CRC32 / torn-write truncation', 'catch-up vs state transfer', 'in-doubt 2PC', 'presumed abort', 'decision re-drive'],
    where: 'StorageNode.recover() · WriteAheadLog.recover() · TxCoordinator.recover() · Replicator',
    widget(el) {
      const sel = h('select', { class: 'inp', id: 'rec-node' }, ...['router', ...ALL].map(id => h('option', { value: id }, id)));
      REC.el = h('div', {}, h('div', { class: 'muted small' }, 'Pick a process and show what its last start-up recovered — parsed from its log file.'));
      el.append(h('h4', {}, 'Recovery report', h('span', { class: 'right', style: { display: 'flex', gap: '6px' } }, sel,
        h('button', { class: 'btn xs', onclick: () => showRecovery(sel.value) }, 'show'))), REC.el);
      return null;
    },
    steps: [
      { t: 'WAL redo + torn-write detection', d: 'Kill a DELHI backup, write while it is down, append half a record to its WAL (a crash mid-append), then restart it.',
        acts: [
          { l: '① kill DELHI backup', cls: 'danger', f: async c => { const b = helpers.backups('DELHI'); const id = b.includes('d2') ? 'd2' : b[0]; if (!id) throw new Error('no live DELHI backup'); S.recVictim = id; await killNote(c, id); } },
          { l: '② 3 writes', f: async c => { for (let i = 0; i < 3; i++) await c.cmd('RESTOCK DELHI PEN 1'); } },
          { l: '③ corrupt WAL', cls: 'danger', f: async c => { const id = S.recVictim || helpers.down('DELHI')[0]; if (!id) throw new Error('kill a DELHI backup first'); c.note(await proc('corrupt', id), 'warn'); } },
          { l: '④ restart + report', cls: 'good', f: async c => {
            const id = S.recVictim && !S.procs[S.recVictim] ? S.recVictim : helpers.down('DELHI')[0]; if (!id) throw new Error('no DELHI node is down');
            await startNote(c, id); await sleep(2800); await showRecovery(id); scrollInto(REC.el);
          } },
          { l: 'CHECK', f: c => c.cmd('CHECK') }],
        e: 'checkpoint → N records replayed → <b>torn write … truncated</b> → catch-up from the primary → <b>IDENTICAL</b>.' },
      { t: 'Checkpoint', d: 'Snapshot all tables atomically (tmp + rename), then truncate the WAL. Bounds recovery time.',
        acts: [{ l: 'CHECKPOINT', f: c => c.cmd('CHECKPOINT') }], e: 'Every live node reports a checkpoint; <code>wal.log</code> becomes empty.' },
      { t: 'Too far behind ⇒ full state transfer', d: 'Kill a PUNE backup, make 60 writes (more than the 50-record checkpoint interval), restart it. Its missing records were checkpointed away, so the primary ships a snapshot.',
        acts: [
          { l: '① kill PUNE backup', cls: 'danger', f: async c => { const id = helpers.backups('PUNE')[0]; if (!id) throw new Error('no live PUNE backup'); S.stVictim = id; await killNote(c, id); } },
          { l: '② 60 writes', f: async c => { const r = await bench(60, 4, 'RESTOCK PUNE LAPTOP 1'); c.note(`60 × RESTOCK PUNE LAPTOP 1: ${r.ok} ok, ${r.err} err, avg ${r.avg} ms`, r.err ? 'bad' : 'ok'); kick(); } },
          { l: '③ restart + show transfer', cls: 'good', f: async c => {
            const id = S.stVictim && !S.procs[S.stVictim] ? S.stVictim : helpers.down('PUNE')[0]; if (!id) throw new Error('no PUNE node down');
            const p = helpers.primary('PUNE'); const m = await markLogs([p, id]);
            await startNote(c, id); await sleep(3500);
            c.log(`${p} + ${id} logs`, await collectLogs(m, /STATE TRANSFER|INSTALL|following|CRASH RECOVERY|WAL redo|checkpoint/i));
            await showRecovery(id);
          } }],
        e: 'Primary: <b>STATE TRANSFER -> p… shipping full snapshot</b>; backup: <b>FULL STATE TRANSFER … installed</b>.' },
      { t: 'Participant crashes right after PREPARE', d: 'The DELHI primary logs + replicates PREP, then dies before voting. The new DELHI primary already knows the transaction is prepared.',
        acts: [
          { l: 'arm DELHI primary', cls: 'danger', f: async c => { const p = helpers.primary('DELHI'); if (!p) throw new Error('DELHI has no primary'); await c.cmd(`NODE ${p} FAIL crash_after_prepare`); } },
          { l: 'ORDER PEN:2@MUMBAI PHONE:1@DELHI', f: c => c.cmd('ORDER PEN:2@MUMBAI PHONE:1@DELHI') },
          { l: 'restart crashed node', cls: 'good', f: async c => { const id = helpers.down('DELHI')[0]; if (!id) return c.note('nothing down in DELHI', 'ok'); await startNote(c, id); } }],
        e: 'It <b>COMMITS</b> with vote <b>YES (already prepared) [@d2, retried after d1 failed]</b>.' },
      { t: 'Coordinator crashes AFTER the decision', d: 'The DECISION is durable but never sent. Participants stay prepared with stock reserved (2PC blocking) until the router comes back and re-drives it.',
        acts: [
          { l: 'FAILPOINT crash_after_decision', cls: 'danger', f: c => c.cmd('FAILPOINT crash_after_decision') },
          { l: 'ORDER PHONE:1@MUMBAI PEN:3@PUNE', f: async c => { await c.cmd('ORDER PHONE:1@MUMBAI PEN:3@PUNE'); c.note('router died — see the sequence diagram in tab 4', 'bad'); } },
          { l: 'watch participants (7 s)', f: async c => {
            const ids = [S.primary.MUMBAI, S.primary.PUNE].filter(Boolean); const m = await markLogs(ids);
            await sleep(7000); c.log(ids.join(' + ') + ' logs', await collectLogs(m, /IN-DOUBT|coordinator unreachable/));
          } },
          { l: 'restart router + report', cls: 'good', f: async c => { await startNote(c, 'router'); await helpers.waitRouter(true); await sleep(2600); await showRecovery('router'); } }],
        e: 'Participants: <b>T… stays IN-DOUBT</b>. Router: <b>re-driving decision COMMIT</b> → <b>finally delivered</b>.' },
      { t: 'Coordinator crashes BEFORE the decision', d: 'Votes are in, no DECISION logged. On restart the coordinator applies presumed abort and the reservations are released.',
        acts: [
          { l: 'FAILPOINT crash_after_prepare', cls: 'danger', f: c => c.cmd('FAILPOINT crash_after_prepare') },
          { l: 'ORDER PEN:7@MUMBAI PEN:7@DELHI', f: async c => { await c.cmd('ORDER PEN:7@MUMBAI PEN:7@DELHI'); c.note('router died before logging a decision', 'bad'); } },
          { l: 'restart router + report', cls: 'good', f: async c => { await startNote(c, 'router'); await helpers.waitRouter(true); await sleep(3000); await showRecovery('router'); } },
          { l: 'STOCK PEN', f: c => c.cmd('STOCK PEN') }],
        e: '<b>presumed ABORT</b>; <b>RESERVED = 0</b> for PEN.' },
    ],
  },
  // ------------------------------------------------------------------ 6. Load balancing
  {
    id: 'lb', n: 6, title: 'Load balancing', color: '#7dd3fc',
    tag: 'Writes go to the primary; reads go to any in-sync replica chosen by the policy. Down or stale replicas are excluded automatically. BUY balances at the data level.',
    concepts: ['round robin', 'least connections', 'random', 'primary-only baseline', 'failure-aware routing', 'stock-aware allocation'],
    where: 'LoadBalancer.order() · ClusterManager.readable() · Router.buy()',
    widget(el) {
      const grid = h('div', { class: 'lb-grid' });
      for (const wh of WH) {
        const col = h('div', {}, h('div', { class: 'lag-wh', style: { color: COLOR[wh] } }, wh));
        NODES[wh].forEach(id => col.append(h('div', { class: 'lb-row', id: 'lb-' + id }, h('span', { class: 'id' }, id), h('div', { class: 'bar' }, h('i', { style: { background: COLOR[wh], width: '0%' } })), h('span', { class: 'r' }, '0 · 0%'))));
        grid.append(col);
      }
      const n = h('input', { class: 'inp num', value: '90', type: 'number' });
      const t = h('input', { class: 'inp num', value: '6', type: 'number' });
      const c = h('input', { class: 'inp', value: 'STOCK LAPTOP', style: { width: '130px' } });
      const btn = h('button', { class: 'btn sm accent' }, 'Run benchmark');
      LBX.hist = h('table', { class: 'tbl' }, h('tr', {}, ...['#', 'policy', 'slow', 'avg', 'p95', 'max', 'distribution'].map((x, i) => h('th', { class: i >= 3 && i <= 5 ? 'r' : '' }, x))));
      btn.onclick = async () => { btn.classList.add('busy'); try { await LBX.bench(+n.value, +t.value, c.value); } catch (e) { toast(e.message, 'bad'); } finally { btn.classList.remove('busy'); } };
      el.append(h('h4', {}, 'Reads served per replica (live STATS)', h('span', { class: 'right' }, h('button', { class: 'btn xs', onclick: () => run('STATS reset') }, 'reset counters'))), grid,
        h('div', { style: { display: 'flex', gap: '6px', alignItems: 'center', marginTop: '12px', flexWrap: 'wrap' } },
          h('span', { class: 'small muted' }, 'requests'), n, h('span', { class: 'small muted' }, 'clients'), t, c, btn),
        h('h4', { style: { marginTop: '14px' } }, 'Run history'), LBX.hist);
      let last = 0;
      const tick = async () => {
        if (Date.now() - last < 900 || !S.routerUp) return; last = Date.now();
        try { LBX.paint((await api('/api/cmd', { method: 'POST', body: 'STATS' })).resp); } catch (e) { }
      };
      return tick;
    },
    steps: [
      { t: 'Round robin spreads reads evenly', d: 'Reset counters, then 90 reads from 6 concurrent clients.',
        acts: [{ l: 'LB rr + bench 90', cls: 'accent', f: async c => { await c.cmd('LB rr'); await LBX.bench(90, 6, 'STOCK LAPTOP', c); } }], e: 'Each replica serves <b>≈33%</b>.' },
      { t: 'Baseline: no balancing', d: 'Every read goes to the primary.',
        acts: [{ l: 'LB primary + bench 90', f: async c => { await c.cmd('LB primary'); await LBX.bench(90, 6, 'STOCK LAPTOP', c); } }], e: '<b>100%</b> of reads on the primaries.' },
      { t: 'A slow replica: round robin vs least-connections', d: 'Add 300 ms to every read on one MUMBAI replica. Round robin still sends it a third; least-connections routes around it.',
        acts: [
          { l: 'slow m1 (+300 ms)', cls: 'danger', f: async c => { const id = S.procs.m1 ? 'm1' : NODES.MUMBAI.find(x => S.procs[x]); await c.cmd(`NODE ${id} DELAY 300`); } },
          { l: 'rr bench', f: async c => { await c.cmd('LB rr'); await LBX.bench(90, 6, 'STOCK LAPTOP', c); } },
          { l: 'leastconn bench', cls: 'accent', f: async c => { await c.cmd('LB leastconn'); await LBX.bench(90, 6, 'STOCK LAPTOP', c); } },
          { l: 'clear delay', cls: 'good', f: async c => { for (const id of Object.keys(S.delay)) await c.cmd(`NODE ${id} DELAY 0`); if (!Object.keys(S.delay).length) await c.cmd('NODE m1 DELAY 0'); } }],
        e: 'rr: higher <b>avg / p95</b>; leastconn: the slow node gets <b>~2%</b> and latency drops. Compare in the run history.' },
      { t: 'Failure-aware: kill a replica', d: 'Down replicas are excluded from the candidate list; reads are redistributed.',
        acts: [
          { l: 'kill a MUMBAI backup', cls: 'danger', f: async c => { const id = helpers.backups('MUMBAI')[0]; if (!id) throw new Error('no MUMBAI backup up'); await killNote(c, id); await sleep(1800); } },
          { l: 'rr bench', f: async c => { await c.cmd('LB rr'); await LBX.bench(90, 6, 'STOCK LAPTOP', c); } },
          { l: 'restart it', cls: 'good', f: async c => { for (const id of helpers.down('MUMBAI')) await startNote(c, id); } }],
        e: 'MUMBAI splits ≈50/50 between the two survivors.' },
      { t: 'Data-level balancing', d: '<code>BUY PEN 6000</code> needs more than one warehouse; it allocates from the warehouse with the most available stock first.',
        acts: [{ l: 'STOCK PEN', f: c => c.cmd('STOCK PEN') }, { l: 'BUY PEN 6000', f: c => c.cmd('BUY PEN 6000') }], e: 'A 2PC across shards, biggest stock first.' },
    ],
  },
];

// ------------------------------------------------------------------ loadgen (availability)
const LG = {
  running: false, buckets: [], ui: null, t0: 0, total: 0, err: 0, worst: 0,
  start(sec, rate) {
    if (this.running) return;
    this.running = true; this.buckets = []; this.total = this.err = this.worst = 0; this.t0 = performance.now();
    const end = this.t0 + sec * 1000, interval = Math.max(20, 1000 / rate);
    let seed = 42; const rnd = () => (seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff;
    if (this.ui) { this.ui.btn.textContent = 'Stop'; this.ui.btn.classList.remove('accent'); this.ui.btn.classList.add('danger'); }
    const loop = async () => {
      while (this.running && performance.now() < end) {
        const s = performance.now();
        const write = rnd() < .3;
        let ok = false;
        try { const d = await api('/api/cmd', { method: 'POST', body: write ? 'BUY PEN 1' : 'STOCK PEN' }); ok = d.resp.startsWith('OK') || d.resp.startsWith('ABORTED'); } catch (e) { ok = false; }
        const lat = performance.now() - s, sec = Math.floor((performance.now() - this.t0) / 1000);
        const b = this.buckets[sec] ||= { ok: 0, err: 0, max: 0 };
        ok ? b.ok++ : b.err++; b.max = Math.max(b.max, lat);
        this.total++; if (!ok) this.err++; this.worst = Math.max(this.worst, lat);
        const wait = interval - (performance.now() - s);
        if (wait > 0) await sleep(wait);
      }
      this.stop();
    };
    loop();
    this.timer = setInterval(() => this.draw(), 500);
  },
  stop() {
    if (!this.running) return;
    this.running = false; clearInterval(this.timer); this.draw();
    if (this.ui) { this.ui.btn.textContent = 'Start traffic'; this.ui.btn.classList.add('accent'); this.ui.btn.classList.remove('danger'); }
    toast(`loadgen done: ${this.total} requests, ${this.err} errors → availability ${(100 * (this.total - this.err) / Math.max(1, this.total)).toFixed(2)}%, worst ${Math.round(this.worst)} ms`, this.err ? 'bad' : 'ok', 7000);
  },
  draw() {
    const u = this.ui; if (!u || !u.chart.isConnected) return;
    u.k.req.textContent = this.total;
    u.k.err.textContent = this.err; u.k.err.className = 'v ' + (this.err ? 'bad' : 'ok');
    u.k.av.textContent = this.total ? (100 * (this.total - this.err) / this.total).toFixed(this.err ? 2 : 0) + '%' : '–';
    u.k.av.className = 'v ' + (this.err ? 'warn' : 'ok');
    u.k.lat.textContent = this.total ? Math.round(this.worst) + ' ms' : '–';
    u.k.lat.className = 'v ' + (this.worst > 1000 ? 'warn' : '');
    if (this.running) u.status.textContent = `running · t=${Math.floor((performance.now() - this.t0) / 1000)} s`;
    else if (this.total) u.status.textContent = 'finished';
    const W = 560, H = 150, padL = 26, padB = 16, n = 40;
    const ch = u.chart; ch.innerHTML = '';
    const data = Array.from(this.buckets.slice(-n));
    const maxOps = Math.max(12, ...data.map(b => b ? b.ok + b.err : 0));
    const maxLat = Math.max(400, ...data.map(b => b ? b.max : 0));
    for (let i = 0; i <= 3; i++) {
      const y = (H - padB) * i / 3;
      ch.append(svg('line', { class: 'grid', x1: padL, x2: W, y1: y + .5, y2: y + .5 }));
    }
    const tl = svg('text', { class: 'axis', x: 2, y: 9 }); tl.textContent = Math.round(maxLat) + 'ms'; ch.append(tl);
    const t2 = svg('text', { class: 'axis', x: 2, y: H - padB }); t2.textContent = '0'; ch.append(t2);
    const bw = (W - padL) / n;
    const linePts = [];
    data.forEach((b, i) => {
      if (!b) return;
      const x = padL + i * bw + 2, w = bw - 4, ih = H - padB;
      const hOk = ih * b.ok / maxOps, hErr = ih * b.err / maxOps;
      ch.append(svg('rect', { class: b.max > 1000 ? 'b-slow' : 'b-ok', x, width: w, y: ih - hOk, height: hOk, rx: 2 }));
      if (b.err) ch.append(svg('rect', { class: 'b-err', x, width: w, y: ih - hOk - hErr, height: hErr, rx: 2 }));
      linePts.push([x + w / 2, ih - ih * b.max / maxLat]);
    });
    if (linePts.length > 1) ch.append(svg('polyline', { class: 'lat', points: linePts.map(p => p.join(',')).join(' ') }));
    linePts.forEach(p => ch.append(svg('circle', { class: 'lat-dot', cx: p[0], cy: p[1], r: 2 })));
    const base = Math.max(0, this.buckets.length - n);
    for (let i = 0; i < n; i += 5) { const t = svg('text', { class: 'axis', x: padL + i * bw, y: H - 3 }); t.textContent = (base + i) + 's'; ch.append(t); }
  },
};

// ------------------------------------------------------------------ load-balancing bench
const LBX = {
  hist: null, runs: 0,
  paint(resp) {
    const per = {};
    for (const l of resp.split('\n')) {
      const m = l.match(/^\s+(\w+)\s+(\w+)\s+(\S+)\s+(\d+) reads\s+(\d+)%/);
      if (m) per[m[2]] = { wh: m[1], role: m[3], n: +m[4], pct: +m[5] };
    }
    for (const id of ALL) {
      const row = $('#lb-' + id), v = per[id]; if (!row || !v) continue;
      row.querySelector('i').style.width = v.pct + '%';
      row.querySelector('.r').textContent = `${v.n} · ${v.pct}%`;
      row.classList.toggle('down', v.role === 'DOWN');
      row.classList.toggle('prim', v.role === 'PRIMARY');
    }
    return per;
  },
  async bench(n, t, c, ctx) {
    await run('STATS reset');
    const r = await bench(n, t, c);
    const st = (await api('/api/cmd', { method: 'POST', body: 'STATS' })).resp;
    const per = this.paint(st);
    kick();
    const slow = Object.entries(S.delay).map(([k, v]) => `${k}+${v}`).join(' ') || '–';
    const dist = h('span', {}, ...WH.map(wh => h('span', { class: 'mini', style: { color: COLOR[wh], marginRight: '8px' }, title: wh },
      ...NODES[wh].map(id => h('i', { style: { height: Math.max(2, (per[id]?.pct || 0) / 5) + 'px', opacity: per[id]?.role === 'DOWN' ? .25 : 1 }, title: `${id} ${per[id]?.pct || 0}%` })))));
    if (this.hist) this.hist.append(h('tr', {}, h('td', {}, ++this.runs), h('td', {}, h('b', {}, S.lb)), h('td', { class: slow === '–' ? 'muted' : 't-bad' }, slow),
      h('td', { class: 'r' }, r.avg + 'ms'), h('td', { class: 'r' }, r.p95 + 'ms'), h('td', { class: 'r' }, r.max + 'ms'), h('td', {}, dist)));
    if (ctx) ctx.note(`bench ${n} × "${c}" with ${t} clients · policy=${S.lb} → ok=${r.ok} err=${r.err} · avg ${r.avg} ms · p95 ${r.p95} ms · max ${r.max} ms · ${r.rps} req/s`, r.err ? 'bad' : 'ok');
    if (ctx) ctx.el.append(renderResp('STATS', st));
    return r;
  },
};

// ------------------------------------------------------------------ topic rendering
let current = null, currentTick = null;
function buildTabs() {
  const tabs = $('#tabs');
  for (const t of TOPICS) tabs.append(h('button', { class: 'tab', 'data-id': t.id, style: { '--tc': t.color }, onclick: () => selectTopic(t.id) },
    h('span', { class: 'num' }, '0' + t.n), h('span', { class: 'name' }, t.title)));
  $$('.tab').forEach(b => b.style.setProperty('--tc', TOPICS.find(t => t.id === b.dataset.id).color));
}
function selectTopic(id) {
  const t = TOPICS.find(x => x.id === id);
  if (!t || current === id) return;
  current = id;
  try { localStorage.setItem('topic', id); } catch (e) { }
  $$('.tab').forEach(b => b.classList.toggle('on', b.dataset.id === id));
  if (currentTick) TOPIC_TICK.delete(currentTick);
  SEQ.el = SEQ.banner = null; REC.el = null;
  const p = $('#panel');
  p.style.setProperty('--tc', t.color);
  p.innerHTML = '';
  p.classList.remove('panel-enter'); void p.offsetWidth; p.classList.add('panel-enter');
  p.append(h('div', { class: 'p-head' }, h('div', { class: 'p-num' }, '0' + t.n), h('div', {}, h('h3', { class: 'p-title' }, t.title), h('div', { class: 'p-tag' }, t.tag))),
    h('div', { class: 'concepts' }, ...t.concepts.map(c => h('span', { class: 'concept' }, c))),
    h('div', { class: 'where', html: 'implemented in <b>' + esc(t.where) + '</b>' }));
  const w = h('div', { class: 'widget' });
  p.append(w);
  currentTick = t.widget(w);
  if (currentTick) TOPIC_TICK.add(currentTick);
  const steps = h('div', { class: 'steps' });
  for (const s of t.steps) {
    const out = h('div', { class: 'out' });
    const step = h('div', { class: 'step' }, h('div', { class: 'step-title' }, s.t), h('div', { class: 'step-desc', html: s.d }));
    const acts = h('div', { class: 'step-acts' });
    for (const a of s.acts) {
      const b = h('button', { class: 'btn sm ' + (a.cls || '') }, a.l);
      b.onclick = () => act(b, step, out, a.f);
      acts.append(b);
    }
    step.append(acts);
    if (s.e) step.append(h('div', { class: 'expect', html: 'Expect: ' + s.e }));
    step.append(out);
    steps.append(step);
  }
  p.append(steps);
  p.scrollTop = 0;
}

// ============================================================================ console
function setupConsole() {
  const hist = []; let hi = 0;
  const inp = $('#console-input'), out = $('#console-out');
  $('#console-form').onsubmit = async e => {
    e.preventDefault();
    const c = inp.value.trim(); if (!c) return;
    hist.push(c); hi = hist.length; inp.value = '';
    try {
      if (/^clear$/i.test(c)) { out.innerHTML = ''; return; }
      const d = await run(c);
      out.append(renderResp(c, d.resp, d.ms));
    } catch (err) { out.append(renderResp(c, 'ERR ' + err.message)); }
    while (out.children.length > 30) out.firstChild.remove();
    out.scrollTop = out.scrollHeight;
  };
  inp.onkeydown = e => {
    if (e.key === 'ArrowUp' && hi > 0) { inp.value = hist[--hi]; e.preventDefault(); }
    if (e.key === 'ArrowDown') { hi = Math.min(hist.length, hi + 1); inp.value = hist[hi] || ''; e.preventDefault(); }
  };
  $('#console-toggle').onclick = e => { const c = $('#console'); c.classList.toggle('collapsed'); e.target.textContent = c.classList.contains('collapsed') ? 'show' : 'collapse'; };
  $('#console-big').onclick = e => {
    const c = $('#console'); c.classList.remove('collapsed'); $('#console-toggle').textContent = 'collapse';
    out.style.height = '';                                   // drop any manual drag size
    c.classList.toggle('big'); e.target.textContent = c.classList.contains('big') ? 'shrink' : 'expand';
    out.scrollTop = out.scrollHeight;
  };
  $('#console-clear').onclick = () => { out.innerHTML = ''; };
}

// ============================================================================ header controls
function consoleNote(c, resp) { const out = $('#console-out'); out.append(renderResp(c, resp)); out.scrollTop = out.scrollHeight; }
async function freshStart(btn) {
  btn && btn.classList.add('busy');
  try {
    toast('wiping data and starting 10 processes…', 'warn');
    await cluster('reset'); EV.off = {}; $('#events').innerHTML = '';
    ALL.forEach(id => Object.assign(S.nodes[id], { up: false, role: '?', epoch: '-', lsn: 0, served: 0 })); S.primary = {}; S.prev = null;
    await cluster('start');
    if (!(await helpers.waitPrimaries(20000))) throw new Error('timed out waiting for elections');
    consoleNote('seed demo/seed.txt', 'OK\n' + await cluster('seed'));
    toast('cluster ready · catalogue loaded', 'ok');
  } catch (e) { toast(e.message, 'bad', 7000); }
  finally { btn && btn.classList.remove('busy'); kick(); }
}
function setupHeader() {
  $$('#seg-mode button').forEach(b => b.onclick = async () => { const d = await run('MODE ' + b.dataset.v); toast(d.resp, d.resp.startsWith('OK') ? 'info' : 'bad', 2500); });
  $$('#seg-lb button').forEach(b => b.onclick = async () => { const d = await run('LB ' + b.dataset.v); toast(d.resp, 'info', 2500); });
  $('#btn-start').onclick = async e => { e.target.classList.add('busy'); try { toast(await cluster('start'), 'ok', 3000); } finally { e.target.classList.remove('busy'); } };
  $('#btn-stop').onclick = async e => { e.target.classList.add('busy'); try { await cluster('stop'); toast('every process killed', 'bad'); } finally { e.target.classList.remove('busy'); } };
  $('#btn-seed').onclick = async e => { e.target.classList.add('busy'); try { consoleNote('seed demo/seed.txt', 'OK\n' + await cluster('seed')); toast('catalogue loaded', 'ok'); } finally { e.target.classList.remove('busy'); } };
  const fresh = $('#btn-fresh'); let armed = 0;
  fresh.onclick = () => {
    if (Date.now() - armed > 3000) { armed = Date.now(); fresh.textContent = 'Wipe all data?'; setTimeout(() => fresh.textContent = 'Fresh start', 3000); return; }
    armed = 0; fresh.textContent = 'Fresh start'; freshStart(fresh);
  };
  $$('#empty [data-act]').forEach(b => b.onclick = async () => {
    if (b.dataset.act === 'fresh') return freshStart(b);
    b.classList.add('busy'); try { await cluster('start'); } finally { b.classList.remove('busy'); }
  });
}

// ============================================================================ boot
buildTopology();
buildTabs();
setupEvents();
setupConsole();
setupHeader();
let saved = 'repl'; try { saved = localStorage.getItem('topic') || 'repl'; } catch (e) { }
selectTopic(TOPICS.some(t => t.id === saved) ? saved : 'repl');
window.addEventListener('resize', () => drawWires());
poll();
tailLoop();
// keyboard: 1-6 switch topics
document.addEventListener('keydown', e => {
  if (e.target.matches('input, select, textarea')) return;
  const t = TOPICS[+e.key - 1]; if (t) selectTopic(t.id);
});
