// alpheus ui: the runs, one run, and one replica with its logs, error log
// and failures, live from the cluster and the runs' storage (GO-142).
"use strict";

const main = document.getElementById("main");
const nsInput = document.getElementById("ns");
const live = document.getElementById("live");
const token = document.querySelector('meta[name="alpheus-token"]').content;
const replayMode = document.querySelector('meta[name="alpheus-replay"]').content;
let namespace = document.querySelector('meta[name="alpheus-namespace"]').content;
let filter = "all";
let source = null;
let pending = null;
let state = {}; // what the replica view keeps between renders

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
}
const enc = encodeURIComponent;

async function getJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error((await r.text()).trim() || r.statusText);
  return r.json();
}

async function getText(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error((await r.text()).trim() || r.statusText);
  return r.text();
}

function ago(t) {
  if (!t) return "–";
  const s = Math.max(0, (Date.now() - new Date(t).getTime()) / 1000);
  if (s < 60) return Math.round(s) + "s ago";
  if (s < 3600) return Math.round(s / 60) + "m ago";
  if (s < 86400) return Math.round(s / 3600) + "h ago";
  return Math.round(s / 86400) + "d ago";
}

function duration(a, b) {
  if (!a) return "–";
  const s = Math.max(0, ((b ? new Date(b) : new Date()) - new Date(a)) / 1000);
  if (s < 60) return Math.round(s) + "s";
  if (s < 3600) return Math.floor(s / 60) + "m " + Math.round(s % 60) + "s";
  return Math.floor(s / 3600) + "h " + Math.round((s % 3600) / 60) + "m";
}

function phase(p) {
  return `<span class="phase ${esc(p || "Pending")}">${esc(p || "Pending")}</span>`;
}

const finished = p => ["Succeeded", "PartiallySucceeded", "Failed", "Cancelled"].includes(p);

// shown is a step's or replica's phase as the page shows it: one that had
// not finished when its run ended was stopped with the run.
function shown(p, run) {
  return finished(run.status?.phase) && (!p || p === "Running" || p === "Pending") ? "Stopped" : (p || "Pending");
}

function bytes(n) {
  const u = ["B", "KiB", "MiB", "GiB", "TiB"];
  let i = 0;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return (i ? n.toFixed(1) : n) + " " + u[i];
}

// --- Live updates ----------------------------------------------------------

function watch() {
  if (source) source.close();
  source = new EventSource("/api/watch?namespace=" + enc(namespace));
  source.onopen = () => live.classList.add("on");
  source.onerror = () => live.classList.remove("on");
  source.onmessage = () => {
    // Many changes come together; render once for them.
    clearTimeout(pending);
    pending = setTimeout(() => render(false), 300);
  };
}

// --- Views -------------------------------------------------------------------

async function runsView() {
  const runs = await getJSON("/api/runs?namespace=" + enc(namespace));
  const shown = runs.filter(r => filter === "all" ||
    (filter === "running" && (r.phase === "Running" || r.phase === "Pending" || !r.phase)) ||
    (filter === "failed" && (r.phase === "Failed" || r.phase === "PartiallySucceeded")));
  const chip = f => `<button type="button" data-filter="${f}" aria-pressed="${filter === f}">${f}</button>`;
  const all = namespace === "*";
  main.innerHTML = `<h1>Runs in ${all ? "every namespace" : esc(namespace)}</h1>
    <div class="chips" role="group" aria-label="Filter">${chip("all")}${chip("running")}${chip("failed")}</div>
    ${shown.length === 0 ? `<p class="muted">No runs${filter === "all" ? "" : " here with this filter"}.</p>` : `
    <table><thead><tr>${all ? "<th>Namespace</th>" : ""}<th>Run</th><th>Phase</th><th>Steps</th><th>Started</th><th>Duration</th><th>Reason</th></tr></thead><tbody>
    ${shown.map(r => `<tr class="click" data-href="#/runs/${enc(r.namespace)}/${enc(r.name)}">
      ${all ? `<td>${esc(r.namespace)}</td>` : ""}<td><a href="#/runs/${enc(r.namespace)}/${enc(r.name)}">${esc(r.name)}</a></td><td>${phase(r.phase)}</td>
      <td>${esc(r.stepsFinished || "–")}</td><td>${ago(r.startedAt || r.created)}</td>
      <td>${duration(r.startedAt, r.finishedAt)}</td><td>${esc(r.reason || "")}</td></tr>`).join("")}
    </tbody></table>`}`;
  main.querySelectorAll("[data-filter]").forEach(b => b.onclick = () => { filter = b.dataset.filter; render(false); });
}

function edgeName(e) {
  return !e.output || e.output === "default" ? `${e.from}-${e.to}` : `${e.from}.${e.output}-${e.to}`;
}

// flow draws the run's steps as boxes, by level from the sources, with a
// dot per replica and the edges' chunks and lag on the arrows.
function flow(run) {
  const steps = run.spec.steps, edges = run.spec.edges || [];
  const status = Object.fromEntries((run.status.steps || []).map(s => [s.name, s]));
  const edgeStatus = Object.fromEntries((run.status.edges || []).map(e => [e.name, e]));
  const level = {};
  const lv = name => {
    if (level[name] !== undefined) return level[name];
    level[name] = 0; // guards a cycle, which the API refuses anyway
    const ins = edges.filter(e => e.to === name);
    return level[name] = ins.length ? 1 + Math.max(...ins.map(e => lv(e.from))) : 0;
  };
  steps.forEach(s => lv(s.name));
  const cols = {};
  steps.forEach(s => (cols[level[s.name]] ||= []).push(s.name));
  const BW = 170, BH = 86, GX = 110, GY = 40;
  const pos = {};
  let rows = 0;
  Object.entries(cols).forEach(([l, names]) => {
    names.forEach((n, i) => pos[n] = { x: 20 + l * (BW + GX), y: 20 + i * (BH + GY) });
    rows = Math.max(rows, names.length);
  });
  const W = 40 + Object.keys(cols).length * (BW + GX) - GX, H = 60 + rows * (BH + GY);
  let svg = `<svg class="flow" viewBox="0 0 ${W} ${H}" role="img" aria-label="The run's steps and edges">
    <defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="currentColor"/></marker></defs>`;
  edges.forEach(e => {
    const a = pos[e.from], b = pos[e.to];
    if (!a || !b) return;
    const x1 = a.x + BW, y1 = a.y + BH / 2, x2 = b.x - 4, y2 = b.y + BH / 2;
    svg += `<path class="edge" d="M${x1},${y1} C${x1 + 50},${y1} ${x2 - 50},${y2} ${x2},${y2}"/>`;
    const es = edgeStatus[edgeName(e)];
    if (es) {
      const mx = (x1 + x2) / 2, my = (y1 + y2) / 2;
      svg += `<text class="small" x="${mx}" y="${my - 8}" text-anchor="middle">${es.chunks} chunks</text>
              <text class="small" x="${mx}" y="${my + 16}" text-anchor="middle">lag ${es.chunks - (es.acked || 0)}${es.eos ? " · done" : ""}</text>`;
    }
  });
  steps.forEach(s => {
    const p = pos[s.name], st = status[s.name] || {};
    svg += `<rect class="box ${esc(shown(st.phase, run))}" x="${p.x}" y="${p.y}" width="${BW}" height="${BH}" rx="6"/>
      <text class="name" x="${p.x + BW / 2}" y="${p.y + 30}" text-anchor="middle">${esc(s.name)}</text>`;
    (st.replicas || []).forEach((rs, i, all) => {
      const cx = p.x + BW / 2 + (i - (all.length - 1) / 2) * 22;
      svg += `<a href="#/runs/${enc(run.metadata.namespace)}/${enc(run.metadata.name)}/${enc(s.name)}/${rs.index}">
        <circle class="dot ${esc(shown(rs.phase, run))}" cx="${cx}" cy="${p.y + 58}" r="7"><title>${esc(s.name)} replica ${rs.index}: ${esc(shown(rs.phase, run))}${rs.reason ? ", " + esc(rs.reason) : ""}</title></circle></a>`;
    });
  });
  svg += `<line class="ground" x1="0" y1="${H - 12}" x2="${W}" y2="${H - 12}"/></svg>`;
  return svg;
}

async function runView(ns, name) {
  const { run, events } = await getJSON(`/api/runs/${enc(ns)}/${enc(name)}`);
  const st = run.status || {};
  const rows = [];
  (st.steps || []).forEach(s => (s.replicas || []).forEach(rs => rows.push({ step: s.name, ...rs })));
  main.innerHTML = `<p><a href="#/">← Runs</a></p>
    <h1>Run ${esc(name)}</h1>
    <div class="card">
      <div class="status">${phase(st.phase)}${st.reason ? ": " + esc(st.reason) : ""}</div>
      ${st.message ? `<p class="message">${esc(st.message)}</p>` : ""}
      <p class="muted">Steps finished ${esc(st.stepsFinished || "–")} · started ${ago(st.startedAt)} · ${duration(st.startedAt, st.finishedAt)}</p>
    </div>
    <h2>Flow</h2>
    <div class="card">${flow(run)}</div>
    <h2>Replicas</h2>
    <table><thead><tr><th>Step</th><th>Replica</th><th>Phase</th><th>Attempt</th><th>Reason</th><th>Message</th></tr></thead><tbody>
    ${rows.map(r => `<tr class="click" data-href="#/runs/${enc(ns)}/${enc(name)}/${enc(r.step)}/${r.index}">
      <td><a href="#/runs/${enc(ns)}/${enc(name)}/${enc(r.step)}/${r.index}">${esc(r.step)}</a></td><td>${r.index}</td>
      <td>${phase(shown(r.phase, run))}</td><td>${r.attempt || 1}${r.restarts ? ` (${r.restarts} restarts)` : ""}</td>
      <td>${esc(r.reason || "")}</td><td>${esc(r.message || "")}${r.failedChunks ? ` (${r.failedChunks} chunks not processed)` : ""}</td></tr>`).join("")}
    </tbody></table>
    ${(st.results || []).length ? `<h2>Results</h2><table><thead><tr><th>Result</th><th>Chunks</th><th>Bytes</th><th>Where</th></tr></thead><tbody>
      ${st.results.map(r => `<tr><td>${esc(r.step)}.${esc(r.output)}</td><td>${r.chunks}</td><td>${bytes(r.bytes || 0)}</td><td><code>${esc(r.bucket)}/${esc(r.prefix)}</code></td></tr>`).join("")}
      </tbody></table>` : ""}
    <h2>Events</h2>
    ${events.length ? `<table class="events"><tbody>${events.map(e => `<tr><td>${new Date(e.time).toLocaleTimeString()}</td>
      <td class="${esc(e.type)}">${esc(e.reason)}</td><td>${esc(e.note)}</td></tr>`).join("")}</tbody></table>`
      : `<p class="muted">No events.</p>`}`;
}

async function replicaView(ns, name, step, replica, fresh) {
  const key = [ns, name, step, replica].join("/");
  if (fresh || state.key !== key) state = { key, tab: "logs", attempt: null, container: "component" };
  const [{ run }, attempts] = await Promise.all([
    getJSON(`/api/runs/${enc(ns)}/${enc(name)}`),
    getJSON(`/api/runs/${enc(ns)}/${enc(name)}/attempts?step=${enc(step)}&replica=${replica}`).catch(() => []),
  ]);
  const ss = (run.status.steps || []).find(s => s.name === step) || {};
  const rs = (ss.replicas || []).find(r => String(r.index) === String(replica)) || {};
  // A finished run's pods are gone, so only kept logs are left.
  const live = Boolean(rs.pod) && !finished(run.status.phase);
  if (state.attempt === null) state.attempt = live ? 0 : (attempts.find(a => a.logs) || {}).attempt || 0;
  const q = `step=${enc(step)}&replica=${replica}`;
  const tab = (id, label) => `<button type="button" role="tab" data-tab="${id}" aria-selected="${state.tab === id}">${label}</button>`;
  const options = (live ? [`<option value="0"${state.attempt === 0 ? " selected" : ""}>attempt ${rs.attempt} (live)</option>`] : [])
    .concat(attempts.filter(a => a.logs).map(a => `<option value="${a.attempt}"${state.attempt === a.attempt ? " selected" : ""}>attempt ${a.attempt} (kept)</option>`));
  let body = "";
  try {
    if (state.tab === "counts") {
      const counts = Object.entries(ss.counts || {});
      body = `<div class="counts">${counts.map(([u, n]) => `<div><b>${n.toLocaleString()}</b>${esc(u)}</div>`).join("") || `<p class="muted">No counts yet.</p>`}
        <div><b>${ss.errors || 0}</b>errors</div><div><b>${ss.warnings || 0}</b>warnings</div></div>
        <p class="muted">The step's committed counts, summed over its replicas: exact, also after restarts.</p>`;
    } else if (state.tab === "errors") {
      const entries = await getJSON(`/api/runs/${enc(ns)}/${enc(name)}/errors?${q}`);
      body = entries.length ? `<table><thead><tr><th>Level</th><th>Code</th><th>Message</th><th>Chunk</th></tr></thead><tbody>
        ${entries.map(e => `<tr><td class="${e.level === "error" ? "Warning" : ""}">${esc(e.level)}</td><td><code>${esc(e.code)}</code></td>
        <td>${esc(e.message)}${e.fields ? `<br><span class="muted">${esc(JSON.stringify(e.fields))}</span>` : ""}</td><td>${esc(e.chunk || "")}</td></tr>`).join("")}
        </tbody></table>` : `<p class="muted">The replica's error log is empty.</p>`;
    } else if (state.tab === "logs") {
      if (!options.length) {
        body = `<p class="muted">No logs: the replica has no pod now, and no failed attempt's logs are kept.</p>`;
      } else {
        const text = await getText(`/api/runs/${enc(ns)}/${enc(name)}/logs?${q}&attempt=${state.attempt}&container=${state.container}`);
        body = `<pre>${esc(text) || '<span class="muted">The log is empty.</span>'}</pre>
          <p class="muted">${state.attempt ? "Kept in the bucket when the attempt failed, so the pod can be gone." : "The last 2000 lines of the running pod."}</p>`;
      }
    } else if (state.tab === "failures") {
      const kept = attempts.filter(a => a.failure);
      if (!kept.length) {
        body = `<p class="muted">No failed attempt is kept for this replica.</p>`;
      } else {
        const a = state.failure || kept[0].attempt;
        const f = await getJSON(`/api/runs/${enc(ns)}/${enc(name)}/failure?${q}&attempt=${a}`);
        body = `<div class="tabs"><select id="failure" aria-label="Attempt">${kept.map(k => `<option value="${k.attempt}"${k.attempt === a ? " selected" : ""}>attempt ${k.attempt}</option>`).join("")}</select></div>
          <div class="card"><div class="status">${esc(f.reason)}</div><p class="message">${esc(f.message)}</p>
          <p class="muted">Image <code>${esc(f.image)}</code></p>
          <p>${f.inputs.length} input chunks kept: ${f.inputs.map(i => `<code>${esc(i.file)}</code>`).join(" ") || "none (a source)"}</p>
          ${replayMode === "off" ? `<p class="muted">Replay is off here. Run <code>alpheus replay ${esc(name)} ${esc(step)} --replica ${replica} --attempt ${a}</code> yourself.</p>` :
          `<p><button type="button" class="primary" id="replay" data-attempt="${a}">Replay attempt ${a} ▸</button>
          <span class="muted">Runs <code>alpheus replay ${esc(name)} ${esc(step)} --replica ${replica} --attempt ${a} --with ${esc(replayMode)}</code>: the same image on these chunks, at debug level, ${replayMode === "cluster" ? "as a pod in the run's namespace" : "with " + esc(replayMode) + " on the machine that runs alpheus ui"}.</span></p>`}</div>
          <pre id="replay-out" hidden></pre>`;
      }
    }
  } catch (err) {
    body = `<p class="Warning">${esc(err.message)}</p>`;
  }
  main.innerHTML = `<p><a href="#/runs/${enc(ns)}/${enc(name)}">← Run ${esc(name)}</a></p>
    <h1>${esc(step)}, replica ${esc(replica)}</h1>
    <div class="card"><div class="status">${phase(shown(rs.phase, run))}${rs.reason ? ": " + esc(rs.reason) : ""}</div>
      ${rs.message ? `<p class="message">${esc(rs.message)}</p>` : ""}
      <p class="muted">Attempt ${rs.attempt || 1} · ${rs.restarts || 0} restarts${live ? " · pod " + esc(rs.pod) : ""}</p></div>
    <div class="tabs" role="tablist">${tab("counts", "Counts")}${tab("errors", "Error log")}${tab("logs", "Logs")}${tab("failures", "Failures")}
      ${state.tab === "logs" && options.length ? `<select id="attempt" aria-label="Attempt">${options.join("")}</select>
      <select id="container" aria-label="Container"><option${state.container === "component" ? " selected" : ""}>component</option><option${state.container === "agent" ? " selected" : ""}>agent</option></select>` : ""}
    </div>${body}`;
  main.querySelectorAll("[data-tab]").forEach(b => b.onclick = () => { state.tab = b.dataset.tab; render(false); });
  const at = document.getElementById("attempt");
  if (at) at.onchange = () => { state.attempt = Number(at.value); render(false); };
  const ct = document.getElementById("container");
  if (ct) ct.onchange = () => { state.container = ct.value; render(false); };
  const fs = document.getElementById("failure");
  if (fs) fs.onchange = () => { state.failure = Number(fs.value); render(false); };
  const rp = document.getElementById("replay");
  if (rp) rp.onclick = () => replay(ns, name, step, replica, rp.dataset.attempt);
}

// replay asks the CLI to run alpheus replay and shows its output as it
// comes. The page does not re-render meanwhile.
async function replay(ns, name, step, replica, attempt) {
  const out = document.getElementById("replay-out");
  out.hidden = false;
  out.textContent = "";
  state.replaying = true;
  try {
    const r = await fetch(`/api/runs/${enc(ns)}/${enc(name)}/replay?step=${enc(step)}&replica=${replica}&attempt=${attempt}`,
      { method: "POST", headers: { "X-Alpheus-Token": token } });
    const reader = r.body.getReader();
    const dec = new TextDecoder();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      out.textContent += dec.decode(value, { stream: true });
      out.scrollTop = out.scrollHeight;
    }
  } catch (err) {
    out.textContent += "\n" + err.message;
  } finally {
    state.replaying = false;
  }
}

// --- Routing -------------------------------------------------------------------

async function render(fresh) {
  if (state.replaying) return;
  const parts = location.hash.replace(/^#\/?/, "").split("/").filter(Boolean).map(decodeURIComponent);
  try {
    if (parts[0] === "runs" && parts.length === 5) await replicaView(parts[1], parts[2], parts[3], parts[4], fresh);
    else if (parts[0] === "runs" && parts.length === 3) await runView(parts[1], parts[2]);
    else await runsView();
  } catch (err) {
    main.innerHTML = `<p class="Warning">${esc(err.message)}</p><p><a href="#/">← Runs</a></p>`;
  }
  main.querySelectorAll("tr[data-href]").forEach(tr => tr.onclick = ev => {
    if (ev.target.closest("a")) return;
    location.hash = tr.dataset.href;
  });
}

// The header's button switches between the light and the dark look.
const themeButton = document.getElementById("theme");
function showTheme() {
  const dark = document.documentElement.dataset.theme === "dark";
  themeButton.setAttribute("aria-pressed", String(dark));
  themeButton.textContent = dark ? "light" : "dark";
  themeButton.title = dark ? "Switch to the light look" : "Switch to the dark look";
}
// rain draws the Matrix's falling code behind the dark look, about 20
// frames a second, and not at all when the viewer asks for less motion or
// the page is hidden.
const rain = (() => {
  const canvas = document.getElementById("rain");
  const ctx = canvas.getContext("2d");
  const glyphs = "アイウエオカキクケコサシスセソタチツテトナニヌネノハヒフヘホマミムメモヤユヨラリルレロワン0123456789ABCDEF";
  const size = 16;
  let drops = [], timer = null;
  const still = window.matchMedia("(prefers-reduced-motion: reduce)");
  function resize() {
    canvas.width = innerWidth;
    canvas.height = innerHeight;
    drops = Array.from({ length: Math.ceil(innerWidth / size) }, () => Math.random() * -innerHeight / size);
  }
  function frame() {
    ctx.fillStyle = "rgba(2, 8, 5, 0.12)";
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    ctx.font = size + "px monospace";
    drops.forEach((y, i) => {
      const ch = glyphs[Math.floor(Math.random() * glyphs.length)];
      ctx.fillStyle = Math.random() < 0.06 ? "#d8ffe6" : "#36f07a";
      ctx.fillText(ch, i * size, y * size);
      drops[i] = y * size > canvas.height && Math.random() > 0.975 ? 0 : y + 1;
    });
  }
  function update() {
    const on = document.documentElement.dataset.theme === "dark" && !still.matches && !document.hidden;
    if (on && !timer) { resize(); timer = setInterval(frame, 50); }
    if (!on && timer) { clearInterval(timer); timer = null; ctx.clearRect(0, 0, canvas.width, canvas.height); }
  }
  addEventListener("resize", () => timer && resize());
  document.addEventListener("visibilitychange", update);
  still.addEventListener("change", update);
  return update;
})();

themeButton.onclick = () => {
  const dark = document.documentElement.dataset.theme !== "dark";
  if (dark) document.documentElement.dataset.theme = "dark";
  else delete document.documentElement.dataset.theme;
  try { localStorage.setItem("alpheus-theme", dark ? "dark" : "light"); } catch (e) { /* no storage: this visit only */ }
  showTheme();
  rain();
};
showTheme();
rain();

nsInput.value = namespace;
document.getElementById("ns-form").onsubmit = ev => {
  ev.preventDefault();
  namespace = nsInput.value.trim() || namespace;
  watch();
  location.hash = "#/";
  render(true);
};
window.addEventListener("hashchange", () => { render(true); main.focus(); });
watch();
render(true);
