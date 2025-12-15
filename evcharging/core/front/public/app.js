const $ = (id) => document.getElementById(id);

const cpTbody = $("cpTbody");
const cpFilter = $("cpFilter");
const refreshBtn = $("refreshBtn");
const connBadge = $("connBadge");

const logBox = $("logBox");
const logFilter = $("logFilter");
const clearLogs = $("clearLogs");

let cpsCache = [];
let logs = [];

// Backend real de SD (HTTPS)
const SD_BASE = "https://localhost:5000";

function badge(text, ok = true) {
  connBadge.textContent = text;
  connBadge.className = "badge " + (ok ? "ok" : "bad");
}

function safeStr(v) {
  if (v === undefined || v === null) return "—";
  if (typeof v === "object") return JSON.stringify(v);
  const s = String(v);
  return s.length ? s : "—";
}

// ---------- LOGS ----------
function renderLogs() {
  const f = logFilter.value;
  const items = logs.filter(l => (f === "all" ? true : (l.type || "info") === f));

  if (!items.length) {
    logBox.innerHTML = `<div class="muted">Sin logs</div>`;
    return;
  }

  logBox.innerHTML = items.map(l => {
    const type = l.type || "info";
    const cls = `log ${type}`;

    const ts = safeStr(l.ts);
    const source = safeStr(l.source || "central_audit");
    const ip = safeStr(l.ip);
    const action = safeStr(l.action);
    const params = safeStr(l.params);

    return `
      <div class="${cls}">
        <div class="log-head">
          <span class="mono small">${ts}</span>
          <span class="pill">${type}</span>
          <span class="muted">· ${source}</span>
        </div>
        <div class="log-msg">
          <span class="mono small">${ip}</span>
          <span class="muted">·</span>
          <b>${action}</b>
        </div>
        ${params !== "—" ? `<pre class="log-payload">${params}</pre>` : ""}
      </div>
    `;
  }).join("");
}

async function loadLogs() {
  try {
    const r = await fetch("/api/logs?limit=300");
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();

    const items = Array.isArray(data.items) ? data.items : [];
    // newest-first
    logs = items.slice().reverse();
    renderLogs();
  } catch (e) {
    logs = [{
      ts: new Date().toISOString(),
      source: "front",
      type: "error",
      ip: "—",
      action: "LOAD_LOGS_ERROR",
      params: safeStr(e.message),
    }];
    renderLogs();
  }
}

// ---------- CPS ----------
function renderCPs(items) {
  const q = (cpFilter.value || "").toLowerCase().trim();

  const filtered = items.filter(cp => {
    const blob = `${cp.id ?? ""} ${cp.state ?? ""} ${cp.location ?? ""}`.toLowerCase();
    return !q || blob.includes(q);
  });

  if (!filtered.length) {
    cpTbody.innerHTML = `<tr><td colspan="5" class="muted">Sin resultados</td></tr>`;
    return;
  }

  cpTbody.innerHTML = filtered.map(cp => {
    const id = safeStr(cp.id);
    const state = safeStr(cp.state);
    const location = safeStr(cp.location);
    const price = safeStr(cp.price);

    return `
      <tr>
        <td class="mono">${id}</td>
        <td><span class="pill">${state}</span></td>
        <td>${location}</td>
        <td class="mono">${price}</td>
      </tr>
    `;
  }).join("");
}

async function loadCPs() {
  try {
    badge("Cargando…", true);

    const r = await fetch("/api/cps");
    if (!r.ok) throw new Error(`HTTP ${r.status}`);

    const cps = await r.json();
    cpsCache = Array.isArray(cps) ? cps : [];
    renderCPs(cpsCache);

    badge("Conectado", true);
  } catch (e) {
    badge("Error CPs", false);
    cpTbody.innerHTML = `<tr><td colspan="5" class="muted">Error cargando CPs: ${safeStr(e.message)}</td></tr>`;
  }
}


// ---------- UI ----------
cpFilter.addEventListener("input", () => renderCPs(cpsCache));
refreshBtn.addEventListener("click", () => { loadCPs(); loadLogs(); });
clearLogs.addEventListener("click", () => { logs = []; renderLogs(); });

// ---------- INIT ----------
loadCPs();
loadLogs();
setInterval(loadCPs, 10000);
setInterval(loadLogs, 2000);
