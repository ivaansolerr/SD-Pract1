const $ = (id) => document.getElementById(id);

const cpTbody = $("cpTbody");
const cpFilter = $("cpFilter");
const refreshBtn = $("refreshBtn");
const connBadge = $("connBadge");

const logBox = $("logBox");
const logFilter = $("logFilter");
const clearLogs = $("clearLogs");

const stateDialog = $("stateDialog");
const dialogCpId = $("dialogCpId");
const newStateInput = $("newStateInput");
const saveStateBtn = $("saveStateBtn");
const dialogMsg = $("dialogMsg");

let cpsCache = [];
let selectedCpId = null;
let logs = [];

function badge(text, ok = true) {
  connBadge.textContent = text;
  connBadge.className = "badge " + (ok ? "ok" : "bad");
}

function safeStr(v) {
  if (v === undefined || v === null) return "—";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

function renderCPs(items) {
  const q = (cpFilter.value || "").toLowerCase().trim();
  const filtered = items.filter(cp => {
    const blob = `${cp.id ?? ""} ${cp.state ?? ""} ${cp.registered ?? ""} ${cp.activated ?? ""}`.toLowerCase();
    return !q || blob.includes(q);
  });

  if (!filtered.length) {
    cpTbody.innerHTML = `<tr><td colspan="6" class="muted">Sin resultados</td></tr>`;
    return;
  }

  cpTbody.innerHTML = filtered.map(cp => {
    const id = safeStr(cp.id);
    const state = safeStr(cp.state);

    // Estos campos dependen de tu esquema en Mongo. Si no existen, se verán como "—".
    const registered = safeStr(cp.registered ?? cp.is_registered ?? cp.registration?.status);
    const activated  = safeStr(cp.activated  ?? cp.is_activated  ?? cp.activation?.status);
    const token      = safeStr(cp.token      ?? cp.auth_token);

    return `
      <tr>
        <td class="mono">${id}</td>
        <td><span class="pill">${state}</span></td>
        <td>${registered}</td>
        <td>${activated}</td>
        <td class="mono small">${token}</td>
        <td>
          <button class="btn btn-small" data-action="change" data-id="${encodeURIComponent(id)}">Cambiar state</button>
        </td>
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
    badge("Error API", false);
    cpTbody.innerHTML = `<tr><td colspan="6" class="muted">Error cargando CPs: ${safeStr(e.message)}</td></tr>`;
    pushLog({ type: "error", message: `Error cargando CPs: ${safeStr(e.message)}`, source: "front" });
  }
}

function pushLog(evt) {
  logs.unshift(evt);
  if (logs.length > 300) logs.pop();
  renderLogs();
}

function renderLogs() {
  const f = logFilter.value;
  const items = logs.filter(l => (f === "all" ? true : l.type === f));

  if (!items.length) {
    logBox.innerHTML = `<div class="muted">Sin logs</div>`;
    return;
  }

  logBox.innerHTML = items.map(l => {
    const cls = `log ${l.type || "info"}`;
    const ts = safeStr(l.ts);
    const source = safeStr(l.source);
    const msg = safeStr(l.message);
    const payload = l.payload ? safeStr(l.payload) : "";
    return `
      <div class="${cls}">
        <div class="log-head">
          <span class="mono small">${ts}</span>
          <span class="pill">${l.type}</span>
          <span class="muted">· ${source}</span>
        </div>
        <div class="log-msg">${msg}</div>
        ${payload ? `<pre class="log-payload">${payload}</pre>` : ""}
      </div>
    `;
  }).join("");
}

// ====== Cambiar state CP (UI) ======
cpTbody.addEventListener("click", (e) => {
  const btn = e.target.closest("button[data-action='change']");
  if (!btn) return;
  selectedCpId = decodeURIComponent(btn.dataset.id);
  dialogCpId.textContent = `CP: ${selectedCpId}`;
  dialogMsg.textContent = "";
  newStateInput.value = "";
  stateDialog.showModal();
});

saveStateBtn.addEventListener("click", async (e) => {
  // si el input está vacío, deja que el required actúe
  if (!newStateInput.value.trim() || !selectedCpId) return;

  e.preventDefault(); // evitamos cerrar hasta que termine
  dialogMsg.textContent = "Guardando…";

  try {
    const r = await fetch(`/api/changeState/${encodeURIComponent(selectedCpId)}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ state: newStateInput.value.trim() })
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(data?.message || `HTTP ${r.status}`);

    dialogMsg.textContent = "✅ Estado actualizado";
    pushLog({ type: "status", ts: new Date().toISOString(), source: "front", message: `CP ${selectedCpId} -> ${newStateInput.value.trim()}` });

    await loadCPs();
    setTimeout(() => stateDialog.close(), 600);
  } catch (e2) {
    dialogMsg.textContent = `❌ Error: ${safeStr(e2.message)}`;
    pushLog({ type: "error", ts: new Date().toISOString(), source: "front", message: `Error changeState: ${safeStr(e2.message)}` });
  }
});

// ====== SSE (eventos en tiempo real) ======
function startSSE() {
  const es = new EventSource("/events");

  es.addEventListener("open", () => {
    badge("Conectado", true);
  });

  es.addEventListener("error", () => {
    badge("Reconectando…", false);
  });

  es.addEventListener("snapshot", (ev) => {
    try {
      const snap = JSON.parse(ev.data);
      (snap.items || []).slice().reverse().forEach(pushLog); // mantiene orden
    } catch {}
  });

  // Tipos esperados: error | alert | status | info | evw | weather ...
  ["error", "alert", "status", "info", "evw", "weather"].forEach(type => {
    es.addEventListener(type, (ev) => {
      try {
        const data = JSON.parse(ev.data);

        // Pintar algunas secciones dedicadas
        if (type === "evw") {
          $("evwLast").textContent = data.message ?? "—";
          $("evwSource").textContent = data.source ?? "—";
          $("evwTs").textContent = data.ts ?? "—";
        }
        if (type === "weather") {
          $("wxTemp").textContent = safeStr(data.payload?.temp ?? data.payload?.temperature);
          $("wxDesc").textContent = safeStr(data.payload?.desc ?? data.payload?.status);
          $("wxTs").textContent = safeStr(data.ts);
        }

        pushLog(data);
      } catch {}
    });
  });
}

// ====== UI bindings ======
cpFilter.addEventListener("input", () => renderCPs(cpsCache));
refreshBtn.addEventListener("click", loadCPs);
clearLogs.addEventListener("click", () => { logs = []; renderLogs(); });

// ====== Init ======
loadCPs();
startSSE();

// Poll de CPs cada 10s (para tener “monitorización” aunque no haya eventos)
setInterval(loadCPs, 10000);
