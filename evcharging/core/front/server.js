const express = require("express");
const path = require("path");
const fs = require("fs");
const https = require("https");

const app = express();
app.use(express.json());
app.use(express.static("public"));

// ====== CONFIG ======
const AUDIT_PATH = path.resolve(process.cwd(), "../../central_audit.log");

// Backend SD (HTTPS)
const SD_HOST = "localhost";
const SD_PORT = 3000;

// ====== LOG PARSER ======
function inferType(action = "", params = "") {
  const a = String(action).toUpperCase();
  const p = String(params).toLowerCase();

  if (a.includes("ERROR") || p.includes("error") || p.includes("exception") || p.includes("traceback")) return "error";
  if (a.includes("ALERT") || p.includes("alert")) return "alert";
  if (a.includes("STATUS") || a.includes("STATE") || p.includes("connected") || p.includes("disconnected")) return "status";
  return "info";
}

function parseLine(line) {
  const parts = String(line).split("|").map(s => s.trim());

  // Formato: ts | ip | action | params...
  const tsRaw = parts[0] || "";
  const ip = parts[1] || "—";
  const action = parts[2] || "—";
  const params = parts.slice(3).join(" | ") || "";

  const parsed = Date.parse(tsRaw);
  const ts = Number.isNaN(parsed) ? tsRaw : new Date(parsed).toISOString();

  return { ts, source: "central_audit", type: inferType(action, params), ip, action, params };
}

// ====== API: LOGS ======
app.get("/api/logs", async (req, res) => {
  const limit = Math.max(1, Math.min(parseInt(req.query.limit || "300", 10), 2000));

  try {
    const raw = await fs.promises.readFile(AUDIT_PATH, "utf-8");
    const lines = raw.split(/\r?\n/).filter(Boolean);
    res.json({ items: lines.slice(-limit).map(parseLine) });
  } catch (e) {
    res.status(500).json({ message: `No se pudo leer ${AUDIT_PATH}`, error: e.message });
  }
});

// ====== API: CPS (PROXY hacia appSD HTTPS) ======
app.get("/api/cps", (req, res) => {
  const options = {
    hostname: SD_HOST,
    port: SD_PORT,
    path: "/cps",
    method: "GET",
    rejectUnauthorized: false, // 👈 clave para self-signed
  };

  const req2 = https.request(options, (r2) => {
    let data = "";
    r2.on("data", (chunk) => (data += chunk));
    r2.on("end", () => {
      try {
        const json = JSON.parse(data || "[]");
        res.status(200).json(json);
      } catch (e) {
        res.status(502).json({ message: "Respuesta inválida del backend SD", raw: data });
      }
    });
  });

  req2.on("error", (err) => {
    res.status(500).json({ message: "Error conectando con backend SD", error: err.message });
  });

  req2.end();
});

const PORT = process.env.PORT || 5050;
app.listen(PORT, () => console.log(`[monitor] http://localhost:${PORT}`));
