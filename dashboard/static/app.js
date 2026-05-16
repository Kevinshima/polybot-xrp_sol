/* Polymarket Bot Dashboard — vanilla JS */
"use strict";

const $ = id => document.getElementById(id);
let activeProfile = null;

// ── Analytics charts ──────────────────────────────────────────────────────────
let equityChart = null;
let dailyChart = null;
let assetChart = null;
let entrypathChart = null;
let analyticsData = null;  // full dataset cached from /api/analytics
let activeRange = "all";
let equityCurvePoints = [];  // current filtered curve, for tooltip access

const CHART_GRID   = "rgba(48,54,61,0.8)";
const CHART_TICK   = { color: "#8b949e", font: { size: 9 } };
const GREEN        = "#3fb950";
const RED          = "#f85149";
const GREEN_BG     = "rgba(63,185,80,0.55)";
const RED_BG       = "rgba(248,81,73,0.55)";

function fmtDateLabel(ts) {
  const d = new Date(ts * 1000);
  return d.toLocaleDateString(undefined, { month: "short", day: "numeric" })
    + " " + d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
}

function initAnalyticsCharts() {
  // ── Equity curve (large) ──────────────────────────────────────────────────
  equityChart = new Chart($("equity-chart").getContext("2d"), {
    type: "line",
    data: { labels: [], datasets: [{
      label: "Cumulative PnL",
      data: [],
      borderColor: "#58a6ff",
      backgroundColor: "rgba(88,166,255,0.07)",
      borderWidth: 1.5,
      pointRadius: 3,
      pointHoverRadius: 5,
      pointBackgroundColor: [],
      pointBorderWidth: 0,
      tension: 0.2,
      fill: true,
    }]},
    options: {
      responsive: true, maintainAspectRatio: false, animation: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: "rgba(22,27,34,0.96)",
          borderColor: "#30363d", borderWidth: 1,
          titleColor: "#8b949e", bodyColor: "#c9d1d9",
          callbacks: {
            title: items => {
              const p = equityCurvePoints[items[0].dataIndex];
              return p ? fmtDateLabel(p.timestamp) : "";
            },
            label: item => {
              const p = equityCurvePoints[item.dataIndex];
              if (!p) return "";
              const q = p.question.length > 48 ? p.question.slice(0, 48) + "…" : p.question;
              return [
                q,
                `Trade: ${p.pnl >= 0 ? "+" : ""}${p.pnl.toFixed(4)} USDC`,
                `Running total: ${p.cumulative_pnl >= 0 ? "+" : ""}${p.cumulative_pnl.toFixed(2)} USDC`,
                `${p.asset} · ${p.timeframe} · ${p.side}`,
              ];
            },
          },
        },
      },
      scales: {
        x: {
          display: true,
          ticks: { ...CHART_TICK, maxTicksLimit: 6, maxRotation: 0 },
          grid: { display: false },
        },
        y: {
          grid: { color: CHART_GRID },
          ticks: { ...CHART_TICK, maxTicksLimit: 6,
            callback: v => (v >= 0 ? "+" : "") + v.toFixed(0),
          },
        },
      },
    },
  });

  // ── Daily P&L bar chart ───────────────────────────────────────────────────
  dailyChart = new Chart($("daily-chart").getContext("2d"), {
    type: "bar",
    data: { labels: [], datasets: [{
      label: "Net PnL",
      data: [],
      backgroundColor: [],
      borderWidth: 0,
      borderRadius: 2,
    }]},
    options: {
      responsive: true, maintainAspectRatio: false, animation: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: "rgba(22,27,34,0.96)",
          borderColor: "#30363d", borderWidth: 1,
          titleColor: "#8b949e", bodyColor: "#c9d1d9",
          callbacks: {
            label: item => {
              const d = item.chart.data._raw[item.dataIndex];
              return d
                ? [`PnL: ${d.pnl >= 0 ? "+" : ""}${d.pnl.toFixed(2)} USDC`, `W ${d.wins}  L ${d.losses}`]
                : "";
            },
          },
        },
      },
      scales: {
        x: { ticks: { ...CHART_TICK, maxRotation: 30 }, grid: { display: false } },
        y: { grid: { color: CHART_GRID }, ticks: CHART_TICK },
      },
    },
  });

  // ── By Asset bar chart ────────────────────────────────────────────────────
  assetChart = new Chart($("asset-chart").getContext("2d"), {
    type: "bar",
    data: { labels: [], datasets: [{
      label: "Net PnL",
      data: [],
      backgroundColor: [],
      borderWidth: 0,
      borderRadius: 2,
    }]},
    options: {
      responsive: true, maintainAspectRatio: false, animation: false,
      indexAxis: "y",
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: "rgba(22,27,34,0.96)",
          borderColor: "#30363d", borderWidth: 1,
          titleColor: "#8b949e", bodyColor: "#c9d1d9",
          callbacks: {
            label: item => {
              const d = item.chart.data._raw[item.dataIndex];
              if (!d) return "";
              const wr = d.trades ? ((d.wins / d.trades) * 100).toFixed(0) + "%" : "—";
              return [`PnL: ${d.pnl >= 0 ? "+" : ""}${d.pnl.toFixed(2)} USDC`, `${d.trades} trades · win rate ${wr}`];
            },
          },
        },
      },
      scales: {
        x: { grid: { color: CHART_GRID }, ticks: CHART_TICK },
        y: { ticks: { ...CHART_TICK, font: { size: 10 } }, grid: { display: false } },
      },
    },
  });

  // ── By Entry Path bar chart ───────────────────────────────────────────────
  entrypathChart = new Chart($("entrypath-chart").getContext("2d"), {
    type: "bar",
    data: { labels: [], datasets: [{
      label: "Net PnL",
      data: [],
      backgroundColor: [],
      borderWidth: 0,
      borderRadius: 2,
    }]},
    options: {
      responsive: true, maintainAspectRatio: false, animation: false,
      indexAxis: "y",
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: "rgba(22,27,34,0.96)",
          borderColor: "#30363d", borderWidth: 1,
          titleColor: "#8b949e", bodyColor: "#c9d1d9",
          callbacks: {
            label: item => {
              const d = item.chart.data._raw[item.dataIndex];
              if (!d) return "";
              const wr = d.trades ? ((d.wins / d.trades) * 100).toFixed(0) + "%" : "—";
              return [`PnL: ${d.pnl >= 0 ? "+" : ""}${d.pnl.toFixed(2)} USDC`, `${d.trades} trades · win rate ${wr}`];
            },
          },
        },
      },
      scales: {
        x: { grid: { color: CHART_GRID }, ticks: CHART_TICK },
        y: { ticks: { ...CHART_TICK, font: { size: 10 } }, grid: { display: false } },
      },
    },
  });

  // ── Time range tab handlers ───────────────────────────────────────────────
  document.querySelectorAll(".time-tab").forEach(btn => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".time-tab").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      activeRange = btn.dataset.range;
      if (analyticsData) applyAnalyticsRange(analyticsData, activeRange);
    });
  });
}

function _rangeCutoff(range) {
  if (range === "today") {
    const d = new Date(); d.setHours(0, 0, 0, 0);
    return Math.floor(d.getTime() / 1000);
  }
  if (range === "7d") return Math.floor(Date.now() / 1000) - 7 * 86400;
  return 0;
}

function applyAnalyticsRange(data, range) {
  const cutoff = _rangeCutoff(range);
  const curve = range === "all"
    ? data.equity_curve
    : data.equity_curve.filter(p => p.timestamp >= cutoff);

  // Recompute running cumulative from 0 for filtered range
  let cum = 0;
  equityCurvePoints = curve.map(p => {
    cum = Math.round((cum + p.pnl) * 10000) / 10000;
    return { ...p, cumulative_pnl: cum };
  });

  // Filter breakdowns too
  const daily = range === "all"
    ? data.daily_bars
    : data.daily_bars.filter(d => d.date >= new Date(cutoff * 1000).toISOString().slice(0, 10));

  // Recompute by_asset and by_entry_path from filtered curve
  const assetMap = {};
  const pathMap = {};
  equityCurvePoints.forEach(p => {
    const a = p.asset || "BTC";
    const ep = p.entry_path || "UNKNOWN";
    if (!assetMap[a])  assetMap[a]  = { asset: a, wins: 0, losses: 0, pnl: 0, trades: 0 };
    if (!pathMap[ep])  pathMap[ep]  = { entry_path: ep, wins: 0, losses: 0, pnl: 0, trades: 0 };
    assetMap[a].trades++;   pathMap[ep].trades++;
    assetMap[a].pnl  = Math.round((assetMap[a].pnl  + p.pnl) * 10000) / 10000;
    pathMap[ep].pnl  = Math.round((pathMap[ep].pnl  + p.pnl) * 10000) / 10000;
    if (p.pnl > 0) { assetMap[a].wins++;  pathMap[ep].wins++;  }
    else           { assetMap[a].losses++; pathMap[ep].losses++; }
  });
  const byAsset = Object.values(assetMap).sort((a, b) => a.asset.localeCompare(b.asset));
  const byPath  = Object.values(pathMap).sort((a, b) => a.entry_path.localeCompare(b.entry_path));

  _updateEquityChart(equityCurvePoints);
  _updateDailyChart(daily);
  _updateBreakdownChart(assetChart, byAsset, "asset");
  _updateBreakdownChart(entrypathChart, byPath, "entry_path");
}

function _updateEquityChart(curve) {
  if (!equityChart) return;
  equityChart.data.labels = curve.map(p => fmtDateLabel(p.timestamp));
  equityChart.data.datasets[0].data = curve.map(p => p.cumulative_pnl);
  equityChart.data.datasets[0].pointBackgroundColor = curve.map(p => p.pnl >= 0 ? GREEN : RED);
  equityChart.update("none");
}

function _updateDailyChart(daily) {
  if (!dailyChart) return;
  dailyChart.data.labels = daily.map(d => d.label);
  dailyChart.data.datasets[0].data = daily.map(d => d.pnl);
  dailyChart.data.datasets[0].backgroundColor = daily.map(d => d.pnl >= 0 ? GREEN_BG : RED_BG);
  dailyChart.data._raw = daily;  // stash for tooltip
  dailyChart.update("none");
}

function _updateBreakdownChart(chart, rows, labelKey) {
  if (!chart) return;
  chart.data.labels = rows.map(r => r[labelKey]);
  chart.data.datasets[0].data = rows.map(r => r.pnl);
  chart.data.datasets[0].backgroundColor = rows.map(r => r.pnl >= 0 ? GREEN_BG : RED_BG);
  chart.data._raw = rows;  // stash for tooltip
  chart.update("none");
}

// ── Helpers ───────────────────────────────────────────────────────────────────
const fmt = (n, decimals = 2) => {
  const v = parseFloat(n) || 0;
  const s = Math.abs(v).toFixed(decimals);
  return (v >= 0 ? "+" : "-") + s;
};

const fmtPrice = n => (parseFloat(n) || 0).toFixed(4);

const colorClass = n => parseFloat(n) >= 0 ? "pos" : "neg";

const ts = t => new Date(t * 1000).toLocaleTimeString();

const badge = (text, cls) => `<span class="badge badge-${cls}">${text}</span>`;

function stratBadge(name) {
  const colors = {
    latency_arb: "blue",
    market_maker: "purple",
    ai_sentiment: "yellow",
    copy_trader: "green",
  };
  return colors[name] || "blue";
}

// ── Update functions ──────────────────────────────────────────────────────────
function updateMetrics(data) {
  const daily = data.daily_pnl || 0;
  const cum = data.cumulative_pnl || 0;
  const openVal = data.open_value || 0;
  const risk = data.risk || {};

  $("daily-pnl").textContent = fmt(daily) + " USDC";
  $("daily-pnl").className = "value " + colorClass(daily);

  $("cumulative-pnl").textContent = fmt(cum) + " USDC";
  $("cumulative-pnl").className = "value " + colorClass(cum);

  $("open-value").textContent = fmtPrice(openVal) + " USDC";

  const openOrders = (risk.open_orders || 0);
  $("open-orders").textContent = openOrders + " / " + (risk.max_open_orders || 20);

  const halted = risk.halted === true;
  $("halted-banner").classList.toggle("visible", halted);
  const lossUsed = Math.max(0, -(risk.daily_pnl || 0));
  const cap = risk.daily_loss_cap || 300;
  $("daily-loss").textContent = lossUsed.toFixed(2) + " / " + cap.toFixed(2) + " USDC";
  $("daily-loss").className = "value " + (lossUsed > cap * 0.75 ? "neg" : lossUsed > 0 ? "neutral" : "pos");
}


function updatePositions(positions) {
  const tbody = $("positions-tbody");
  if (!positions || !positions.length) {
    tbody.innerHTML = '<tr><td colspan="7" style="color:var(--muted);text-align:center">No open positions</td></tr>';
    return;
  }
  tbody.innerHTML = positions.map(p => {
    const pnl = p.unrealized_pnl || 0;
    return `<tr>
      <td>${badge(p.strategy, stratBadge(p.strategy))}</td>
      <td style="max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${p.question}">${p.question || p.market_id}</td>
      <td>${badge(p.side, p.side === "BUY" ? "green" : "red")}</td>
      <td>${fmtPrice(p.size)}</td>
      <td>${fmtPrice(p.entry_price)}</td>
      <td>${fmtPrice(p.current_price)}</td>
      <td class="${colorClass(pnl)}">${fmt(pnl)}</td>
    </tr>`;
  }).join("");
}

// ── REST fetches ──────────────────────────────────────────────────────────────
async function fetchPnL() {
  try {
    const r = await fetch("/api/pnl");
    const data = await r.json();
    $("win-rate").textContent = ((data.win_rate || 0) * 100).toFixed(1) + "%";
    $("total-trades").textContent = data.total_trades || 0;
  } catch (e) { console.warn("fetchPnL:", e); }
}

async function fetchAnalytics() {
  try {
    const r = await fetch("/api/analytics");
    analyticsData = await r.json();
    applyAnalyticsRange(analyticsData, activeRange);
  } catch (e) { console.warn("fetchAnalytics:", e); }
}

async function fetchOrders() {
  try {
    const r = await fetch("/api/orders");
    const orders = await r.json();
    const tbody = $("orders-tbody");
    if (!orders.length) {
      tbody.innerHTML = '<tr><td colspan="7" style="color:var(--muted);text-align:center">No trades yet</td></tr>';
      return;
    }
    tbody.innerHTML = orders.slice(0, 30).map(o => {
      const pnl = o.pnl || 0;
      const statusCls = { open: "blue", filled: "green", closed: "green", cancelled: "red", dry_run: "yellow" };
      return `<tr>
        <td>${ts(o.timestamp)}</td>
        <td>${badge(o.strategy, stratBadge(o.strategy))}</td>
        <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${o.question || o.market_id}</td>
        <td>${badge(o.side, o.side === "BUY" ? "green" : "red")}</td>
        <td>${fmtPrice(o.price)}</td>
        <td>${badge(o.status, statusCls[o.status] || "yellow")}</td>
        <td class="${colorClass(pnl)}">${fmt(pnl)}</td>
      </tr>`;
    }).join("");
  } catch (e) { console.warn("fetchOrders:", e); }
}

async function fetchLogs() {
  try {
    const r = await fetch("/api/logs");
    const lines = await r.json();
    $("log-box").textContent = lines.join("\n");
    $("log-box").scrollTop = $("log-box").scrollHeight;
  } catch (e) { console.warn("fetchLogs:", e); }
}

// ── Kill / Resume ─────────────────────────────────────────────────────────────
$("kill-btn").addEventListener("click", async () => {
  if (!confirm("Activate kill switch? This will cancel ALL open orders.")) return;
  try {
    const r = await fetch("/api/kill", { method: "POST" });
    const data = await r.json();
    alert(data.message || "Kill switch activated");
  } catch (e) { alert("Error: " + e); }
});

$("resume-btn").addEventListener("click", async () => {
  try {
    const r = await fetch("/api/resume", { method: "POST" });
    const data = await r.json();
    alert(data.message || "Bot resumed");
  } catch (e) { alert("Error: " + e); }
});

// ── WebSocket live updates ────────────────────────────────────────────────────
function connectWS() {
  const ws = new WebSocket(`ws://${location.host}/ws/live`);
  const statusEl = $("connection-status");

  ws.onopen = () => {
    statusEl.textContent = "● LIVE";
    statusEl.className = "live";
    // Send ping every 20s to keep alive
    setInterval(() => ws.readyState === 1 && ws.send("ping"), 20000);
  };

  ws.onmessage = e => {
    try {
      const data = JSON.parse(e.data);
      updateMetrics(data);
      if (data.positions) updatePositions(data.positions);
    } catch { }
  };

  ws.onclose = () => {
    statusEl.textContent = "○ DISCONNECTED";
    statusEl.className = "";
    setTimeout(connectWS, 3000); // reconnect
  };

  ws.onerror = () => ws.close();
}

// ── Strategy status pills ─────────────────────────────────────────────────────
function renderStrategyPills() {
  const strategies = [
    { name: "latency_arb", label: "Latency Arb (SOL/XRP)" },
  ];

  const container = $("strategy-status");
  container.innerHTML = strategies.map(s => `
    <div class="strategy-pill">
      <span class="dot running" id="dot-${s.name}"></span>
      ${s.label}
    </div>
  `).join("");
}

// ── Polling fallbacks ─────────────────────────────────────────────────────────
async function pollAll() {
  await Promise.allSettled([fetchPnL(), fetchAnalytics(), fetchOrders(), fetchLogs()]);
}

// ── Latency Arb stats (filter counters + ML shadow + regime) ─────────────────
async function fetchLatencyArbStats() {
  try {
    const r = await fetch("/api/latency_arb/stats");
    const d = await r.json();

    // Regime + no-trade streak
    const regime = $("regime-label");
    if (regime) {
      regime.textContent = d.regime || "—";
      regime.className = "value " + (
        d.regime && d.regime.includes("Overnight") ? "neg" :
        d.regime && d.regime.includes("Afternoon") ? "pos" : "neutral"
      );
    }
    const noTradeEl = $("no-trade-hours");
    if (noTradeEl) {
      const h = d.hours_since_last_trade;
      noTradeEl.textContent = h != null ? h + "h" : "—";
      noTradeEl.className = "value " + (h != null && h > 12 ? "neg" : h != null && h < 4 ? "pos" : "neutral");
    }

    // Filter rejection counters
    const rej = d.filter_rejections || {};
    const setCard = (id, val) => { const el = $(id); if (el) el.textContent = val ?? 0; };
    setCard("flt-total", d.total_rejected);
    setCard("flt-ca", rej.ca_not_aligned || 0);
    setCard("flt-danger", rej.ft_danger_zone || 0);
    setCard("flt-circuit", rej.circuit_breaker || 0);
    setCard("flt-delta", rej.delta_not_aligned || 0);
    setCard("flt-ob", rej.weak_ob || 0);
    setCard("flt-overnight", (rej.stale_flat_overnight || 0) + (rej.overnight || 0));
    setCard("flt-stale", rej.cooldown_active || 0);

    // ML shadow panel
    const ml = d.ml || {};
    const setMl = (id, val, cls) => {
      const el = $(id); if (!el) return;
      el.textContent = val ?? "—";
      if (cls) el.className = "value " + cls;
    };

    const auc = parseFloat(ml.cv_roc_auc) || 0;
    setMl("ml-auc", auc ? auc.toFixed(3) : "—", auc >= 0.60 ? "pos" : auc >= 0.55 ? "neutral" : "neg");

    const aftAuc = parseFloat(ml.afternoon_auc) || 0;
    setMl("ml-afternoon-auc", aftAuc ? aftAuc.toFixed(3) : "—", aftAuc >= 0.58 ? "pos" : aftAuc >= 0.52 ? "neutral" : "neg");

    setMl("ml-n-trades", ml.n_trades ?? "—", "neutral");
    const wr = parseFloat(ml.win_rate) || 0;
    setMl("ml-wr", wr ? (wr * 100).toFixed(1) + "%" : "—", wr >= 0.55 ? "pos" : "neutral");

    const gateEnabled = ml.gate_enabled === true;
    setMl("ml-gate", gateEnabled ? "ACTIVE" : "Shadow", gateEnabled ? "pos" : "neutral");

    const trainedAt = ml.trained_at ? ml.trained_at.toString().slice(0, 16).replace("T", " ") : "—";
    setMl("ml-trained-at", trainedAt, "neutral");

  } catch (e) { console.warn("fetchLatencyArbStats:", e); }
}

// ── Boot ──────────────────────────────────────────────────────────────────────
initAnalyticsCharts();
renderStrategyPills();
connectWS();
pollAll();
fetchLatencyArbStats();

// Slower polls for orders/logs (WS handles positions + metrics)
setInterval(fetchOrders, 10000);
setInterval(fetchLogs, 15000);
setInterval(fetchPnL, 30000);
setInterval(fetchAnalytics, 60000);

// Latency arb stats (filter counters, ML shadow, regime)
setInterval(fetchLatencyArbStats, 60000);
