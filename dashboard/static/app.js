/* Polymarket Bot Dashboard — vanilla JS */
"use strict";

const $ = id => document.getElementById(id);
let sentimentProfileActive = false;
let sentimentPollingStarted = false;
let activeProfile = null;

// ── Analytics charts ──────────────────────────────────────────────────────────
let equityChart = null;
let dailyChart = null;
let assetChart = null;
let timeframeChart = null;
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

  // ── By Timeframe bar chart ────────────────────────────────────────────────
  timeframeChart = new Chart($("timeframe-chart").getContext("2d"), {
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

  // Recompute by_asset and by_timeframe from filtered curve
  const assetMap = {};
  const tfMap = {};
  equityCurvePoints.forEach(p => {
    const a = p.asset || "BTC";
    const tf = p.timeframe || "?";
    if (!assetMap[a]) assetMap[a] = { asset: a, wins: 0, losses: 0, pnl: 0, trades: 0 };
    if (!tfMap[tf])   tfMap[tf]   = { timeframe: tf, wins: 0, losses: 0, pnl: 0, trades: 0 };
    assetMap[a].trades++;  tfMap[tf].trades++;
    assetMap[a].pnl = Math.round((assetMap[a].pnl + p.pnl) * 10000) / 10000;
    tfMap[tf].pnl   = Math.round((tfMap[tf].pnl   + p.pnl) * 10000) / 10000;
    if (p.pnl > 0) { assetMap[a].wins++;  tfMap[tf].wins++;  }
    else           { assetMap[a].losses++; tfMap[tf].losses++; }
  });
  const byAsset = Object.values(assetMap).sort((a, b) => a.asset.localeCompare(b.asset));
  const byTf    = Object.values(tfMap).sort((a, b) => a.timeframe.localeCompare(b.timeframe));

  _updateEquityChart(equityCurvePoints);
  _updateDailyChart(daily);
  _updateBreakdownChart(assetChart, byAsset, "asset");
  _updateBreakdownChart(timeframeChart, byTf, "timeframe");
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
    synth_arb: "teal",
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
  $("daily-loss").textContent =
    fmt(-(risk.daily_pnl || 0)) + " / " + fmt(risk.daily_loss_cap || 500) + " USDC";
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
    { name: "latency_arb", label: "Latency Arb" },
    { name: "market_maker", label: "Market Maker" },
    { name: "ai_sentiment", label: "AI Sentiment" },
    { name: "copy_trader", label: "Copy Trader" },
    { name: "synth_arb", label: "Synth Arb" },
  ];

  const container = $("strategy-status");
  container.innerHTML = strategies.map(s => `
    <div class="strategy-pill">
      <span class="dot running" id="dot-${s.name}"></span>
      ${s.label}
    </div>
  `).join("");
}

function applyProfileLayout(profile) {
  if (activeProfile === profile) return;
  activeProfile = profile;

  document.body.classList.toggle("profile-sentiment", profile === "sentiment");

  const sentimentSection = $("sentiment-section");
  const target = $(profile === "sentiment" ? "orders-section" : "sentiment-default-anchor");
  if (sentimentSection && target) {
    target.insertAdjacentElement("afterend", sentimentSection);
  }
}

function startSentimentPolling() {
  if (sentimentPollingStarted) return;
  sentimentPollingStarted = true;
  fetchSentimentNews();
  fetchSentimentDecisions();
  fetchSentimentMetrics();
  setInterval(fetchSentimentNews, 30000);
  setInterval(fetchSentimentDecisions, 30000);
  setInterval(fetchSentimentMetrics, 30000);
}

// ── Polling fallbacks ─────────────────────────────────────────────────────────
async function pollAll() {
  await Promise.allSettled([fetchPnL(), fetchAnalytics(), fetchOrders(), fetchLogs()]);
}

// ── Sentiment panel ───────────────────────────────────────────────────────────
async function fetchSentimentStatus() {
  try {
    const r = await fetch("/api/sentiment/status");
    const d = await r.json();
    const profile = d.profile || "latency";
    const sentimentSection = document.getElementById("sentiment-section");
    applyProfileLayout(profile);
    sentimentProfileActive = profile === "sentiment";
    sentimentSection.style.display = sentimentProfileActive ? "" : "none";
    document.getElementById("sent-profile").textContent = d.profile || "—";
    document.getElementById("sent-analyzer").textContent = d.analyzer || "—";
    const antEl = document.getElementById("sent-anthropic");
    antEl.textContent = d.anthropic_key_set ? "✓ set" : "✗ missing";
    antEl.className = "value " + (d.anthropic_key_set ? "positive" : "negative");
    const newsEl = document.getElementById("sent-newsapi");
    newsEl.textContent = d.newsapi_key_set ? "✓ set" : "✗ missing";
    newsEl.className = "value " + (d.newsapi_key_set ? "positive" : "negative");
    if (d.strategy_daily_pnl != null) {
      document.getElementById("sent-analyzer").title =
        `Daily PnL ${fmt(d.strategy_daily_pnl)} / Cap ${fmt(d.strategy_loss_cap || 0)}`;
    }
    if (sentimentProfileActive) {
      startSentimentPolling();
    }
  } catch (_) {}
}

async function fetchSentimentNews() {
  if (!sentimentProfileActive) return;
  try {
    const r = await fetch("/api/sentiment/news");
    const rows = await r.json();
    const tbody = document.getElementById("sent-news-tbody");
    if (!rows.length) { tbody.innerHTML = '<tr><td colspan="4" style="color:var(--muted);text-align:center">No news yet</td></tr>'; return; }
    tbody.innerHTML = rows.map(n => `
      <tr>
        <td style="white-space:nowrap">${n.published_at ? n.published_at.slice(0, 16).replace("T"," ") : "—"}</td>
        <td>${n.source || "—"}</td>
        <td>${(n.title || "").slice(0, 60)}</td>
        <td style="color:var(--accent)">${n.raw_themes || "—"}</td>
      </tr>`).join("");
  } catch (_) {}
}

async function fetchSentimentDecisions() {
  if (!sentimentProfileActive) return;
  try {
    const r = await fetch("/api/sentiment/decisions");
    const rows = await r.json();
    const tbody = document.getElementById("sent-decisions-tbody");
    if (!rows.length) { tbody.innerHTML = '<tr><td colspan="6" style="color:var(--muted);text-align:center">No decisions yet</td></tr>'; return; }
    tbody.innerHTML = rows.map(d => {
      const badge = d.decision === "skip"
        ? `<span class="badge badge-neutral">skip</span>`
        : `<span class="badge badge-positive">${d.decision}</span>`;
      return `<tr>
        <td>${(d.market_question || "").slice(0, 50)}</td>
        <td>${d.market_price != null ? d.market_price.toFixed(3) : "—"}</td>
        <td>${d.estimated_probability != null ? d.estimated_probability.toFixed(3) : "—"}</td>
        <td>${d.edge != null ? d.edge.toFixed(3) : "—"}</td>
        <td>${badge}</td>
        <td style="color:var(--muted)">${d.skip_reason || ""}</td>
      </tr>`;
    }).join("");
  } catch (_) {}
}

async function fetchSentimentMetrics() {
  if (!sentimentProfileActive) return;
  try {
    const r = await fetch("/api/sentiment/metrics");
    const data = await r.json();
    const funnel = data.funnel || {};
    document.getElementById("sent-seen").textContent = funnel.headlines_seen ?? "—";
    document.getElementById("sent-relevant").textContent = funnel.relevant_headlines ?? "—";
    document.getElementById("sent-mapped").textContent = funnel.mapped_headlines ?? "—";
    document.getElementById("sent-traded").textContent = funnel.traded_markets ?? "—";

    const horizonsBody = document.getElementById("sent-horizons-tbody");
    const horizons = data.horizons || {};
    const horizonRows = ["15m", "1h", "4h", "24h"].map(key => {
      const row = horizons[key] || {};
      return `<tr>
        <td>${key}</td>
        <td>${row.eligible_trades ?? 0}</td>
        <td class="${colorClass(row.total_pnl || 0)}">${fmt(row.total_pnl || 0)}</td>
        <td class="${colorClass(row.avg_pnl || 0)}">${fmt(row.avg_pnl || 0)}</td>
        <td>${(((row.win_rate || 0) * 100)).toFixed(1)}%</td>
      </tr>`;
    }).join("");
    horizonsBody.innerHTML = horizonRows || '<tr><td colspan="5" style="color:var(--muted);text-align:center">No trades yet</td></tr>';

    const renderBreakdown = (rows, tbodyId, keyName) => {
      const tbody = document.getElementById(tbodyId);
      if (!rows || !rows.length) {
        tbody.innerHTML = '<tr><td colspan="5" style="color:var(--muted);text-align:center">No data yet</td></tr>';
        return;
      }
      tbody.innerHTML = rows.map(row => `<tr>
        <td>${row[keyName] || "—"}</td>
        <td>${row.trades || 0}</td>
        <td class="${colorClass(row.total_pnl || 0)}">${fmt(row.total_pnl || 0)}</td>
        <td class="${colorClass(row.avg_pnl || 0)}">${fmt(row.avg_pnl || 0)}</td>
        <td>${(((row.win_rate || 0) * 100)).toFixed(1)}%</td>
      </tr>`).join("");
    };

    renderBreakdown(data.by_source, "sent-source-tbody", "source");
    renderBreakdown(data.by_theme, "sent-theme-tbody", "theme");
  } catch (_) {}
}

// ── Synth Arb panel ───────────────────────────────────────────────────────────
async function fetchSynthStats() {
  try {
    const r = await fetch("/api/synth_arb/stats");
    const d = await r.json();

    const section = $("synth-section");
    if (!d.enabled) { section.style.display = "none"; return; }
    section.style.display = "";

    const setVal = (id, text, cls) => {
      const el = $(id); if (!el) return;
      el.textContent = text;
      if (cls) el.className = "value " + cls;
    };

    setVal("synth-daily-pnl",
      (d.daily_pnl >= 0 ? "+" : "") + d.daily_pnl.toFixed(2) + " USDC",
      d.daily_pnl >= 0 ? "pos" : "neg");
    setVal("synth-cumulative-pnl",
      (d.cumulative_pnl >= 0 ? "+" : "") + d.cumulative_pnl.toFixed(2) + " USDC",
      d.cumulative_pnl >= 0 ? "pos" : "neg");
    setVal("synth-open-count", d.open_count, "neutral");
    setVal("synth-total-trades", d.total_trades, "neutral");
    setVal("synth-win-rate", ((d.win_rate || 0) * 100).toFixed(1) + "%", "neutral");
    setVal("synth-avg-gap", d.avg_gap_pct > 0 ? d.avg_gap_pct.toFixed(2) + "%" : "—", "neutral");

    const table = $("synth-positions-table");
    const tbody = $("synth-positions-tbody");
    if (!d.positions || !d.positions.length) {
      table.style.display = "none";
      return;
    }
    table.style.display = "";
    tbody.innerHTML = d.positions.map(p => `<tr>
      <td style="max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${p.question}">${p.question || p.condition_id}</td>
      <td>${p.yes_price.toFixed(3)}</td>
      <td>${p.no_price.toFixed(3)}</td>
      <td>$${p.total_cost.toFixed(2)}</td>
      <td>$${p.payout_target.toFixed(0)}</td>
      <td style="color:var(--teal)">${p.gap_pct.toFixed(2)}%</td>
      <td>${p.age_minutes.toFixed(0)}m</td>
    </tr>`).join("");
  } catch (e) { console.warn("fetchSynthStats:", e); }
}

// ── Boot ──────────────────────────────────────────────────────────────────────
initAnalyticsCharts();
renderStrategyPills();
connectWS();
pollAll();
fetchSentimentStatus();
fetchSynthStats();

// Slower polls for orders/logs (WS handles positions + metrics)
setInterval(fetchOrders, 10000);
setInterval(fetchLogs, 15000);
setInterval(fetchPnL, 30000);
setInterval(fetchAnalytics, 60000);  // refresh analytics every minute

// Sentiment profile detection / panel polls
setInterval(fetchSentimentStatus, 60000);

// Synth arb panel
setInterval(fetchSynthStats, 30000);
