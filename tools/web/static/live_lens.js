(function () {
  const bootstrap = window.MLBLiveLensBootstrap || {};
  const state = {
    date: String(bootstrap.date || ""),
  };

  const metaNode = document.getElementById("liveLensMeta");
  const overviewNode = document.getElementById("liveLensOverview");
  const gamesNode = document.getElementById("liveLensGames");

  function escapeHtml(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function toNumber(value) {
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  }

  function formatLine(value) {
    const num = toNumber(value);
    if (num == null) return "-";
    if (Math.abs(num) >= 10 || Number.isInteger(num)) return String(Number(num.toFixed(1)));
    return num.toFixed(2).replace(/\.00$/, ".0");
  }

  function formatOdds(value) {
    const num = toNumber(value);
    if (num == null) return "-";
    return num > 0 ? `+${num}` : String(num);
  }

  function formatPercent(value) {
    const num = toNumber(value);
    if (num == null) return "-";
    return `${(num * 100).toFixed(1)}%`;
  }

  function formatSigned(value, digits = 2) {
    const num = toNumber(value);
    if (num == null) return "-";
    const rounded = Number(num.toFixed(digits));
    return rounded > 0 ? `+${rounded}` : String(rounded);
  }

  function pickTone(value) {
    return toNumber(value) != null && Math.abs(Number(value)) > 0 ? "#005f73" : "#6c757d";
  }

  function propLabel(prop) {
    const raw = String(prop?.marketLabel || prop?.prop || prop?.market || "Prop");
    return raw.replace(/_/g, " ");
  }

  function badgeTone(status) {
    const token = String(status || "").toLowerCase();
    if (token === "win") return "#2d6a4f";
    if (token === "loss") return "#9b2226";
    if (token === "push") return "#6c757d";
    if (token === "live") return "#005f73";
    return "#6c757d";
  }

  function renderMetric(label, value) {
    return `<div style="display:flex;justify-content:space-between;gap:12px;"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></div>`;
  }

  function renderLensCard(lens) {
    const projection = lens?.projection || {};
    const moneyline = lens?.markets?.moneyline || {};
    const spread = lens?.markets?.spread || {};
    const total = lens?.markets?.total || {};
    const marketProb = formatPercent(moneyline.marketHomeProb);
    const modelProb = formatPercent(lens?.modelHomeWinProb);
    const baselineProb = formatPercent(lens?.baselineHomeWinProb);
    const projectionLine = projection.closed
      ? "Segment closed"
      : `${formatLine(projection.away)} - ${formatLine(projection.home)} | Total ${formatLine(projection.total)} | Home margin ${formatSigned(projection.homeMargin)}`;
    return `
      <div style="border:1px solid #d7dce1;border-radius:14px;padding:14px;background:#fff;min-width:220px;flex:1 1 220px;">
        <div style="display:flex;justify-content:space-between;gap:12px;align-items:center;">
          <div style="font-weight:700;">${escapeHtml(String(lens?.label || "Segment"))}</div>
          <span style="display:inline-block;padding:2px 8px;border-radius:999px;background:${lens?.closed ? "#6c757d" : "#005f73"};color:#fff;">${escapeHtml(lens?.closed ? "Closed" : String(lens?.source || "live"))}</span>
        </div>
        <div class="status-line" style="margin-top:6px;">${escapeHtml(projectionLine)}</div>
        <div style="display:grid;gap:6px;margin-top:12px;">
          ${renderMetric("Home win", modelProb)}
          ${renderMetric("Baseline", baselineProb)}
          ${renderMetric("Market", marketProb)}
          ${renderMetric("ML", moneyline.pick ? `${String(moneyline.pick).toUpperCase()} ${formatPercent(moneyline.edge)}` : "-" )}
          ${renderMetric("Spread", spread.pick ? `${String(spread.pick).toUpperCase()} ${formatSigned(spread.homeLine, 1)} (${formatSigned(spread.edge)})` : "-" )}
          ${renderMetric("Total", total.pick ? `${String(total.pick).toUpperCase()} ${formatLine(total.line)} (${formatSigned(total.edge)})` : "-" )}
        </div>
      </div>`;
  }

  function renderGameLens(game) {
    const lensRows = Array.isArray(game?.gameLens) ? game.gameLens : [];
    if (!lensRows.length) {
      return '<div class="empty">No live game lens available.</div>';
    }
    return `
      <div style="display:flex;flex-wrap:wrap;gap:12px;margin:14px 0 18px;">
        ${lensRows.map((lens) => renderLensCard(lens)).join("")}
      </div>`;
  }

  function renderPropTable(props) {
    if (!Array.isArray(props) || !props.length) {
      return '<div class="empty">No tracked player props for this game.</div>';
    }
    return `
      <table class="table">
        <thead>
          <tr>
            <th>Player</th>
            <th>Prop</th>
            <th>Tier</th>
            <th>Side</th>
            <th>Line</th>
            <th>Actual</th>
            <th>Projection</th>
            <th>Model</th>
            <th>Live Edge</th>
            <th>Pregame Edge</th>
            <th>Odds</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          ${props.map((prop) => `
            <tr>
              <td>${escapeHtml(prop.playerName || "")}</td>
              <td>${escapeHtml(propLabel(prop))}</td>
              <td>${escapeHtml(String(prop.tier || ""))}</td>
              <td>${escapeHtml(String(prop.selection || ""))}</td>
              <td>${escapeHtml(formatLine(prop.line))}</td>
              <td>${escapeHtml(formatLine(prop.actual))}</td>
              <td style="color:${pickTone(prop.liveEdge)};font-weight:600;">${escapeHtml(formatLine(prop.liveProjection))}</td>
              <td>${escapeHtml(formatLine(prop.modelMean))}</td>
              <td style="color:${pickTone(prop.liveEdge)};font-weight:600;">${escapeHtml(formatSigned(prop.liveEdge))}</td>
              <td>${escapeHtml(formatPercent(prop.edge))}</td>
              <td>${escapeHtml(formatOdds(prop.odds))}</td>
              <td><span style="display:inline-block;padding:2px 8px;border-radius:999px;background:${badgeTone(prop.status)};color:#fff;">${escapeHtml(String(prop.status || "pending"))}</span></td>
            </tr>`).join("")}
        </tbody>
      </table>`;
  }

  function renderGames(payload) {
    const games = Array.isArray(payload?.games) ? payload.games : [];
    if (!games.length) {
      gamesNode.innerHTML = `<div class="empty">No games found for ${escapeHtml(state.date)}.</div>`;
      return;
    }
    gamesNode.innerHTML = games.map((game) => {
      const away = game?.matchup?.away || {};
      const home = game?.matchup?.home || {};
      const score = game?.matchup?.score || {};
      const liveText = String(game?.matchup?.liveText || "").trim();
      const status = game?.status || {};
      return `
        <section class="panel">
          <div class="panel-title">${escapeHtml(String(away.abbr || away.name || "Away"))} at ${escapeHtml(String(home.abbr || home.name || "Home"))}</div>
          <div class="status-line">${escapeHtml(String(status.abstract || ""))} — ${escapeHtml(String(status.detailed || game.startTime || ""))}</div>
          <div class="status-line">Score: ${escapeHtml(String(score.away ?? "-"))} - ${escapeHtml(String(score.home ?? "-"))}${liveText ? ` | ${escapeHtml(liveText)}` : ""}</div>
          ${renderGameLens(game)}
          ${renderPropTable(game.props)}
        </section>`;
    }).join("");
  }

  async function load() {
    try {
      const response = await fetch(`/api/live-lens?date=${encodeURIComponent(state.date)}`);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const payload = await response.json();
      const counts = payload?.counts || {};
      if (metaNode) {
        metaNode.textContent = `Games ${counts.games || 0} | Live ${counts.live || 0} | Final ${counts.final || 0} | Pregame ${counts.pregame || 0} | Props ${counts.props || 0}`;
      }
      if (overviewNode) {
        overviewNode.textContent = JSON.stringify({
          generatedAt: payload?.generatedAt,
          dataRoot: payload?.dataRoot,
          liveLensDir: payload?.liveLensDir,
          counts: payload?.counts,
        }, null, 2);
      }
      renderGames(payload);
    } catch (error) {
      const message = error && error.message ? error.message : "Unknown error";
      if (metaNode) metaNode.textContent = `Failed to load live lens: ${message}`;
      if (gamesNode) gamesNode.innerHTML = `<div class="empty">Failed to load live lens.</div>`;
    }
  }

  load();
})();