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
    return Number.isInteger(num) ? String(num) : num.toFixed(1);
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
            <th>Delta</th>
            <th>Edge</th>
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
              <td>${escapeHtml(formatLine(prop.delta))}</td>
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