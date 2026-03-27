(function () {
  const bootstrap = window.MLBLiveLensBootstrap || {};
  const state = {
    date: String(bootstrap.date || ""),
    season: Number(bootstrap.season || 0),
    apiPath: String(bootstrap.apiPath || "/api/live-lens"),
    manifest: null,
    monthFilter: "all",
  };

  const metaNode = document.getElementById("liveLensMeta");
  const overviewNode = document.getElementById("liveLensOverview");
  const gamesNode = document.getElementById("liveLensGames");
  const monthsNode = document.getElementById("liveLensMonths");
  const daysNode = document.getElementById("liveLensDays");
  const dateInputNode = document.querySelector('input[name="date"]');

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
    if (toNumber(value) != null && Math.abs(Number(value)) > 0) {
      return state.season ? "#9af3de" : "#005f73";
    }
    return state.season ? "#bfd0df" : "#6c757d";
  }

  function propLabel(prop) {
    const raw = String(prop?.marketLabel || prop?.prop || prop?.market || "Prop");
    return raw.replace(/_/g, " ");
  }

  function propTierLabel(prop) {
    const tier = String(prop?.tier || prop?.source || "").toLowerCase();
    if (tier === "live" || tier === "current_market" || tier === "live_registry") return "live";
    if (tier === "official") return "official";
    if (tier === "playable") return "playable";
    return tier || "tracked";
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

  function renderSeasonMetric(label, value) {
    return `
      <div class="live-lens-metric-row">
        <span class="live-lens-metric-label">${escapeHtml(label)}</span>
        <strong class="live-lens-metric-value">${escapeHtml(value)}</strong>
      </div>`;
  }

  function updateUrl(dateStr) {
    const url = new URL(window.location.href);
    if (dateStr) url.searchParams.set("date", dateStr);
    else url.searchParams.delete("date");
    window.history.replaceState({}, "", url.toString());
  }

  function monthLabel(monthKey) {
    const dt = new Date(`${monthKey}-01T12:00:00`);
    if (Number.isNaN(dt.getTime())) return monthKey;
    return dt.toLocaleDateString(undefined, { month: "short", year: "numeric" });
  }

  function formatDateLong(dateStr) {
    const dt = new Date(`${dateStr}T12:00:00`);
    if (Number.isNaN(dt.getTime())) return dateStr;
    return dt.toLocaleDateString(undefined, { weekday: "short", month: "short", day: "numeric", year: "numeric" });
  }

  function filteredDays() {
    const allDays = Array.isArray(state.manifest?.days) ? state.manifest.days : [];
    if (state.monthFilter === "all") return allDays;
    return allDays.filter((row) => String(row?.month || "") === state.monthFilter);
  }

  function renderMonths() {
    if (!monthsNode || !state.season) return;
    const allDays = Array.isArray(state.manifest?.days) ? state.manifest.days : [];
    const months = Array.isArray(state.manifest?.months) ? state.manifest.months : [];
    const options = [{ key: "all", label: `All ${allDays.length}` }].concat(
      months.map((row) => ({
        key: String(row?.month || ""),
        label: `${monthLabel(String(row?.month || ""))} ${row?.days ?? 0}`,
      }))
    );
    monthsNode.innerHTML = options.map((option) => `
      <button
        type="button"
        class="cards-filter-pill ${option.key === state.monthFilter ? "is-active" : ""}"
        data-live-lens-month="${escapeHtml(option.key)}"
      >
        ${escapeHtml(option.label)}
      </button>`).join("");
  }

  function renderDaysRail() {
    if (!daysNode || !state.season) return;
    const days = filteredDays();
    if (!days.length) {
      daysNode.innerHTML = '<div class="season-empty-copy">No season dates match the current month filter.</div>';
      return;
    }
    daysNode.innerHTML = days.map((day) => {
      const dateStr = String(day?.date || "");
      const isActive = dateStr === state.date;
      return `
        <article class="season-day-entry">
          <button
            type="button"
            class="season-day-button ${isActive ? "is-active" : ""}"
            data-live-lens-date="${escapeHtml(dateStr)}"
          >
            <div class="season-day-row">
              <div class="season-day-primary">${escapeHtml(dateStr)}</div>
              <span class="cards-chip">${escapeHtml(String(day?.games || 0))} games</span>
            </div>
            <div class="season-day-secondary">${escapeHtml(formatDateLong(dateStr))}</div>
          </button>
        </article>`;
    }).join("");
  }

  async function loadSeasonManifest() {
    if (!state.season) return;
    const response = await fetch(`/api/season/${encodeURIComponent(state.season)}`, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    state.manifest = await response.json();
    const days = Array.isArray(state.manifest?.days) ? state.manifest.days : [];
    if (!days.length) {
      renderMonths();
      renderDaysRail();
      return;
    }
    const hasSelected = days.some((row) => String(row?.date || "") === state.date);
    if (!state.date || !hasSelected) {
      state.date = String(days[days.length - 1]?.date || "");
      updateUrl(state.date);
      if (dateInputNode) dateInputNode.value = state.date;
    }
    renderMonths();
    renderDaysRail();
  }

  async function activateDate(dateStr) {
    const nextDate = String(dateStr || "");
    if (!nextDate || nextDate === state.date) return;
    state.date = nextDate;
    updateUrl(state.date);
    if (dateInputNode) dateInputNode.value = state.date;
    renderDaysRail();
    await load();
  }

  function setMonthFilter(monthKey) {
    state.monthFilter = monthKey || "all";
    renderMonths();
    renderDaysRail();
    const visible = filteredDays();
    if (!visible.length) return;
    if (!visible.some((row) => String(row?.date || "") === state.date)) {
      activateDate(String(visible[visible.length - 1]?.date || ""));
    }
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
    if (state.season) {
      return `
        <article class="live-lens-segment-card">
          <div class="live-lens-segment-head">
            <div class="live-lens-segment-title">${escapeHtml(String(lens?.label || "Segment"))}</div>
            <span class="live-lens-status-badge" style="background:${lens?.closed ? "#6c757d" : "#005f73"};color:#fff;">${escapeHtml(lens?.closed ? "Closed" : String(lens?.source || "live"))}</span>
          </div>
          <div class="live-lens-segment-copy">${escapeHtml(projectionLine)}</div>
          <div class="live-lens-metric-grid">
            ${renderSeasonMetric("Home win", modelProb)}
            ${renderSeasonMetric("Baseline", baselineProb)}
            ${renderSeasonMetric("Market", marketProb)}
            ${renderSeasonMetric("ML", moneyline.pick ? `${String(moneyline.pick).toUpperCase()} ${formatPercent(moneyline.edge)}` : "-")}
            ${renderSeasonMetric("Spread", spread.pick ? `${String(spread.pick).toUpperCase()} ${formatSigned(spread.homeLine, 1)} (${formatSigned(spread.edge)})` : "-")}
            ${renderSeasonMetric("Total", total.pick ? `${String(total.pick).toUpperCase()} ${formatLine(total.line)} (${formatSigned(total.edge)})` : "-")}
          </div>
        </article>`;
    }
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
    if (state.season) {
      return `
        <div class="live-lens-segment-grid">
          ${lensRows.map((lens) => renderLensCard(lens)).join("")}
        </div>`;
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
    const tableClass = state.season ? "table live-lens-table" : "table";
    const projectionClass = state.season ? "live-lens-value-accent" : "";
    return `
      <table class="${tableClass}">
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
              <td>${escapeHtml(propTierLabel(prop))}</td>
              <td>${escapeHtml(String(prop.selection || ""))}</td>
              <td>${escapeHtml(formatLine(prop.line))}</td>
              <td>${escapeHtml(formatLine(prop.actual))}</td>
              <td class="${projectionClass}" style="color:${pickTone(prop.liveEdge)};font-weight:600;">${escapeHtml(formatLine(prop.liveProjection))}</td>
              <td>${escapeHtml(formatLine(prop.modelMean))}</td>
              <td class="${projectionClass}" style="color:${pickTone(prop.liveEdge)};font-weight:600;">${escapeHtml(formatSigned(prop.liveEdge))}</td>
              <td>${escapeHtml(formatPercent(prop.edge))}</td>
              <td>${escapeHtml(formatOdds(prop.odds))}</td>
              <td><span style="display:inline-block;padding:2px 8px;border-radius:999px;background:${badgeTone(prop.status)};color:#fff;">${escapeHtml(String(prop.status || "pending"))}</span></td>
            </tr>`).join("")}
        </tbody>
      </table>`;
  }

  function renderPropSection(title, copy, props, emptyMessage) {
    if (!Array.isArray(props) || !props.length) {
      return `
        <section class="live-lens-prop-section">
          <div class="live-lens-prop-head">
            <div class="live-lens-prop-title">${escapeHtml(title)}</div>
          </div>
          <div class="empty">${escapeHtml(emptyMessage)}</div>
        </section>`;
    }
    return `
      <section class="live-lens-prop-section">
        <div class="live-lens-prop-head">
          <div>
            <div class="live-lens-prop-title">${escapeHtml(title)}</div>
            <div class="live-lens-prop-copy">${escapeHtml(copy)}</div>
          </div>
          <span class="cards-chip">${escapeHtml(String(props.length))} rows</span>
        </div>
        ${renderPropTable(props)}
      </section>`;
  }

  function liveOpportunitiesEmptyMessage(game) {
    const status = String(game?.status?.abstract || "").toLowerCase();
    if (status && status !== "live" && status !== "final") {
      return "Live opportunities unlock when the game goes live.";
    }
    return "No current live prop opportunities for this game.";
  }

  function renderPropSections(game) {
    const liveProps = Array.isArray(game?.liveProps) ? game.liveProps : [];
    const trackedProps = Array.isArray(game?.trackedProps) ? game.trackedProps : [];
    if (!liveProps.length && !trackedProps.length) {
      return '<div class="empty">No live or tracked player props for this game.</div>';
    }
    const sections = [];
    sections.push(renderPropSection(
      "Live opportunities",
      "Current in-game market lines with positive model-vs-market edge, ranked by edge then live projection gap.",
      liveProps,
      liveOpportunitiesEmptyMessage(game)
    ));
    if (trackedProps.length) {
      sections.push(renderPropSection(
        "Tracked pregame props",
        "Original official and playable pregame props, updated with current game state.",
        trackedProps,
        "No tracked pregame props for this game."
      ));
    }
    return sections.join("");
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
      const panelClass = state.season ? "panel live-lens-game-panel" : "panel";
      return `
        <section class="${panelClass}">
          <div class="panel-title">${escapeHtml(String(away.abbr || away.name || "Away"))} at ${escapeHtml(String(home.abbr || home.name || "Home"))}</div>
          <div class="status-line">${escapeHtml(String(status.abstract || ""))} — ${escapeHtml(String(status.detailed || game.startTime || ""))}</div>
          <div class="status-line">Score: ${escapeHtml(String(score.away ?? "-"))} - ${escapeHtml(String(score.home ?? "-"))}${liveText ? ` | ${escapeHtml(liveText)}` : ""}</div>
          ${renderGameLens(game)}
          ${renderPropSections(game)}
        </section>`;
    }).join("");
  }

  async function load() {
    try {
      const response = await fetch(`${state.apiPath}?date=${encodeURIComponent(state.date)}`);
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

  if (monthsNode) {
    monthsNode.addEventListener("click", function (event) {
      const button = event.target.closest("[data-live-lens-month]");
      if (!button || !monthsNode.contains(button)) return;
      event.preventDefault();
      setMonthFilter(button.getAttribute("data-live-lens-month") || "all");
    });
  }

  if (daysNode) {
    daysNode.addEventListener("click", function (event) {
      const button = event.target.closest("[data-live-lens-date]");
      if (!button || !daysNode.contains(button)) return;
      event.preventDefault();
      activateDate(button.getAttribute("data-live-lens-date") || "");
    });
  }

  (async function init() {
    if (state.season) {
      await loadSeasonManifest();
    }
    await load();
  })();
})();