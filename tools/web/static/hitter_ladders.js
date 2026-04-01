(function () {
  let bootstrap = {};
  try {
    const bootstrapEl = document.getElementById("hitterLaddersBootstrap");
    bootstrap = bootstrapEl ? JSON.parse(bootstrapEl.textContent || "{}") : {};
  } catch (_error) {
    bootstrap = {};
  }

  const state = {
    date: String(bootstrap.date || ""),
    prop: String(bootstrap.prop || "hits"),
    game: String(bootstrap.game || ""),
    team: String(bootstrap.team || ""),
    hitter: String(bootstrap.hitter || ""),
    sort: String(bootstrap.sort || "team"),
  };

  const formEl = document.getElementById("hitterLadderForm");
  const dateInputEl = document.getElementById("hitterLadderDateInput");
  const propInputEl = document.getElementById("hitterLadderPropInput");
  const gameInputEl = document.getElementById("hitterLadderGameInput");
  const teamInputEl = document.getElementById("hitterLadderTeamInput");
  const hitterInputEl = document.getElementById("hitterLadderHitterInput");
  const sortInputEl = document.getElementById("hitterLadderSortInput");
  const headerMetaEl = document.getElementById("hitterLadderHeaderMeta");
  const sourceMetaEl = document.getElementById("hitterLadderSourceMeta");
  const summaryEl = document.getElementById("hitterLadderSummary");
  const selectedHitterEl = document.getElementById("hitterLadderSelectedHitter");
  const gridEl = document.getElementById("hitterLadderGrid");
  const dateBadgeEl = document.getElementById("hitterLadderDateBadge");
  const propBadgeEl = document.getElementById("hitterLadderPropBadge");
  const prevDateLinkEl = document.getElementById("hitterLadderPrevDateLink");
  const nextDateLinkEl = document.getElementById("hitterLadderNextDateLink");

  function escapeHtml(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function formatNumber(value, digits = 2) {
    if (value == null || value === "") return "-";
    const num = Number(value);
    if (!Number.isFinite(num)) return "-";
    return num.toFixed(digits);
  }

  function formatCount(value) {
    const num = Number(value);
    if (!Number.isFinite(num)) return "-";
    return String(Math.round(num));
  }

  function formatPercent(value) {
    if (value == null || value === "") return "-";
    const num = Number(value);
    if (!Number.isFinite(num)) return "-";
    return `${(num * 100).toFixed(1)}%`;
  }

  function formatOdds(value) {
    if (value == null || value === "") return "-";
    const num = Number(value);
    if (!Number.isFinite(num) || num === 0) return "-";
    return num > 0 ? `+${Math.round(num)}` : `${Math.round(num)}`;
  }

  function toNumber(value) {
    if (value == null || value === "") return null;
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  }

  function activeMarketEntry(row, payload) {
    const entries = Array.isArray(row.marketLinesByStat) ? row.marketLinesByStat : [];
    const activeStat = String(payload.prop || "");
    return entries.find((entry) => String(entry.stat || "") === activeStat) || null;
  }

  function thresholdBookOdds(row, payload, total) {
    const market = activeMarketEntry(row, payload);
    if (!market) return "";
    const targetLine = Number(total) - 0.5;
    if (!Number.isFinite(targetLine) || targetLine < 0) return "";
    const candidates = [market, ...(Array.isArray(market.alternates) ? market.alternates : [])];
    const match = candidates.find((candidate) => {
      const line = toNumber(candidate && (candidate.line ?? candidate.value));
      return line != null && Math.abs(line - targetLine) < 1e-9;
    });
    if (!match) return "";
    const overOdds = match.overOdds ?? match.over_odds;
    const formatted = formatOdds(overOdds);
    return formatted === "-" ? "" : formatted;
  }

  function renderMarketLineChips(row, payload) {
    const entries = Array.isArray(row.marketLinesByStat) ? row.marketLinesByStat : [];
    if (!entries.length) return "";
    const activeStat = String(payload.prop || "");
    return `
      <div class="ladder-market-lines">
        ${entries.map((entry) => {
          const isActive = String(entry.stat || "") === activeStat;
          const oddsBits = [];
          if (entry.overOdds != null) oddsBits.push(`O ${escapeHtml(formatOdds(entry.overOdds))}`);
          if (entry.underOdds != null) oddsBits.push(`U ${escapeHtml(formatOdds(entry.underOdds))}`);
          return `
            <span class="ladder-market-line${isActive ? " is-active" : ""}">
              <span class="ladder-market-line-label">${escapeHtml(entry.label || entry.stat || "Prop")}</span>
              <strong>${escapeHtml(formatNumber(entry.line, 1))}</strong>
              ${oddsBits.length ? `<span class="ladder-market-line-odds">${oddsBits.join(" / ")}</span>` : ""}
            </span>
          `;
        }).join("")}
      </div>
    `;
  }

  function pageHref(dateValue, propValue, gameValue, teamValue, hitterValue, sortValue) {
    const params = new URLSearchParams();
    if (dateValue) params.set("date", dateValue);
    if (propValue) params.set("prop", propValue);
    if (gameValue) params.set("game", gameValue);
    if (teamValue) params.set("team", teamValue);
    if (hitterValue) params.set("hitter", hitterValue);
    if (sortValue) params.set("sort", sortValue);
    return `/hitter-ladders?${params.toString()}`;
  }

  function apiHref(dateValue, propValue, gameValue, teamValue, hitterValue, sortValue) {
    const params = new URLSearchParams();
    if (dateValue) params.set("date", dateValue);
    if (propValue) params.set("prop", propValue);
    if (gameValue) params.set("game", gameValue);
    if (teamValue) params.set("team", teamValue);
    if (hitterValue) params.set("hitter", hitterValue);
    if (sortValue) params.set("sort", sortValue);
    return `/api/hitter-ladders?${params.toString()}`;
  }

  function renderGameSelector(payload) {
    if (!gameInputEl) return;
    const options = Array.isArray(payload.gameOptions) ? payload.gameOptions : [];
    const currentValue = String(payload.selectedGame || state.game || "");
    gameInputEl.innerHTML = [
      '<option value="">All games</option>',
      ...options.map((option) => {
        const value = String(option.value || "");
        const selected = value === currentValue ? ' selected' : '';
        return `<option value="${escapeHtml(value)}"${selected}>${escapeHtml(option.label || value)}</option>`;
      }),
    ].join("");
    gameInputEl.value = currentValue;
  }

  function renderTeamSelector(payload) {
    const options = Array.isArray(payload.teamOptions) ? payload.teamOptions : [];
    const currentValue = String(payload.selectedTeam || state.team || "");
    teamInputEl.innerHTML = [
      '<option value="">All teams</option>',
      ...options.map((option) => {
        const value = String(option.value || "");
        const selected = value === currentValue ? ' selected' : '';
        return `<option value="${escapeHtml(value)}"${selected}>${escapeHtml(option.label || value)}</option>`;
      }),
    ].join("");
    teamInputEl.value = currentValue;
  }

  function renderHitterSelector(payload) {
    const options = Array.isArray(payload.hitterOptions) ? payload.hitterOptions : [];
    const currentValue = String(payload.selectedHitter || state.hitter || "");
    hitterInputEl.innerHTML = [
      '<option value="">All hitters</option>',
      ...options.map((option) => {
        const value = String(option.value || "");
        const selected = value === currentValue ? ' selected' : '';
        return `<option value="${escapeHtml(value)}"${selected}>${escapeHtml(option.label || value)}</option>`;
      }),
    ].join("");
    hitterInputEl.value = currentValue;
  }

  function historyCalloutsForTotal(row, total) {
    const historyRows = Array.isArray(row.historyRows) ? row.historyRows : [];
    const targetTotal = Number(total);
    if (!Number.isFinite(targetTotal)) return [];
    return historyRows.filter((historyRow) => {
      const value = Number(historyRow && historyRow.value);
      return Number.isFinite(value) && Math.round(value) === Math.round(targetTotal);
    });
  }

  function renderHistoryCallouts(row, total) {
    const callouts = historyCalloutsForTotal(row, total);
    if (!callouts.length) return "";
    return `
      <div class="ladder-row-history-callouts">
        ${callouts.map((historyRow) => `
          <span class="ladder-row-history-chip is-${escapeHtml(String(historyRow.trend || 'neutral'))}" title="${escapeHtml(`${historyRow.label || ''}: ${formatNumber(historyRow.value, 2)} over ${formatCount(historyRow.games)} G`)}">
            <span class="ladder-row-history-chip-label">${escapeHtml(historyRow.label || "")}</span>
            <strong>${escapeHtml(formatNumber(historyRow.value, 2))}</strong>
          </span>
        `).join("")}
      </div>
    `;
  }

  function renderTotalCell(row, total, prefix = "") {
    const totalText = prefix ? `${prefix} ${formatCount(total)}` : formatCount(total);
    return `
      <div class="ladder-total-cell">
        <span class="ladder-total-value">${escapeHtml(totalText)}</span>
        ${renderHistoryCallouts(row, total)}
      </div>
    `;
  }

  function renderSelectedHitter(payload) {
    selectedHitterEl.innerHTML = "";
    selectedHitterEl.style.display = "none";
  }

  function renderSummary(payload) {
    const summary = payload.summary || {};
    const simCounts = Array.isArray(summary.simCounts) ? summary.simCounts : [];
    const isExact = String(payload.ladderShape || "threshold") === "exact";
    summaryEl.innerHTML = `
      <article class="ladder-stat">
        <div class="ladder-stat-label">Date</div>
        <div class="ladder-stat-value">${escapeHtml(payload.date || state.date || "-")}</div>
      </article>
      <article class="ladder-stat">
        <div class="ladder-stat-label">Prop</div>
        <div class="ladder-stat-value">${escapeHtml(payload.propLabel || state.prop || "-")}</div>
      </article>
      <article class="ladder-stat">
        <div class="ladder-stat-label">Games</div>
        <div class="ladder-stat-value">${formatCount(summary.games)}</div>
      </article>
      <article class="ladder-stat">
        <div class="ladder-stat-label">Available games</div>
        <div class="ladder-stat-value">${formatCount(summary.availableGames)}</div>
      </article>
      <article class="ladder-stat">
        <div class="ladder-stat-label">Hitters</div>
        <div class="ladder-stat-value">${formatCount(summary.hitters)}</div>
      </article>
      <article class="ladder-stat">
        <div class="ladder-stat-label">Available hitters</div>
        <div class="ladder-stat-value">${formatCount(summary.availableHitters)}</div>
      </article>
      <article class="ladder-stat">
        <div class="ladder-stat-label">${isExact ? "Ladder shape" : "Stored top N"}</div>
        <div class="ladder-stat-value">${isExact ? "Exact" : escapeHtml(summary.topN == null ? "-" : formatCount(summary.topN))}</div>
      </article>
      <article class="ladder-stat">
        <div class="ladder-stat-label">Sim counts seen</div>
        <div class="ladder-stat-value">${escapeHtml(simCounts.length ? simCounts.join(", ") : "-")}</div>
      </article>
    `;
  }

  function renderEmpty(payload) {
    const detail = payload && payload.error ? ` (${escapeHtml(payload.error)})` : "";
    gridEl.innerHTML = `<div class="ladder-empty">No hitter ladder data found for this date and prop${detail}.</div>`;
  }

  function renderCard(row, payload) {
    const ladderRows = Array.isArray(row.ladder) ? row.ladder : [];
    const isExact = String(row.ladderShape || payload.ladderShape || "threshold") === "exact";
    const overLineText = row.marketLine == null || row.overLineCount == null
      ? ""
      : `<span class="ladder-pill"><span>Over ${escapeHtml(formatNumber(row.marketLine, 1))}</span><strong>${escapeHtml(formatCount(row.overLineCount))}</strong><span>${escapeHtml(formatPercent(row.overLineProb))}</span></span>`;
    const gameHref = row.gamePk != null && payload.date
      ? `/game/${encodeURIComponent(String(row.gamePk))}?date=${encodeURIComponent(String(payload.date))}`
      : "";
    const teamLogo = row.teamLogoUrl
      ? `<img class="ladder-team-logo ladder-team-logo-primary" src="${escapeHtml(row.teamLogoUrl)}" alt="${escapeHtml(row.team || 'Team')} logo" loading="lazy" />`
      : `<div class="ladder-team-logo ladder-team-logo-primary ladder-team-logo-fallback">${escapeHtml(String((row.team || "?").slice(0, 1) || "?"))}</div>`;
    const headshot = row.headshotUrl
      ? `<img class="ladder-player-headshot" src="${escapeHtml(row.headshotUrl)}" alt="${escapeHtml(row.hitterName || 'Hitter')} headshot" loading="lazy" />`
      : `<div class="ladder-player-headshot ladder-player-headshot-fallback">${escapeHtml(String((row.hitterName || "?").slice(0, 1) || "?"))}</div>`;
    const subtitleBits = [row.matchup || `${row.team || ""} vs ${row.opponent || ""}`];
    if (row.lineupOrder != null && row.lineupOrder !== "") {
      subtitleBits.push(`No. ${escapeHtml(formatCount(row.lineupOrder))}`);
    }
    const ladderTableRows = isExact
      ? ladderRows.map((ladderRow) => `
        <tr>
          <td>${renderTotalCell(row, ladderRow.total)}</td>
          <td>${escapeHtml(formatCount(ladderRow.hitCount))}</td>
          <td>${escapeHtml(formatPercent(ladderRow.hitProb))}</td>
          <td>${escapeHtml(thresholdBookOdds(row, payload, ladderRow.total))}</td>
          <td>${escapeHtml(formatCount(ladderRow.exactCount))}</td>
          <td>${escapeHtml(formatPercent(ladderRow.exactProb))}</td>
        </tr>
      `).join("")
      : ladderRows.map((ladderRow) => `
        <tr>
          <td>${renderTotalCell(row, ladderRow.total, '>=')}</td>
          <td>${escapeHtml(formatCount(ladderRow.hitCount))}</td>
          <td>${escapeHtml(formatPercent(ladderRow.hitProb))}</td>
          <td>${escapeHtml(thresholdBookOdds(row, payload, ladderRow.total))}</td>
        </tr>
      `).join("");

    return `
      <article class="ladder-card">
        <div class="ladder-card-head">
          <div class="ladder-card-identity">
            ${headshot}
            <div>
              <h2 class="ladder-card-title">${escapeHtml(row.hitterName || "Unknown hitter")}</h2>
              <div class="ladder-card-subtitle">${subtitleBits.join(" • ")}</div>
            </div>
          </div>
          <div class="ladder-card-actions">
            ${gameHref ? `<a class="ladder-card-link" href="${gameHref}">Game view</a>` : ""}
            ${teamLogo}
          </div>
        </div>
        <div class="ladder-pills">
          <span class="ladder-pill"><span>Mean</span><strong>${escapeHtml(formatNumber(row.mean, 2))}</strong></span>
          <span class="ladder-pill"><span>Mode</span><strong>${escapeHtml(row.mode == null ? "-" : formatCount(row.mode))}</strong><span>${escapeHtml(row.modeProb == null ? "-" : formatPercent(row.modeProb))}</span></span>
          <span class="ladder-pill"><span>PA mean</span><strong>${escapeHtml(formatNumber(row.paMean, 2))}</strong></span>
          <span class="ladder-pill"><span>AB mean</span><strong>${escapeHtml(formatNumber(row.abMean, 2))}</strong></span>
          ${row.marketLine == null ? "" : `<span class="ladder-pill"><span>Market line</span><strong>${escapeHtml(formatNumber(row.marketLine, 1))}</strong></span>`}
          ${overLineText}
        </div>
        ${renderMarketLineChips(row, payload)}
        <div class="ladder-table-wrap">
          <table class="ladder-table">
            <thead>
              ${isExact
                ? `<tr>
                    <th>Total</th>
                    <th>&ge; Total</th>
                    <th>Hit %</th>
                    <th>Hit Odds</th>
                    <th>Exact</th>
                    <th>Exact %</th>
                  </tr>`
                : `<tr>
                    <th>Threshold</th>
                    <th>Hit Count</th>
                    <th>Hit %</th>
                    <th>Hit Odds</th>
                  </tr>`}
            </thead>
            <tbody>
              ${ladderTableRows}
            </tbody>
          </table>
        </div>
      </article>
    `;
  }

  function renderPayload(payload) {
    dateBadgeEl.textContent = payload.date || state.date || "-";
    propBadgeEl.textContent = payload.propLabel || payload.prop || state.prop || "-";
    if (sortInputEl) {
      sortInputEl.value = String(payload.selectedSort || state.sort || "team");
    }
    renderGameSelector(payload);
    renderTeamSelector(payload);
    renderHitterSelector(payload);
    renderSelectedHitter(payload);

    const summary = payload.summary || {};
    const simCounts = Array.isArray(summary.simCounts) ? summary.simCounts : [];
    const shape = String(payload.ladderShape || "threshold");
    const sortLabel = String((Array.isArray(payload.sortOptions) ? payload.sortOptions : []).find((option) => String(option.value || "") === String(payload.selectedSort || state.sort || "team"))?.label || (payload.selectedSort || state.sort || "team"));
    const gameLabel = String((Array.isArray(payload.gameOptions) ? payload.gameOptions : []).find((option) => String(option.value || "") === String(payload.selectedGame || state.game || ""))?.label || (payload.selectedGame || state.game || ""));
    const teamLabel = String((Array.isArray(payload.teamOptions) ? payload.teamOptions : []).find((option) => String(option.value || "") === String(payload.selectedTeam || state.team || ""))?.label || (payload.selectedTeam || state.team || ""));
    headerMetaEl.textContent = payload.found
      ? `${summary.hitters || 0} hitters across ${summary.games || 0} games from ${shape === "exact" ? "stored exact hitter distributions" : `stored top-${summary.topN || "?"} hitter likelihoods`}. Sorted by ${sortLabel}. Sim counts: ${simCounts.length ? simCounts.join(", ") : "-"}.${state.game ? ` Filtered to game ${gameLabel || state.game}.` : ""}${state.team ? ` Filtered to team ${teamLabel || state.team}.` : ""}${state.hitter ? ` Filtered to hitter ${state.hitter}.` : ""}`
      : "No hitter ladder data found for this selection.";

    sourceMetaEl.textContent = `Sim dir: ${payload.sourceDir || "-"} | Market file: ${payload.marketSource || "-"} | Default daily sims: ${payload.defaultSims || "-"} | Shape: ${shape} ladder`;

    const nav = payload.nav || {};
    prevDateLinkEl.href = pageHref(nav.prevDate || state.date, state.prop, state.game, state.team, state.hitter, state.sort);
    nextDateLinkEl.href = pageHref(nav.nextDate || state.date, state.prop, state.game, state.team, state.hitter, state.sort);
    prevDateLinkEl.style.visibility = nav.prevDate ? "visible" : "hidden";
    nextDateLinkEl.style.visibility = nav.nextDate ? "visible" : "hidden";

    renderSummary(payload);
    if (!payload.found || !Array.isArray(payload.rows) || !payload.rows.length) {
      renderEmpty(payload);
      return;
    }
    gridEl.innerHTML = payload.rows.map((row) => renderCard(row, payload)).join("");
  }

  async function loadPayload() {
    dateInputEl.value = state.date;
    propInputEl.value = state.prop;
    if (gameInputEl) gameInputEl.value = state.game;
    if (teamInputEl) teamInputEl.value = state.team;
    hitterInputEl.value = state.hitter;
    if (sortInputEl) sortInputEl.value = state.sort;
    gridEl.innerHTML = '<div class="cards-loading-state">Loading hitter ladders...</div>';
    summaryEl.innerHTML = '<div class="cards-loading-state">Loading ladder summary...</div>';

    const response = await fetch(apiHref(state.date, state.prop, state.game, state.team, state.hitter, state.sort));
    const payload = await response.json();
    renderPayload(payload);
  }

  formEl.addEventListener("submit", async (event) => {
    event.preventDefault();
    state.date = String(dateInputEl.value || "");
    state.prop = String(propInputEl.value || "hits");
    state.game = String((gameInputEl && gameInputEl.value) || "");
    state.team = String((teamInputEl && teamInputEl.value) || "");
    state.hitter = String(hitterInputEl.value || "");
    state.sort = String((sortInputEl && sortInputEl.value) || "team");
    window.history.replaceState({}, "", pageHref(state.date, state.prop, state.game, state.team, state.hitter, state.sort));
    await loadPayload();
  });

  propInputEl.addEventListener("change", () => {
    state.game = "";
    if (gameInputEl) gameInputEl.value = "";
    state.team = "";
    if (teamInputEl) teamInputEl.value = "";
    state.hitter = "";
    if (hitterInputEl) hitterInputEl.value = "";
    if (formEl.requestSubmit) {
      formEl.requestSubmit();
      return;
    }
    formEl.dispatchEvent(new Event("submit", { cancelable: true }));
  });

  if (teamInputEl) {
    teamInputEl.addEventListener("change", () => {
      state.hitter = "";
      if (hitterInputEl) hitterInputEl.value = "";
      if (formEl.requestSubmit) {
        formEl.requestSubmit();
        return;
      }
      formEl.dispatchEvent(new Event("submit", { cancelable: true }));
    });
  }

  if (gameInputEl) {
    gameInputEl.addEventListener("change", () => {
      state.team = "";
      state.hitter = "";
      if (teamInputEl) teamInputEl.value = "";
      if (hitterInputEl) hitterInputEl.value = "";
      if (formEl.requestSubmit) {
        formEl.requestSubmit();
        return;
      }
      formEl.dispatchEvent(new Event("submit", { cancelable: true }));
    });
  }

  hitterInputEl.addEventListener("change", () => {
    if (formEl.requestSubmit) {
      formEl.requestSubmit();
      return;
    }
    formEl.dispatchEvent(new Event("submit", { cancelable: true }));
  });

  if (sortInputEl) {
    sortInputEl.addEventListener("change", () => {
      if (formEl.requestSubmit) {
        formEl.requestSubmit();
        return;
      }
      formEl.dispatchEvent(new Event("submit", { cancelable: true }));
    });
  }

  loadPayload().catch((error) => {
    headerMetaEl.textContent = "Failed to load hitter ladders.";
    sourceMetaEl.textContent = String(error && error.message ? error.message : error || "unknown error");
    summaryEl.innerHTML = '<div class="ladder-empty">Hitter ladder data could not be loaded.</div>';
    selectedHitterEl.innerHTML = "";
    gridEl.innerHTML = "";
  });
})();