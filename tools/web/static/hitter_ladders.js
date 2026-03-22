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
    team: String(bootstrap.team || ""),
    hitter: String(bootstrap.hitter || ""),
    sort: String(bootstrap.sort || "team"),
  };

  const formEl = document.getElementById("hitterLadderForm");
  const dateInputEl = document.getElementById("hitterLadderDateInput");
  const propInputEl = document.getElementById("hitterLadderPropInput");
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

  function pageHref(dateValue, propValue, teamValue, hitterValue, sortValue) {
    const params = new URLSearchParams();
    if (dateValue) params.set("date", dateValue);
    if (propValue) params.set("prop", propValue);
    if (teamValue) params.set("team", teamValue);
    if (hitterValue) params.set("hitter", hitterValue);
    if (sortValue) params.set("sort", sortValue);
    return `/hitter-ladders?${params.toString()}`;
  }

  function apiHref(dateValue, propValue, teamValue, hitterValue, sortValue) {
    const params = new URLSearchParams();
    if (dateValue) params.set("date", dateValue);
    if (propValue) params.set("prop", propValue);
    if (teamValue) params.set("team", teamValue);
    if (hitterValue) params.set("hitter", hitterValue);
    if (sortValue) params.set("sort", sortValue);
    return `/api/hitter-ladders?${params.toString()}`;
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

  function renderSelectedHitter(payload) {
    const options = Array.isArray(payload.hitterOptions) ? payload.hitterOptions : [];
    const currentValue = String(payload.selectedHitter || state.hitter || "");
    if (!currentValue) {
      selectedHitterEl.innerHTML = "";
      selectedHitterEl.style.display = "none";
      return;
    }

    const selected = options.find((option) => String(option.value || "") === currentValue)
      || (Array.isArray(payload.rows) ? payload.rows.find((row) => String(row.hitterId || "") === currentValue) : null);

    if (!selected) {
      selectedHitterEl.innerHTML = "";
      selectedHitterEl.style.display = "none";
      return;
    }

    const headshotUrl = selected.headshotUrl || "";
    const teamLogoUrl = selected.teamLogoUrl || "";
    const hitterName = selected.hitterName || selected.playerName || selected.label || currentValue;
    const label = selected.label || hitterName;
    const clearHref = pageHref(state.date, state.prop, state.team, "", state.sort);

    selectedHitterEl.style.display = "block";
    selectedHitterEl.innerHTML = `
      <div class="ladder-selected-card">
        <div class="ladder-selected-identity">
          ${teamLogoUrl ? `<img class="ladder-selected-team-logo" src="${escapeHtml(teamLogoUrl)}" alt="team logo" loading="lazy" />` : ""}
          ${headshotUrl ? `<img class="ladder-selected-headshot" src="${escapeHtml(headshotUrl)}" alt="${escapeHtml(hitterName)} headshot" loading="lazy" />` : ""}
          <div>
            <div class="ladder-selected-kicker">Selected hitter</div>
            <div class="ladder-selected-name">${escapeHtml(hitterName)}</div>
            <div class="ladder-selected-meta">${escapeHtml(label)}</div>
          </div>
        </div>
        <a class="ladder-selected-clear" href="${clearHref}">Show all</a>
      </div>
    `;
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
          <td>${escapeHtml(formatCount(ladderRow.total))}</td>
          <td>${escapeHtml(formatCount(ladderRow.hitCount))}</td>
          <td>${escapeHtml(formatPercent(ladderRow.hitProb))}</td>
          <td>${escapeHtml(formatCount(ladderRow.exactCount))}</td>
          <td>${escapeHtml(formatPercent(ladderRow.exactProb))}</td>
        </tr>
      `).join("")
      : ladderRows.map((ladderRow) => `
        <tr>
          <td>&ge; ${escapeHtml(formatCount(ladderRow.total))}</td>
          <td>${escapeHtml(formatCount(ladderRow.hitCount))}</td>
          <td>${escapeHtml(formatPercent(ladderRow.hitProb))}</td>
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
        <div class="ladder-table-wrap">
          <table class="ladder-table">
            <thead>
              ${isExact
                ? `<tr>
                    <th>Total</th>
                    <th>&ge; Total</th>
                    <th>Hit %</th>
                    <th>Exact</th>
                    <th>Exact %</th>
                  </tr>`
                : `<tr>
                    <th>Threshold</th>
                    <th>Hit Count</th>
                    <th>Hit %</th>
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
    renderTeamSelector(payload);
    renderHitterSelector(payload);
    renderSelectedHitter(payload);

    const summary = payload.summary || {};
    const simCounts = Array.isArray(summary.simCounts) ? summary.simCounts : [];
    const shape = String(payload.ladderShape || "threshold");
    const sortLabel = String((Array.isArray(payload.sortOptions) ? payload.sortOptions : []).find((option) => String(option.value || "") === String(payload.selectedSort || state.sort || "team"))?.label || (payload.selectedSort || state.sort || "team"));
    const teamLabel = String((Array.isArray(payload.teamOptions) ? payload.teamOptions : []).find((option) => String(option.value || "") === String(payload.selectedTeam || state.team || ""))?.label || (payload.selectedTeam || state.team || ""));
    headerMetaEl.textContent = payload.found
      ? `${summary.hitters || 0} hitters across ${summary.games || 0} games from ${shape === "exact" ? "stored exact hitter distributions" : `stored top-${summary.topN || "?"} hitter likelihoods`}. Sorted by ${sortLabel}. Sim counts: ${simCounts.length ? simCounts.join(", ") : "-"}.${state.team ? ` Filtered to team ${teamLabel || state.team}.` : ""}${state.hitter ? ` Filtered to hitter ${state.hitter}.` : ""}`
      : "No hitter ladder data found for this selection.";

    sourceMetaEl.textContent = `Sim dir: ${payload.sourceDir || "-"} | Market file: ${payload.marketSource || "-"} | Default daily sims: ${payload.defaultSims || "-"} | Shape: ${shape} ladder`;

    const nav = payload.nav || {};
    prevDateLinkEl.href = pageHref(nav.prevDate || state.date, state.prop, state.team, state.hitter, state.sort);
    nextDateLinkEl.href = pageHref(nav.nextDate || state.date, state.prop, state.team, state.hitter, state.sort);
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
    if (teamInputEl) teamInputEl.value = state.team;
    hitterInputEl.value = state.hitter;
    if (sortInputEl) sortInputEl.value = state.sort;
    gridEl.innerHTML = '<div class="cards-loading-state">Loading hitter ladders...</div>';
    summaryEl.innerHTML = '<div class="cards-loading-state">Loading ladder summary...</div>';

    const response = await fetch(apiHref(state.date, state.prop, state.team, state.hitter, state.sort));
    const payload = await response.json();
    renderPayload(payload);
  }

  formEl.addEventListener("submit", async (event) => {
    event.preventDefault();
    state.date = String(dateInputEl.value || "");
    state.prop = String(propInputEl.value || "hits");
    state.team = String((teamInputEl && teamInputEl.value) || "");
    state.hitter = String(hitterInputEl.value || "");
    state.sort = String((sortInputEl && sortInputEl.value) || "team");
    window.history.replaceState({}, "", pageHref(state.date, state.prop, state.team, state.hitter, state.sort));
    await loadPayload();
  });

  propInputEl.addEventListener("change", () => {
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