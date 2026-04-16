(function () {
  let bootstrap = {};
  try {
    const bootstrapEl = document.getElementById("hrTargetsBootstrap");
    bootstrap = bootstrapEl ? JSON.parse(bootstrapEl.textContent || "{}") : {};
  } catch (_error) {
    bootstrap = {};
  }

  const state = {
    date: String(bootstrap.date || ""),
    game: String(bootstrap.game || ""),
    team: String(bootstrap.team || ""),
    hitter: String(bootstrap.hitter || ""),
    sort: String(bootstrap.sort || "score"),
  };

  const root = {
    form: document.getElementById("hrTargetsForm"),
    dateInput: document.getElementById("hrTargetsDateInput"),
    gameInput: document.getElementById("hrTargetsGameInput"),
    teamInput: document.getElementById("hrTargetsTeamInput"),
    hitterInput: document.getElementById("hrTargetsHitterInput"),
    sortInput: document.getElementById("hrTargetsSortInput"),
    headerMeta: document.getElementById("hrTargetsHeaderMeta"),
    sourceMeta: document.getElementById("hrTargetsSourceMeta"),
    summary: document.getElementById("hrTargetsSummary"),
    grid: document.getElementById("hrTargetsGrid"),
    dateBadge: document.getElementById("hrTargetsDateBadge"),
    prevLink: document.getElementById("hrTargetsPrevDateLink"),
    nextLink: document.getElementById("hrTargetsNextDateLink"),
  };

  function escapeHtml(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function formatPercent(value) {
    const num = Number(value);
    if (!Number.isFinite(num)) return "-";
    return `${(num * 100).toFixed(1)}%`;
  }

  function formatNumber(value, digits = 1) {
    const num = Number(value);
    if (!Number.isFinite(num)) return "-";
    return num.toFixed(digits);
  }

  function shiftDate(dateValue, deltaDays) {
    if (!dateValue) return "";
    const parts = String(dateValue).split("-").map((part) => Number.parseInt(part, 10));
    if (parts.length !== 3 || parts.some((value) => !Number.isFinite(value))) return dateValue;
    const dt = new Date(parts[0], parts[1] - 1, parts[2]);
    dt.setDate(dt.getDate() + deltaDays);
    const year = dt.getFullYear();
    const month = String(dt.getMonth() + 1).padStart(2, "0");
    const day = String(dt.getDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  }

  function pageHref(dateValue, gameValue, teamValue, hitterValue, sortValue) {
    const params = new URLSearchParams();
    if (dateValue) params.set("date", dateValue);
    if (gameValue) params.set("game", gameValue);
    if (teamValue) params.set("team", teamValue);
    if (hitterValue) params.set("hitter", hitterValue);
    if (sortValue) params.set("sort", sortValue);
    return `/hr-targets?${params.toString()}`;
  }

  function apiHref(dateValue, gameValue, teamValue, hitterValue, sortValue) {
    const params = new URLSearchParams();
    if (dateValue) params.set("date", dateValue);
    if (gameValue) params.set("game", gameValue);
    if (teamValue) params.set("team", teamValue);
    if (hitterValue) params.set("hitter", hitterValue);
    if (sortValue) params.set("sort", sortValue);
    return `/api/hr-targets?${params.toString()}`;
  }

  function renderOptions(selectEl, options, currentValue, placeholder) {
    if (!selectEl) return;
    selectEl.innerHTML = [
      `<option value="">${escapeHtml(placeholder)}</option>`,
      ...options.map((option) => {
        const value = String(option.value || "");
        const selected = value === currentValue ? ' selected' : '';
        return `<option value="${escapeHtml(value)}"${selected}>${escapeHtml(option.label || value)}</option>`;
      }),
    ].join("");
    selectEl.value = currentValue;
  }

  function badgeClass(label) {
    const normalized = String(label || "").toLowerCase();
    if (normalized === "strong") return "is-strong";
    if (normalized === "solid") return "is-solid";
    if (normalized === "watch") return "is-watch";
    return "is-thin";
  }

  function targetCardMarkup(row) {
    const reasons = Array.isArray(row.hr_target_reasons) ? row.hr_target_reasons : [];
    return `
      <article class="hr-target-card">
        <div class="hr-target-card-head">
          <div class="hr-target-player-block">
            ${row.headshotUrl ? `<img class="hr-target-headshot" src="${escapeHtml(row.headshotUrl)}" alt="${escapeHtml(row.hitterName || row.player_name || "Hitter")}" loading="lazy" />` : ""}
            <div>
              <div class="hr-target-player-name">${escapeHtml(row.hitterName || row.player_name || "Unknown")}</div>
              <div class="hr-target-player-meta">${escapeHtml(row.team || "")} vs ${escapeHtml(row.opponent || "")} · ${escapeHtml(row.opponent_pitcher_name || "TBD")}</div>
            </div>
          </div>
          <div class="hr-target-rank-block">
            <span class="cards-nav-pill">#${escapeHtml(row.rank || row.game_rank || "-")}</span>
            <span class="hr-target-support-badge ${badgeClass(row.hr_support_label)}">${escapeHtml(row.hr_support_label || "thin")}</span>
          </div>
        </div>
        <div class="hr-target-metrics">
          <div class="hr-target-metric">
            <span class="hr-target-metric-label">1+ HR</span>
            <strong>${escapeHtml(formatPercent(row.p_hr_1plus))}</strong>
          </div>
          <div class="hr-target-metric">
            <span class="hr-target-metric-label">Support</span>
            <strong>${escapeHtml(formatNumber(row.hr_support_score, 1))}</strong>
          </div>
          <div class="hr-target-metric">
            <span class="hr-target-metric-label">PA</span>
            <strong>${escapeHtml(formatNumber(row.pa_mean, 1))}</strong>
          </div>
          <div class="hr-target-metric">
            <span class="hr-target-metric-label">Lineup</span>
            <strong>${escapeHtml(row.lineup_order || row.lineup_status || "-")}</strong>
          </div>
        </div>
        <p class="hr-target-summary">${escapeHtml(row.hr_target_summary || "")}</p>
        ${reasons.length ? `<ul class="hr-target-reasons">${reasons.slice(0, 3).map((reason) => `<li>${escapeHtml(reason)}</li>`).join("")}</ul>` : ""}
      </article>
    `;
  }

  function gameSectionMarkup(game) {
    const rows = Array.isArray(game.rows) ? game.rows : [];
    return `
      <section class="hr-target-game-card">
        <div class="hr-target-game-head">
          <div>
            <div class="hr-targets-kicker">Game view</div>
            <h2>${escapeHtml(game.matchup || "Game")}</h2>
          </div>
          <div class="hr-target-game-meta">
            <span class="cards-nav-pill">${escapeHtml(rows.length)} targets</span>
          </div>
        </div>
        <div class="hr-target-game-grid">
          ${rows.map((row) => targetCardMarkup(row)).join("")}
        </div>
      </section>
    `;
  }

  function renderSummary(payload) {
    const counts = payload && payload.counts ? payload.counts : {};
    const policy = payload && payload.policy ? payload.policy : {};
    root.summary.innerHTML = `
      <div class="hr-targets-summary-grid">
        <div class="hr-targets-summary-card">
          <span class="hr-targets-summary-label">Visible targets</span>
          <strong>${escapeHtml(counts.filteredRows || 0)}</strong>
        </div>
        <div class="hr-targets-summary-card">
          <span class="hr-targets-summary-label">Games</span>
          <strong>${escapeHtml(counts.games || 0)}</strong>
        </div>
        <div class="hr-targets-summary-card">
          <span class="hr-targets-summary-label">Min HR prob</span>
          <strong>${escapeHtml(formatPercent(policy.min_prob))}</strong>
        </div>
        <div class="hr-targets-summary-card">
          <span class="hr-targets-summary-label">Min support</span>
          <strong>${escapeHtml(formatNumber(policy.min_support_score, 1))}</strong>
        </div>
      </div>
    `;
  }

  function renderGrid(payload) {
    const games = Array.isArray(payload.games) ? payload.games : [];
    if (!games.length) {
      root.grid.innerHTML = `<div class="cards-loading-state">${escapeHtml(payload.error || "No HR targets were available for this slate.")}</div>`;
      return;
    }
    root.grid.innerHTML = games.map((game) => gameSectionMarkup(game)).join("");
  }

  function updateChrome(payload) {
    if (root.headerMeta) {
      const sourcePath = payload && payload.sourcePath ? `Source: ${payload.sourcePath}` : "HR targets artifact unavailable";
      root.headerMeta.textContent = payload && payload.found
        ? `${payload.counts.filteredRows || 0} visible HR targets across ${payload.counts.games || 0} games.`
        : "No HR targets found for this slate.";
      if (root.sourceMeta) {
        root.sourceMeta.textContent = sourcePath;
      }
    }
    if (root.dateBadge) {
      root.dateBadge.textContent = state.date || "-";
    }
    if (root.prevLink) {
      root.prevLink.href = pageHref(shiftDate(state.date, -1), state.game, state.team, state.hitter, state.sort);
    }
    if (root.nextLink) {
      root.nextLink.href = pageHref(shiftDate(state.date, 1), state.game, state.team, state.hitter, state.sort);
    }
  }

  function applyPayload(payload) {
    renderOptions(root.gameInput, Array.isArray(payload.gameOptions) ? payload.gameOptions : [], String(payload.selectedGame || state.game || ""), "All games");
    renderOptions(root.teamInput, Array.isArray(payload.teamOptions) ? payload.teamOptions : [], String(payload.selectedTeam || state.team || ""), "All teams");
    renderOptions(root.hitterInput, Array.isArray(payload.hitterOptions) ? payload.hitterOptions : [], String(payload.selectedHitter || state.hitter || ""), "All hitters");
    if (root.sortInput) {
      root.sortInput.value = String(payload.selectedSort || state.sort || "score");
    }
    renderSummary(payload);
    renderGrid(payload);
    updateChrome(payload);
  }

  async function loadPayload() {
    root.grid.innerHTML = '<div class="cards-loading-state">Loading HR target board...</div>';
    try {
      const response = await fetch(apiHref(state.date, state.game, state.team, state.hitter, state.sort), { headers: { Accept: "application/json" } });
      const payload = await response.json();
      applyPayload(payload || {});
    } catch (error) {
      root.summary.innerHTML = '<div class="cards-loading-state">Failed to load HR targets.</div>';
      root.grid.innerHTML = `<div class="cards-loading-state">${escapeHtml(error && error.message ? error.message : 'Failed to load HR targets.')}</div>`;
    }
  }

  if (root.form) {
    root.form.addEventListener("submit", function () {
      if (root.dateInput) state.date = String(root.dateInput.value || state.date || "");
      if (root.gameInput) state.game = String(root.gameInput.value || "");
      if (root.teamInput) state.team = String(root.teamInput.value || "");
      if (root.hitterInput) state.hitter = String(root.hitterInput.value || "");
      if (root.sortInput) state.sort = String(root.sortInput.value || "score");
    });
  }

  loadPayload();
})();