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

  function hrSupportToneClass(label) {
    const normalized = String(label || "").toLowerCase();
    if (normalized === "strong") return "is-success";
    if (normalized === "solid") return "is-soft";
    if (normalized === "watch") return "is-warning";
    return "";
  }

  function hrTargetInitials(name) {
    const parts = String(name || "")
      .trim()
      .split(/\s+/)
      .filter(Boolean)
      .slice(0, 2);
    if (!parts.length) return "?";
    return parts.map((part) => part[0]).join("").toUpperCase();
  }

  function hrTargetDriverToneClass(driver) {
    const delta = Number(driver && driver.delta);
    if (!Number.isFinite(delta)) return "";
    if (delta >= 0.08) return "is-up-strong";
    if (delta > 0) return "is-up";
    return "is-down";
  }

  function hrTargetDriverMarkup(row) {
    const drivers = Array.isArray(row && row.drivers) ? row.drivers : [];
    if (!drivers.length) return "";
    return `
      <div class="cards-hr-target-driver-grid">
        ${drivers.map((driver) => `
          <div class="cards-hr-target-driver ${hrTargetDriverToneClass(driver)}">
            <span class="cards-hr-target-driver-label">${escapeHtml(String(driver.label || "Driver"))}</span>
            <strong>${escapeHtml(String(driver.display || "-"))}</strong>
          </div>`).join("")}
      </div>`;
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

  function targetCardMarkup(row) {
    return `
      <article class="cards-hr-target-chip hr-target-page-chip">
        <div class="cards-hr-target-chip-top">
          <span class="cards-hr-target-rank">#${escapeHtml(row.rank || row.game_rank || "-")}</span>
          <span class="cards-source-meta-pill ${hrSupportToneClass(row.supportLabel)}">${escapeHtml(String(row.supportLabel || "watch"))}</span>
        </div>
        <div class="cards-hr-target-identity">
          <div class="cards-hr-target-identity-main">
            ${row.headshotUrl
              ? `<img class="cards-hr-target-headshot" src="${escapeHtml(String(row.headshotUrl))}" alt="${escapeHtml(String(row.playerName || "Player"))}" loading="lazy" />`
              : `<div class="cards-hr-target-headshot cards-hr-target-headshot-fallback">${escapeHtml(hrTargetInitials(row.playerName))}</div>`}
            <div class="cards-hr-target-name-block">
              <div class="cards-hr-target-name">${escapeHtml(String(row.playerName || "Unknown"))}</div>
              <div class="cards-hr-target-meta">
                ${row.teamLogoUrl ? `<img class="cards-hr-target-team-logo" src="${escapeHtml(String(row.teamLogoUrl))}" alt="${escapeHtml(String(row.team || "Team"))}" loading="lazy" />` : ""}
                <span>${escapeHtml(String(row.team || ""))}</span>
                ${row.opponent ? `<span>vs ${escapeHtml(String(row.opponent))}</span>` : ""}
              </div>
            </div>
          </div>
          <div class="cards-hr-target-prob">
            ${escapeHtml(formatPercent(row.pHr1Plus))}
            <span class="cards-hr-target-prob-label">1+ HR</span>
          </div>
        </div>
        <div class="cards-hr-target-context-row">
          <div class="cards-hr-target-context">${escapeHtml(row.opponentPitcherName ? `vs ${String(row.opponentPitcherName)}` : String(row.matchup || ""))}</div>
          ${row.opponentLogoUrl ? `<img class="cards-hr-target-opponent-logo" src="${escapeHtml(String(row.opponentLogoUrl))}" alt="${escapeHtml(String(row.opponent || "Opponent"))}" loading="lazy" />` : ""}
        </div>
        <div class="cards-hr-target-metrics">
          <div class="cards-hr-target-pill">
            <strong>${escapeHtml(formatNumber(row.supportScore, 1))}</strong>
            <span>support</span>
          </div>
          <div class="cards-hr-target-pill">
            <strong>${escapeHtml(formatNumber(row.paMean, 1))}</strong>
            <span>PA</span>
          </div>
          <div class="cards-hr-target-pill">
            <strong>${escapeHtml(row.lineupOrder == null ? "-" : String(row.lineupOrder))}</strong>
            <span>lineup spot</span>
          </div>
        </div>
        ${hrTargetDriverMarkup(row)}
        <div class="cards-hr-target-summary">${escapeHtml(String(row.writeup || row.summary || row.matchup || ""))}</div>
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
      root.grid.innerHTML = `<div class="hr-targets-empty-state">${escapeHtml(payload.error || "No HR targets were available for this slate.")}</div>`;
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
      root.grid.innerHTML = `<div class="cards-loading-state">${escapeHtml(error && error.message ? error.message : "Failed to load HR targets.")}</div>`;
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