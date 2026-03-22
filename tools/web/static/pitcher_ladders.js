(function () {
  let bootstrap = {};
  try {
    const bootstrapEl = document.getElementById("pitcherLaddersBootstrap");
    bootstrap = bootstrapEl ? JSON.parse(bootstrapEl.textContent || "{}") : {};
  } catch (_error) {
    bootstrap = {};
  }

  const state = {
    date: String(bootstrap.date || ""),
    prop: String(bootstrap.prop || "strikeouts"),
    pitcher: String(bootstrap.pitcher || ""),
    sort: String(bootstrap.sort || "team"),
  };

  const formEl = document.getElementById("ladderForm");
  const dateInputEl = document.getElementById("ladderDateInput");
  const propInputEl = document.getElementById("ladderPropInput");
  const pitcherInputEl = document.getElementById("ladderPitcherInput");
  const sortInputEl = document.getElementById("ladderSortInput");
  const headerMetaEl = document.getElementById("ladderHeaderMeta");
  const sourceMetaEl = document.getElementById("ladderSourceMeta");
  const summaryEl = document.getElementById("ladderSummary");
  const selectedPitcherEl = document.getElementById("ladderSelectedPitcher");
  const gridEl = document.getElementById("ladderGrid");
  const dateBadgeEl = document.getElementById("ladderDateBadge");
  const propBadgeEl = document.getElementById("ladderPropBadge");
  const prevDateLinkEl = document.getElementById("ladderPrevDateLink");
  const nextDateLinkEl = document.getElementById("ladderNextDateLink");

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

  function pageHref(dateValue, propValue, pitcherValue, sortValue) {
    const params = new URLSearchParams();
    if (dateValue) params.set("date", dateValue);
    if (propValue) params.set("prop", propValue);
    if (pitcherValue) params.set("pitcher", pitcherValue);
    if (sortValue) params.set("sort", sortValue);
    return `/pitcher-ladders?${params.toString()}`;
  }

  function apiHref(dateValue, propValue, pitcherValue, sortValue) {
    const params = new URLSearchParams();
    if (dateValue) params.set("date", dateValue);
    if (propValue) params.set("prop", propValue);
    if (pitcherValue) params.set("pitcher", pitcherValue);
    if (sortValue) params.set("sort", sortValue);
    return `/api/pitcher-ladders?${params.toString()}`;
  }

  function renderPitcherSelector(payload) {
    const options = Array.isArray(payload.pitcherOptions) ? payload.pitcherOptions : [];
    const currentValue = String(payload.selectedPitcher || state.pitcher || "");
    pitcherInputEl.innerHTML = [
      '<option value="">All starters</option>',
      ...options.map((option) => {
        const value = String(option.value || "");
        const selected = value === currentValue ? ' selected' : '';
        return `<option value="${escapeHtml(value)}"${selected}>${escapeHtml(option.label || value)}</option>`;
      }),
    ].join("");
    pitcherInputEl.value = currentValue;
  }

  function renderSelectedPitcher(payload) {
    const options = Array.isArray(payload.pitcherOptions) ? payload.pitcherOptions : [];
    const currentValue = String(payload.selectedPitcher || state.pitcher || "");
    if (!currentValue) {
      selectedPitcherEl.innerHTML = "";
      selectedPitcherEl.style.display = "none";
      return;
    }

    const selected = options.find((option) => String(option.value || "") === currentValue)
      || (Array.isArray(payload.rows) ? payload.rows.find((row) => String(row.pitcherId || "") === currentValue) : null);

    if (!selected) {
      selectedPitcherEl.innerHTML = "";
      selectedPitcherEl.style.display = "none";
      return;
    }

    const headshotUrl = selected.headshotUrl || "";
    const teamLogoUrl = selected.teamLogoUrl || "";
    const pitcherName = selected.pitcherName || selected.label || currentValue;
    const label = selected.label || pitcherName;
    const clearHref = pageHref(state.date, state.prop, "", state.sort);

    selectedPitcherEl.style.display = "block";
    selectedPitcherEl.innerHTML = `
      <div class="ladder-selected-card">
        <div class="ladder-selected-identity">
          ${teamLogoUrl ? `<img class="ladder-selected-team-logo" src="${escapeHtml(teamLogoUrl)}" alt="team logo" loading="lazy" />` : ""}
          ${headshotUrl ? `<img class="ladder-selected-headshot" src="${escapeHtml(headshotUrl)}" alt="${escapeHtml(pitcherName)} headshot" loading="lazy" />` : ""}
          <div>
            <div class="ladder-selected-kicker">Selected pitcher</div>
            <div class="ladder-selected-name">${escapeHtml(pitcherName)}</div>
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
        <div class="ladder-stat-label">Starters</div>
        <div class="ladder-stat-value">${formatCount(summary.starters)}</div>
      </article>
      <article class="ladder-stat">
        <div class="ladder-stat-label">Available starters</div>
        <div class="ladder-stat-value">${formatCount(summary.availableStarters)}</div>
      </article>
      <article class="ladder-stat">
        <div class="ladder-stat-label">Sim counts seen</div>
        <div class="ladder-stat-value">${escapeHtml(simCounts.length ? simCounts.join(", ") : "-")}</div>
      </article>
    `;
  }

  function renderEmpty(payload) {
    const detail = payload && payload.error ? ` (${escapeHtml(payload.error)})` : "";
    gridEl.innerHTML = `<div class="ladder-empty">No pitcher ladder data found for this date and prop${detail}.</div>`;
  }

  function renderCard(row, payload) {
    const ladderRows = Array.isArray(row.ladder) ? row.ladder : [];
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
      ? `<img class="ladder-player-headshot" src="${escapeHtml(row.headshotUrl)}" alt="${escapeHtml(row.pitcherName || 'Pitcher')} headshot" loading="lazy" />`
      : `<div class="ladder-player-headshot ladder-player-headshot-fallback">${escapeHtml(String((row.pitcherName || "?").slice(0, 1) || "?"))}</div>`;
    const ladderTableRows = ladderRows.map((ladderRow) => `
      <tr>
        <td>${escapeHtml(formatCount(ladderRow.total))}</td>
        <td>${escapeHtml(formatCount(ladderRow.hitCount))}</td>
        <td>${escapeHtml(formatPercent(ladderRow.hitProb))}</td>
        <td>${escapeHtml(formatCount(ladderRow.exactCount))}</td>
        <td>${escapeHtml(formatPercent(ladderRow.exactProb))}</td>
      </tr>
    `).join("");

    return `
      <article class="ladder-card">
        <div class="ladder-card-head">
          <div class="ladder-card-identity">
            ${headshot}
            <div>
              <h2 class="ladder-card-title">${escapeHtml(row.pitcherName || "Unknown starter")}</h2>
              <div class="ladder-card-subtitle">${escapeHtml(row.matchup || `${row.team || ""} vs ${row.opponent || ""}`)}</div>
            </div>
          </div>
          <div class="ladder-card-actions">
            ${gameHref ? `<a class="ladder-card-link" href="${gameHref}">Game view</a>` : ""}
            ${teamLogo}
          </div>
        </div>
        <div class="ladder-pills">
          <span class="ladder-pill"><span>Mean</span><strong>${escapeHtml(formatNumber(row.mean, 2))}</strong></span>
          <span class="ladder-pill"><span>Mode</span><strong>${escapeHtml(formatCount(row.mode))}</strong><span>${escapeHtml(formatPercent(row.modeProb))}</span></span>
          ${row.marketLine == null ? "" : `<span class="ladder-pill"><span>Market line</span><strong>${escapeHtml(formatNumber(row.marketLine, 1))}</strong></span>`}
          ${overLineText}
        </div>
        <div class="ladder-table-wrap">
          <table class="ladder-table">
            <thead>
              <tr>
                <th>Total</th>
                <th>&ge; Total</th>
                <th>Hit %</th>
                <th>Exact</th>
                <th>Exact %</th>
              </tr>
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
    renderPitcherSelector(payload);
    renderSelectedPitcher(payload);

    const summary = payload.summary || {};
    const simCounts = Array.isArray(summary.simCounts) ? summary.simCounts : [];
    const sortLabel = String((Array.isArray(payload.sortOptions) ? payload.sortOptions : []).find((option) => String(option.value || "") === String(payload.selectedSort || state.sort || "team"))?.label || (payload.selectedSort || state.sort || "team"));
    headerMetaEl.textContent = payload.found
      ? `${summary.starters || 0} starters across ${summary.games || 0} games. Sorted by ${sortLabel}. Sim counts: ${simCounts.length ? simCounts.join(", ") : "-"}.${state.pitcher ? ` Filtered to pitcher ${state.pitcher}.` : ""}`
      : "No ladder data found for this selection.";

    sourceMetaEl.textContent = `Sim dir: ${payload.sourceDir || "-"} | Market file: ${payload.marketSource || "-"} | Default daily sims: ${payload.defaultSims || "-"}`;

    const nav = payload.nav || {};
    prevDateLinkEl.href = pageHref(nav.prevDate || state.date, state.prop, state.pitcher, state.sort);
    nextDateLinkEl.href = pageHref(nav.nextDate || state.date, state.prop, state.pitcher, state.sort);
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
    pitcherInputEl.value = state.pitcher;
    if (sortInputEl) sortInputEl.value = state.sort;
    gridEl.innerHTML = '<div class="cards-loading-state">Loading pitcher ladders...</div>';
    summaryEl.innerHTML = '<div class="cards-loading-state">Loading ladder summary...</div>';

    const response = await fetch(apiHref(state.date, state.prop, state.pitcher, state.sort));
    const payload = await response.json();
    renderPayload(payload);
  }

  formEl.addEventListener("submit", async (event) => {
    event.preventDefault();
    state.date = String(dateInputEl.value || "");
    state.prop = String(propInputEl.value || "strikeouts");
    state.pitcher = String(pitcherInputEl.value || "");
    state.sort = String((sortInputEl && sortInputEl.value) || "team");
    window.history.replaceState({}, "", pageHref(state.date, state.prop, state.pitcher, state.sort));
    await loadPayload();
  });

  propInputEl.addEventListener("change", () => {
    if (formEl.requestSubmit) {
      formEl.requestSubmit();
      return;
    }
    formEl.dispatchEvent(new Event("submit", { cancelable: true }));
  });

  pitcherInputEl.addEventListener("change", () => {
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
    headerMetaEl.textContent = "Failed to load pitcher ladders.";
    sourceMetaEl.textContent = String(error && error.message ? error.message : error || "unknown error");
    summaryEl.innerHTML = '<div class="ladder-empty">Pitcher ladder data could not be loaded.</div>';
    selectedPitcherEl.innerHTML = "";
    gridEl.innerHTML = "";
  });
})();