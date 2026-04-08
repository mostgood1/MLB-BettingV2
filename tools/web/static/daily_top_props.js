(function () {
  const bootstrap = window.MLBDailyTopPropsBootstrap || {};
  const state = {
    group: String(bootstrap.group || 'pitcher'),
    groupLabel: String(bootstrap.groupLabel || 'Player'),
    title: String(bootstrap.title || 'Top Props'),
    date: String(bootstrap.date || ''),
    season: Number.parseInt(bootstrap.season, 10) || null,
    payload: null,
  };

  const root = {
    datePill: document.getElementById('topPropsDatePill'),
    headerMeta: document.getElementById('topPropsHeaderMeta'),
    sourceMeta: document.getElementById('topPropsSourceMeta'),
    prevLink: document.getElementById('topPropsPrevLink'),
    nextLink: document.getElementById('topPropsNextLink'),
    statFilter: document.getElementById('topPropsStatFilter'),
    gameFilter: document.getElementById('topPropsGameFilter'),
    sections: document.getElementById('topPropsSections'),
    dateInput: document.getElementById('topPropsDateInput'),
  };

  function escapeHtml(value) {
    return String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function formatPercent(value, digits) {
    const num = Number(value);
    if (!Number.isFinite(num)) return '-';
    return `${(num * 100).toFixed(digits ?? 1)}%`;
  }

  function formatSignedPercent(value, digits) {
    const num = Number(value);
    if (!Number.isFinite(num)) return '-';
    const scaled = (num * 100).toFixed(digits ?? 1);
    return `${num >= 0 ? '+' : ''}${scaled}%`;
  }

  function formatNumber(value, digits) {
    const num = Number(value);
    if (!Number.isFinite(num)) return '-';
    return num.toFixed(digits ?? 0);
  }

  function parseValue(rawValue, type) {
    const text = String(rawValue ?? '').trim();
    if (type === 'number') {
      const numeric = Number.parseFloat(text);
      return Number.isFinite(numeric) ? numeric : Number.NEGATIVE_INFINITY;
    }
    return text.toLowerCase();
  }

  function cellValue(row, columnIndex, type) {
    const cell = row.children[columnIndex];
    if (!cell) {
      return parseValue('', type);
    }
    return parseValue(cell.getAttribute('data-sort-value') ?? cell.textContent, type);
  }

  function attachTableSorting(table) {
    const headerButtons = Array.from(table.querySelectorAll('.top-props-sort-button'));
    const tbody = table.tBodies[0];
    if (!tbody || !headerButtons.length) {
      return;
    }

    headerButtons.forEach((button, columnIndex) => {
      button.addEventListener('click', function () {
        const type = button.getAttribute('data-sort-type') || 'text';
        const currentDirection = button.getAttribute('data-sort-direction') === 'asc' ? 'asc' : 'desc';
        const nextDirection = currentDirection === 'asc' ? 'desc' : 'asc';
        headerButtons.forEach((other) => {
          if (other !== button) {
            other.removeAttribute('data-sort-direction');
          }
        });
        button.setAttribute('data-sort-direction', nextDirection);

        const rows = Array.from(tbody.querySelectorAll('tr'));
        rows.sort((leftRow, rightRow) => {
          const leftValue = cellValue(leftRow, columnIndex, type);
          const rightValue = cellValue(rightRow, columnIndex, type);
          if (leftValue < rightValue) {
            return nextDirection === 'asc' ? -1 : 1;
          }
          if (leftValue > rightValue) {
            return nextDirection === 'asc' ? 1 : -1;
          }
          return 0;
        });

        rows.forEach((row) => tbody.appendChild(row));
      });
    });
  }

  function attachFilters() {
    const statSelect = root.statFilter;
    const gameSelect = root.gameFilter;
    const sections = Array.from(document.querySelectorAll('[data-stat-section]'));
    if (!statSelect || !sections.length) {
      return;
    }

    function applyFilter() {
      const selectedStat = String(statSelect.value || '');
      const selectedGame = gameSelect ? String(gameSelect.value || '') : '';
      sections.forEach((section) => {
        const sectionValue = String(section.getAttribute('data-stat-section') || '');
        const statHidden = !!selectedStat && sectionValue !== selectedStat;
        const rows = Array.from(section.querySelectorAll('tbody tr'));
        let visibleRows = 0;
        rows.forEach((row) => {
          const rowGame = String(row.getAttribute('data-game-pk') || '');
          const gameHidden = !!selectedGame && rowGame !== selectedGame;
          row.hidden = statHidden || gameHidden;
          if (!row.hidden) visibleRows += 1;
        });
        section.hidden = statHidden || (!statHidden && visibleRows <= 0);
      });
    }

    statSelect.addEventListener('change', applyFilter);
    if (gameSelect) {
      gameSelect.addEventListener('change', applyFilter);
    }
    applyFilter();
  }

  function resultBadge(row) {
    const reconciliation = row?.reconciliation || {};
    const status = String(reconciliation.status || 'unavailable');
    const label = String(reconciliation.label || status);
    return `<span class="top-props-result-badge top-props-result-${escapeHtml(status)}">${escapeHtml(label)}</span>`;
  }

  function rowMarkup(row, reconciliationEnabled) {
    const rawEdge = Number(row?.rawEdge);
    const edgeClass = Number.isFinite(rawEdge)
      ? (rawEdge > 0 ? 'top-props-edge-positive' : (rawEdge < 0 ? 'top-props-edge-negative' : ''))
      : '';
    const actualValue = row?.actual;
    return `
      <tr data-game-pk="${escapeHtml(row?.gamePk || '')}">
        <td class="top-props-rank" data-label="Rank" data-sort-value="${escapeHtml(row?.rank || '')}">${escapeHtml(row?.rank || '')}</td>
        <td data-label="${escapeHtml(state.groupLabel)}" data-sort-value="${escapeHtml(row?.playerName || '')}">
          <div class="top-props-player">${escapeHtml(row?.playerName || '')}</div>
          <div class="top-props-player-meta">${escapeHtml(row?.team || '')}${row?.opponent ? ` vs ${escapeHtml(row.opponent)}` : ''}</div>
        </td>
        <td data-label="Matchup" data-sort-value="${escapeHtml(row?.matchup || '')}">${escapeHtml(row?.matchup || '-')}</td>
        <td data-label="Target" data-sort-value="${escapeHtml(row?.targetLabel || '')}">${escapeHtml(row?.targetLabel || '-')}</td>
        <td data-label="Side" data-sort-value="${escapeHtml(row?.selectionLabel || '')}">${escapeHtml(row?.selectionLabel || '-')}</td>
        ${reconciliationEnabled ? `<td data-label="Actual" data-sort-value="${escapeHtml(actualValue ?? '')}">${actualValue != null ? escapeHtml(formatNumber(actualValue, 0)) : '-'}</td>` : ''}
        ${reconciliationEnabled ? `<td data-label="Result" data-sort-value="${escapeHtml((row?.reconciliation || {}).label || '')}">${resultBadge(row)}</td>` : ''}
        <td data-label="Sim %" data-sort-value="${escapeHtml(row?.simProb ?? '')}">${escapeHtml(formatPercent(row?.simProb, 1))}</td>
        <td data-label="Market %" data-sort-value="${escapeHtml(row?.marketProb ?? '')}">${escapeHtml(formatPercent(row?.marketProb, 1))}</td>
        <td data-label="Raw edge" data-sort-value="${escapeHtml(row?.rawEdge ?? '')}" class="${edgeClass}">${escapeHtml(formatSignedPercent(row?.rawEdge, 1))}</td>
      </tr>`;
  }

  function sectionMarkup(section, reconciliationEnabled) {
    const rows = Array.isArray(section?.rows) ? section.rows : [];
    const metaBits = [
      `<span class="cards-nav-pill">${escapeHtml(section?.candidateCount ?? 0)} priced</span>`,
      `<span class="cards-nav-pill">${escapeHtml(section?.positiveEdgeCount ?? 0)} positive</span>`,
    ];
    if (reconciliationEnabled && section?.reconciliation?.resultCounts) {
      const counts = section.reconciliation.resultCounts;
      metaBits.push(`<span class="cards-nav-pill">${escapeHtml(counts.win ?? 0)}-${escapeHtml(counts.loss ?? 0)}-${escapeHtml(counts.push ?? 0)}</span>`);
    }
    const columns = reconciliationEnabled
      ? `
          <th><button type="button" class="top-props-sort-button" data-sort-type="number">Actual</button></th>
          <th><button type="button" class="top-props-sort-button" data-sort-type="text">Result</button></th>`
      : '';
    const body = rows.length
      ? `
        <div class="top-props-table-wrap">
          <table class="top-props-table" data-sortable-table>
            <thead>
              <tr>
                <th><button type="button" class="top-props-sort-button" data-sort-type="number">Rank</button></th>
                <th><button type="button" class="top-props-sort-button" data-sort-type="text">${escapeHtml(state.groupLabel)}</button></th>
                <th><button type="button" class="top-props-sort-button" data-sort-type="text">Matchup</button></th>
                <th><button type="button" class="top-props-sort-button" data-sort-type="text">Target</button></th>
                <th><button type="button" class="top-props-sort-button" data-sort-type="text">Side</button></th>${columns}
                <th><button type="button" class="top-props-sort-button" data-sort-type="number">Sim %</button></th>
                <th><button type="button" class="top-props-sort-button" data-sort-type="number">Market %</button></th>
                <th><button type="button" class="top-props-sort-button" data-sort-type="number">Raw edge</button></th>
              </tr>
            </thead>
            <tbody>${rows.map((row) => rowMarkup(row, reconciliationEnabled)).join('')}</tbody>
          </table>
        </div>`
      : `<div class="cards-loading-state">${escapeHtml(section?.error || `No priced rows were available for ${String(section?.label || 'this section').toLowerCase()}.`)}</div>`;
    return `
      <section class="top-props-card" data-stat-section="${escapeHtml(section?.stat || '')}" aria-labelledby="top-props-${escapeHtml(section?.stat || '')}">
        <div class="top-props-card-head">
          <div>
            <div class="top-props-card-kicker">${escapeHtml(state.groupLabel)} stat</div>
            <h2 id="top-props-${escapeHtml(section?.stat || '')}">${escapeHtml(section?.label || '')}</h2>
          </div>
          <div class="top-props-card-meta">${metaBits.join('')}</div>
        </div>
        ${body}
      </section>`;
  }

  function populateFilters(payload) {
    if (root.statFilter) {
      const sections = Array.isArray(payload?.sections) ? payload.sections : [];
      const defaultStat = String(payload?.defaultStat || (sections[0]?.stat || ''));
      root.statFilter.innerHTML = sections.map((section) => `
        <option value="${escapeHtml(section?.stat || '')}"${String(section?.stat || '') === defaultStat ? ' selected' : ''}>${escapeHtml(section?.label || section?.stat || '')}</option>`).join('');
      root.statFilter.disabled = sections.length <= 1;
    }
    if (root.gameFilter) {
      const options = Array.isArray(payload?.gameOptions) ? payload.gameOptions : [];
      const defaultGame = String(payload?.defaultGame || '');
      root.gameFilter.innerHTML = ['<option value="">All games</option>'].concat(options.map((option) => `
        <option value="${escapeHtml(option?.value || option?.gamePk || '')}"${String(option?.value || option?.gamePk || '') === defaultGame ? ' selected' : ''}>${escapeHtml(option?.label || option?.value || '')}</option>`)).join('');
      root.gameFilter.disabled = false;
    }
  }

  function updateNav(payload) {
    const nav = payload?.nav || {};
    if (root.prevLink) root.prevLink.href = `/${state.group}-top-props?date=${encodeURIComponent(String(nav.prevDate || state.date))}`;
    if (root.nextLink) root.nextLink.href = `/${state.group}-top-props?date=${encodeURIComponent(String(nav.nextDate || state.date))}`;
    if (root.datePill) root.datePill.textContent = String(payload?.date || state.date || '');
    if (root.dateInput && payload?.date) root.dateInput.value = String(payload.date);
  }

  function renderHeader(payload) {
    if (root.headerMeta) {
      const summary = payload?.summary || {};
      const reconciliation = payload?.reconciliation || {};
      const parts = [];
      if (payload?.found) {
        parts.push(`${summary.displayedCount ?? 0} rows shown across ${summary.sectionCount ?? 0} tracked stats.`);
        parts.push(`${summary.positiveEdgeCount ?? 0} candidates are currently above market.`);
        if (reconciliation.enabled) {
          const counts = reconciliation.resultCounts || {};
          parts.push(`Settled: ${reconciliation.settledCount ?? 0}. Wins ${counts.win ?? 0}, losses ${counts.loss ?? 0}, pushes ${counts.push ?? 0}.`);
        }
      } else {
        parts.push('No top-props board is available for this slate yet.');
      }
      root.headerMeta.textContent = parts.join(' ');
    }
    if (root.sourceMeta) {
      const bits = [];
      if (payload?.marketMode) bits.push(`Market mode: ${payload.marketMode}`);
      if (payload?.marketSource) bits.push(`Source: ${payload.marketSource}`);
      root.sourceMeta.textContent = bits.join('  ');
    }
  }

  function renderSections(payload) {
    if (!root.sections) return;
    const sections = Array.isArray(payload?.sections) ? payload.sections : [];
    const reconciliationEnabled = Boolean(payload?.reconciliation?.enabled);
    if (!sections.length) {
      root.sections.innerHTML = '<div class="season-empty-copy">No top-props board is available for this slate yet.</div>';
      return;
    }
    root.sections.innerHTML = sections.map((section) => sectionMarkup(section, reconciliationEnabled)).join('');
    root.sections.querySelectorAll('[data-sortable-table]').forEach(attachTableSorting);
    attachFilters();
  }

  async function fetchJson(url) {
    const response = await fetch(url, { headers: { 'Accept': 'application/json' } });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      const message = payload?.error || payload?.message || `Request failed (${response.status})`;
      throw new Error(message);
    }
    return payload;
  }

  async function loadPage() {
    try {
      const payload = await fetchJson(`/api/${encodeURIComponent(state.group)}-top-props?date=${encodeURIComponent(state.date)}`);
      state.payload = payload;
      state.date = String(payload?.date || state.date || '');
      state.season = Number.parseInt(payload?.season, 10) || state.season;
      updateNav(payload);
      renderHeader(payload);
      populateFilters(payload);
      renderSections(payload);
    } catch (error) {
      if (root.headerMeta) root.headerMeta.textContent = String(error?.message || 'Failed to load top-props board.');
      if (root.sections) root.sections.innerHTML = `<div class="season-empty-copy">Failed to load top-props board. ${escapeHtml(error?.message || '')}</div>`;
    }
  }

  loadPage();
})();