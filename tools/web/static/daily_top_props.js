(function () {
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

  function attachFilters(statSelect, gameSelect) {
    const sections = Array.from(document.querySelectorAll('[data-stat-section]'));
    if (!sections.length) {
      return;
    }

    const defaultStatValue = String(statSelect.getAttribute('data-default-stat') || statSelect.value || '');
    if (defaultStatValue) {
      statSelect.value = defaultStatValue;
    }
    if (gameSelect) {
      const defaultGameValue = String(gameSelect.getAttribute('data-default-game') || gameSelect.value || '');
      gameSelect.value = defaultGameValue;
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
          if (!row.hidden) {
            visibleRows += 1;
          }
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

  document.querySelectorAll('[data-sortable-table]').forEach(attachTableSorting);
  const statFilter = document.getElementById('topPropsStatFilter');
  const gameFilter = document.getElementById('topPropsGameFilter');
  if (statFilter) {
    attachFilters(statFilter, gameFilter);
  }
})();