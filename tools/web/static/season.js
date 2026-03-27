(function () {
  const bootstrap = window.MLBSeasonBootstrap || {};
  const state = {
    season: Number(bootstrap.season || 0),
    selectedDate: String(bootstrap.date || ""),
    monthFilter: "all",
    manifest: null,
    bettingCards: {
      baseline: null,
      retuned: null,
    },
    bettingProfile: "retuned",
    day: null,
    liveLens: null,
    dayPicksMode: "props",
  };

  const BETTING_MARKET_ORDER = [
    "combined",
    "totals",
    "ml",
    "pitcher_props",
    "hitter_props",
    "hitter_home_runs",
    "hitter_hits",
    "hitter_total_bases",
    "hitter_runs",
    "hitter_rbis",
  ];

  const BETTING_MARKET_LABELS = {
    combined: "Combined",
    totals: "Totals",
    ml: "Moneyline",
    pitcher_props: "Pitcher props",
    hitter_props: "Hitter props",
    hitter_home_runs: "Hitter HR",
    hitter_hits: "Hitter hits",
    hitter_total_bases: "Hitter total bases",
    hitter_runs: "Hitter runs",
    hitter_rbis: "Hitter RBIs",
  };

  const BETTING_PROFILE_LABELS = {
    baseline: "Baseline recap",
    retuned: "Retuned live recap",
  };

  const HITTER_PROP_ORDER = [
    "hits_1plus",
    "hits_2plus",
    "hits_3plus",
    "doubles_1plus",
    "triples_1plus",
    "runs_1plus",
    "runs_2plus",
    "runs_3plus",
    "rbi_1plus",
    "rbi_2plus",
    "rbi_3plus",
    "rbi_4plus",
    "total_bases_1plus",
    "total_bases_2plus",
    "total_bases_3plus",
    "total_bases_4plus",
    "total_bases_5plus",
    "sb_1plus",
  ];

  const HITTER_PROP_LABELS = {
    hits_1plus: "H 1+",
    hits_2plus: "H 2+",
    hits_3plus: "H 3+",
    doubles_1plus: "2B 1+",
    triples_1plus: "3B 1+",
    runs_1plus: "R 1+",
    runs_2plus: "R 2+",
    runs_3plus: "R 3+",
    rbi_1plus: "RBI 1+",
    rbi_2plus: "RBI 2+",
    rbi_3plus: "RBI 3+",
    rbi_4plus: "RBI 4+",
    total_bases_1plus: "TB 1+",
    total_bases_2plus: "TB 2+",
    total_bases_3plus: "TB 3+",
    total_bases_4plus: "TB 4+",
    total_bases_5plus: "TB 5+",
    sb_1plus: "SB 1+",
  };

  const root = {
    headerMeta: document.getElementById("seasonHeaderMeta"),
    statusPill: document.getElementById("seasonStatusPill"),
    summary: document.getElementById("seasonSummary"),
    hitterCalibration: document.getElementById("seasonHitterCalibration"),
    monthlyBreakdown: document.getElementById("seasonMonthlyBreakdown"),
    bettingCards: document.getElementById("seasonBettingCards"),
    months: document.getElementById("seasonMonths"),
    days: document.getElementById("seasonDays"),
    dayTitle: document.getElementById("seasonDayTitle"),
    dayMeta: document.getElementById("seasonDayMeta"),
    dayActions: document.getElementById("seasonDayActions"),
    dayMetrics: document.getElementById("seasonDayMetrics"),
    dayPicks: document.getElementById("seasonDayPicks"),
    liveLens: document.getElementById("seasonLiveLens"),
    games: document.getElementById("seasonGames"),
  };

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

  function formatPercent(value, digits) {
    const num = toNumber(value);
    if (num == null) return "-";
    return `${(num * 100).toFixed(digits == null ? 1 : digits)}%`;
  }

  function formatSigned(value, digits) {
    const num = toNumber(value);
    if (num == null) return "-";
    const fixed = num.toFixed(digits == null ? 1 : digits);
    return num > 0 ? `+${fixed}` : fixed;
  }

  function formatNumber(value, digits) {
    const num = toNumber(value);
    if (num == null) return "-";
    if (Number.isInteger(num) && (digits == null || digits <= 0)) return String(num);
    return num.toFixed(digits == null ? 2 : digits);
  }

  function formatSignedPercentPoints(value, digits) {
    const num = toNumber(value);
    if (num == null) return "-";
    const scaled = num * 100;
    const fixed = scaled.toFixed(digits == null ? 1 : digits);
    return scaled > 0 ? `+${fixed} pts` : `${fixed} pts`;
  }

  function formatUnits(value, digits) {
    const num = toNumber(value);
    if (num == null) return "-";
    const fixed = num.toFixed(digits == null ? 2 : digits);
    return num > 0 ? `+${fixed}u` : `${fixed}u`;
  }

  function formatLine(value) {
    const num = toNumber(value);
    if (num == null) return "-";
    return Number.isInteger(num) ? String(num) : num.toFixed(1);
  }

  function formatOdds(value) {
    const text = String(value == null ? "" : value).trim();
    if (!text) return "-";
    if (/^-?\d+$/.test(text)) {
      const num = Number(text);
      if (Number.isFinite(num) && num > 0) return `+${num}`;
      if (Number.isFinite(num)) return String(num);
    }
    return text;
  }

  function formatDateLong(dateStr) {
    const dt = new Date(`${dateStr}T12:00:00`);
    if (Number.isNaN(dt.getTime())) return dateStr;
    return dt.toLocaleDateString(undefined, { weekday: "short", month: "short", day: "numeric", year: "numeric" });
  }

  function monthLabel(monthKey) {
    const dt = new Date(`${monthKey}-01T12:00:00`);
    if (Number.isNaN(dt.getTime())) return monthKey;
    return dt.toLocaleDateString(undefined, { month: "short", year: "numeric" });
  }

  function fetchJson(url) {
    return fetch(url, { cache: "no-store" }).then((response) => {
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      return response.json();
    });
  }

  function updateUrl(dateStr) {
    const url = new URL(window.location.href);
    if (dateStr) url.searchParams.set("date", dateStr);
    else url.searchParams.delete("date");
    window.history.replaceState({}, "", url.toString());
  }

  function metricCard(label, value, subcopy) {
    return `
      <article class="season-metric-card">
        <div class="season-metric-label">${escapeHtml(label)}</div>
        <div class="season-metric-value">${escapeHtml(value)}</div>
        <div class="season-metric-sub">${escapeHtml(subcopy || "")}</div>
      </article>`;
  }

  function metricActionCard(label, value, subcopy, actionKey, options) {
    const isActive = Boolean(options?.isActive);
    const disabled = Boolean(options?.disabled);
    return `
      <button
        type="button"
        class="season-metric-card season-metric-action ${isActive ? "is-active" : ""}"
        data-day-picks-filter="${escapeHtml(actionKey)}"
        aria-pressed="${isActive ? "true" : "false"}"
        ${disabled ? "disabled" : ""}
      >
        <div class="season-metric-label">${escapeHtml(label)}</div>
        <div class="season-metric-value">${escapeHtml(value)}</div>
        <div class="season-metric-sub">${escapeHtml(subcopy || "")}</div>
      </button>`;
  }

  function filteredDays() {
    const allDays = Array.isArray(state.manifest?.days) ? state.manifest.days : [];
    if (state.monthFilter === "all") return allDays;
    return allDays.filter((row) => String(row?.month || "") === state.monthFilter);
  }

  function renderHeader() {
    if (!root.headerMeta) return;
    const meta = state.manifest?.meta || {};
    const progress = meta.progress || {};
    const overview = state.manifest?.overview || {};
    const parts = [];
    const bettingOverview = overview?.betting_cards || {};
    const bettingManifest = selectedBettingManifest();
    const fallbackBettingDays = Array.isArray(bettingManifest?.days) ? bettingManifest.days.filter((row) => Boolean(row?.card_path)).length : null;
    const bettingDaysByProfile = bettingOverview?.days_by_profile || {};
    const bettingDefaultProfile = String(bettingOverview?.default_profile || "");
    const bettingDays = bettingDaysByProfile?.[state.bettingProfile] ?? bettingDaysByProfile?.[bettingDefaultProfile] ?? bettingOverview?.days_any ?? fallbackBettingDays;
    const legacyCardsDays = overview?.legacy_cards_page_days ?? overview?.cards_available_days;
    if (meta.partial) {
      if (progress.completed_reports != null && progress.expected_reports != null) {
        parts.push(`Partial ${progress.completed_reports}/${progress.expected_reports}`);
      } else {
        parts.push("Partial season publish");
      }
    }
    if (overview.days != null) parts.push(`${overview.days} days`);
    if (overview.total_games != null) parts.push(`${overview.total_games} games`);
    if (bettingDays != null) {
      parts.push(`${bettingDays} season card days`);
      if (legacyCardsDays != null) {
        parts.push(`${legacyCardsDays} legacy cards-page days`);
      }
    } else if (legacyCardsDays != null) {
      parts.push(`${legacyCardsDays} legacy cards-page days`);
    }
    if (overview.first_date && overview.last_date) parts.push(`${overview.first_date} to ${overview.last_date}`);
    root.headerMeta.textContent = parts.join(" | ") || "Season manifest loaded.";
    if (root.statusPill) {
      root.statusPill.textContent = meta.partial ? "Partial" : "Complete";
      root.statusPill.classList.toggle("is-partial", Boolean(meta.partial));
    }
  }

  function renderSummary() {
    if (!root.summary) return;
    const meta = state.manifest?.meta || {};
    const progress = meta.progress || {};
    const overview = state.manifest?.overview || {};
    const recap = state.manifest?.recap || {};
    const fullGame = recap.full_game || {};
    const segments = recap.segments || {};
    const moneyline = fullGame.moneyline || {};
    const totals = fullGame.totals || {};
    const runline = fullGame.runline_fav_minus_1_5 || {};
    const starters = fullGame.pitcher_props_starters || {};
    const marketProps = fullGame.pitcher_props_at_market_lines || {};
    const soMarket = marketProps.strikeouts || {};

    const publishedValue = progress.expected_reports != null
      ? `${overview.days ?? 0}/${progress.expected_reports}`
      : String(overview.days ?? "-");
    const publishedSubcopy = meta.partial
      ? `Through ${overview.last_date ?? "-"}`
      : `${overview.first_date ?? "-"} to ${overview.last_date ?? "-"}`;

    root.summary.innerHTML = [
      metricCard("Days published", publishedValue, publishedSubcopy),
      metricCard("Games simulated", String(overview.total_games ?? "-"), `${overview.days ?? "-"} active dates`),
      metricCard("Moneyline brier", formatNumber(moneyline.brier, 3), `Accuracy ${formatPercent(moneyline.accuracy, 1)}`),
      metricCard("Totals mae", formatNumber(totals.mae, 2), `RMSE ${formatNumber(totals.rmse, 2)}`),
      metricCard("Runline accuracy", formatPercent(runline.accuracy, 1), `Brier ${formatNumber(runline.brier, 3)}`),
      metricCard("Starter SO mae", formatNumber(starters.so_mae, 2), `RMSE ${formatNumber(starters.so_rmse, 2)}`),
      metricCard("Starter outs mae", formatNumber(starters.outs_mae, 2), `RMSE ${formatNumber(starters.outs_rmse, 2)}`),
      metricCard("Market SO accuracy", formatPercent(soMarket.accuracy, 1), `Brier ${formatNumber(soMarket.brier, 3)}`),
      metricCard("Full-game total mae", formatNumber((segments.full || {}).mae_total_runs, 2), `First5 ${formatNumber((segments.first5 || {}).mae_total_runs, 2)} | First3 ${formatNumber((segments.first3 || {}).mae_total_runs, 2)}`),
    ].join("");
  }

  function hitterPropMetrics(props, key) {
    if (!props || typeof props !== "object") return null;
    const brier = toNumber(props[`${key}_brier`]);
    const logloss = toNumber(props[`${key}_logloss`]);
    const avgP = toNumber(props[`${key}_avg_p`]);
    const empRate = toNumber(props[`${key}_emp_rate`]);
    const n = toNumber(props[`${key}_brier_weight`] ?? props[`${key}_logloss_weight`] ?? props[`${key}_avg_p_weight`] ?? props[`${key}_emp_rate_weight`]);
    if ([brier, logloss, avgP, empRate, n].every((value) => value == null)) return null;
    return { brier, logloss, avgP, empRate, n };
  }

  function hitterGapClass(gap) {
    const num = toNumber(gap);
    if (num == null) return "";
    if (num > 0.01) return "is-over";
    if (num < -0.01) return "is-under";
    return "is-balanced";
  }

  function hitterCard(propKey, metrics) {
    const gap = (metrics?.avgP ?? null) != null && (metrics?.empRate ?? null) != null ? metrics.avgP - metrics.empRate : null;
    return `
      <article class="season-hitter-card">
        <div class="season-hitter-card-head">
          <div class="season-segment-title">${escapeHtml(HITTER_PROP_LABELS[propKey] || propKey)}</div>
          <span class="season-gap-pill ${hitterGapClass(gap)}">${escapeHtml(formatSignedPercentPoints(gap, 1))}</span>
        </div>
        <div class="season-starter-copy">Pred ${escapeHtml(formatPercent(metrics?.avgP, 1))} | Actual ${escapeHtml(formatPercent(metrics?.empRate, 1))}</div>
        <div class="season-stat-grid">
          <div class="season-stat-box">
            <div class="season-stat-label">Logloss</div>
            <div class="season-stat-value">${escapeHtml(formatNumber(metrics?.logloss, 3))}</div>
          </div>
          <div class="season-stat-box">
            <div class="season-stat-label">Brier</div>
            <div class="season-stat-value">${escapeHtml(formatNumber(metrics?.brier, 3))}</div>
          </div>
        </div>
      </article>`;
  }

  function renderHitterCalibration() {
    if (!root.hitterCalibration) return;
    const recap = state.manifest?.recap || {};
    const props = recap.hitter_props_likelihood_topn || {};
    const hr = recap.hitter_hr_likelihood_topn || {};
    const rows = HITTER_PROP_ORDER
      .map((propKey) => ({ key: propKey, metrics: hitterPropMetrics(props, propKey) }))
      .filter((entry) => entry.metrics && entry.metrics.n != null && entry.metrics.n > 0);

    if (!rows.length) {
      root.hitterCalibration.innerHTML = '<div class="season-empty-copy">No hitter calibration metrics are published for this season yet.</div>';
      return;
    }

    const heroKeys = ["hits_1plus", "runs_1plus", "rbi_1plus", "total_bases_1plus"]
      .map((propKey) => ({ key: propKey, metrics: hitterPropMetrics(props, propKey) }))
      .filter((entry) => entry.metrics);

    const hrText = toNumber(hr.hr_brier) != null
      ? `HR 1+ brier ${formatNumber(hr.hr_brier, 3)} | logloss ${formatNumber(hr.hr_logloss, 3)}`
      : "HR top-N scoring is not published in the current batch.";

    root.hitterCalibration.innerHTML = `
      <div class="season-panel-head">
        <div>
          <div class="season-kicker">Hitter props</div>
          <div class="season-panel-title">Calibration watch</div>
        </div>
        <div class="season-inline-note">Weighted top-list probabilities vs realized rates across published day reports.</div>
      </div>
      <div class="season-hitter-grid">
        ${heroKeys.map((entry) => hitterCard(entry.key, entry.metrics)).join("")}
      </div>
      <div class="season-inline-note season-hitter-note">${escapeHtml(hrText)}</div>
      <div class="season-calibration-table-wrap">
        <table class="season-calibration-table">
          <thead>
            <tr>
              <th>Prop</th>
              <th>Pred</th>
              <th>Actual</th>
              <th>Gap</th>
              <th>Logloss</th>
              <th>Brier</th>
              <th>N</th>
            </tr>
          </thead>
          <tbody>
            ${rows.map((entry) => {
              const gap = (entry.metrics.avgP ?? null) != null && (entry.metrics.empRate ?? null) != null ? entry.metrics.avgP - entry.metrics.empRate : null;
              return `
                <tr>
                  <td>${escapeHtml(HITTER_PROP_LABELS[entry.key] || entry.key)}</td>
                  <td>${escapeHtml(formatPercent(entry.metrics.avgP, 1))}</td>
                  <td>${escapeHtml(formatPercent(entry.metrics.empRate, 1))}</td>
                  <td><span class="season-gap-pill ${hitterGapClass(gap)}">${escapeHtml(formatSignedPercentPoints(gap, 1))}</span></td>
                  <td>${escapeHtml(formatNumber(entry.metrics.logloss, 3))}</td>
                  <td>${escapeHtml(formatNumber(entry.metrics.brier, 3))}</td>
                  <td>${escapeHtml(formatNumber(entry.metrics.n, 0))}</td>
                </tr>`;
            }).join("")}
          </tbody>
        </table>
      </div>`;
  }

  function renderMonthlyBreakdown() {
    if (!root.monthlyBreakdown) return;
    const months = Array.isArray(state.manifest?.months) ? state.manifest.months : [];
    if (!months.length) {
      root.monthlyBreakdown.innerHTML = '<div class="season-empty-copy">No monthly breakdown is available for this season manifest.</div>';
      return;
    }

    const marketRows = months.map((month) => {
      const fullGame = month?.full_game || {};
      const moneyline = fullGame.moneyline || {};
      const totals = fullGame.totals || {};
      const runline = fullGame.runline_fav_minus_1_5 || {};
      const starterProps = fullGame.pitcher_props_starters || {};
      const marketProps = fullGame.pitcher_props_at_market_lines || {};
      const soMarket = marketProps.strikeouts || {};
      const outsMarket = marketProps.outs || {};
      return `
        <tr>
          <td>${escapeHtml(month?.label || month?.month || "-")}</td>
          <td>${escapeHtml(formatNumber(month?.games, 0))}</td>
          <td>${escapeHtml(formatPercent(moneyline.accuracy, 1))}</td>
          <td>${escapeHtml(formatNumber(moneyline.brier, 3))}</td>
          <td>${escapeHtml(formatNumber(totals.mae, 2))}</td>
          <td>${escapeHtml(formatPercent(runline.accuracy, 1))}</td>
          <td>${escapeHtml(formatPercent(soMarket.accuracy, 1))}</td>
          <td>${escapeHtml(formatPercent(outsMarket.accuracy, 1))}</td>
          <td>${escapeHtml(formatNumber(starterProps.so_mae, 2))}</td>
          <td>${escapeHtml(formatNumber(starterProps.outs_mae, 2))}</td>
        </tr>`;
    }).join("");

    const driftRows = months.map((month) => {
      const props = month?.hitter_props_likelihood_topn || {};
      const hr = month?.hitter_hr_likelihood_topn || {};
      const hrGap = (toNumber(hr.hr_avg_p) != null && toNumber(hr.hr_emp_rate) != null)
        ? toNumber(hr.hr_avg_p) - toNumber(hr.hr_emp_rate)
        : null;

      const hits1 = hitterPropMetrics(props, "hits_1plus");
      const runs1 = hitterPropMetrics(props, "runs_1plus");
      const rbi1 = hitterPropMetrics(props, "rbi_1plus");
      const tb1 = hitterPropMetrics(props, "total_bases_1plus");
      const tb2 = hitterPropMetrics(props, "total_bases_2plus");

      function gapPill(metricsOrGap, actualMaybe) {
        let gap = null;
        if (typeof metricsOrGap === "object" && metricsOrGap) {
          gap = (metricsOrGap.avgP != null && metricsOrGap.empRate != null) ? metricsOrGap.avgP - metricsOrGap.empRate : null;
        } else {
          gap = metricsOrGap;
        }
        return `<span class="season-gap-pill ${hitterGapClass(gap)}">${escapeHtml(formatSignedPercentPoints(gap, 1))}</span>`;
      }

      return `
        <tr>
          <td>${escapeHtml(month?.label || month?.month || "-")}</td>
          <td>${gapPill(hrGap)}</td>
          <td>${gapPill(hits1)}</td>
          <td>${gapPill(runs1)}</td>
          <td>${gapPill(rbi1)}</td>
          <td>${gapPill(tb1)}</td>
          <td>${gapPill(tb2)}</td>
        </tr>`;
    }).join("");

    root.monthlyBreakdown.innerHTML = `
      <div class="season-panel-head">
        <div>
          <div class="season-kicker">Season trends</div>
          <div class="season-panel-title">Month-by-month breakdown</div>
        </div>
        <div class="season-inline-note">Market accuracy and hitter drift across the full published season.</div>
      </div>
      <div class="season-breakdown-grid">
        <section class="season-breakdown-card">
          <div class="season-breakdown-title">Markets by month</div>
          <div class="season-calibration-table-wrap">
            <table class="season-calibration-table season-breakdown-table">
              <thead>
                <tr>
                  <th>Month</th>
                  <th>Games</th>
                  <th>ML acc</th>
                  <th>ML brier</th>
                  <th>Total mae</th>
                  <th>RL acc</th>
                  <th>SO mkt acc</th>
                  <th>Outs mkt acc</th>
                  <th>SO mae</th>
                  <th>Outs mae</th>
                </tr>
              </thead>
              <tbody>${marketRows}</tbody>
            </table>
          </div>
        </section>
        <section class="season-breakdown-card">
          <div class="season-breakdown-title">Hitter and HR drift by month</div>
          <div class="season-calibration-table-wrap">
            <table class="season-calibration-table season-breakdown-table">
              <thead>
                <tr>
                  <th>Month</th>
                  <th>HR 1+</th>
                  <th>H 1+</th>
                  <th>R 1+</th>
                  <th>RBI 1+</th>
                  <th>TB 1+</th>
                  <th>TB 2+</th>
                </tr>
              </thead>
              <tbody>${driftRows}</tbody>
            </table>
          </div>
        </section>
      </div>`;
  }

  function bettingStatBox(label, value, subcopy) {
    return `
      <div class="season-stat-box">
        <div class="season-stat-label">${escapeHtml(label)}</div>
        <div class="season-stat-value season-betting-stat-value">${escapeHtml(value)}</div>
        ${subcopy ? `<div class="season-inline-note season-betting-stat-sub">${escapeHtml(subcopy)}</div>` : ""}
      </div>`;
  }

  function bettingComparisonDeltas() {
    const baseline = state.bettingCards?.baseline;
    const retuned = state.bettingCards?.retuned;
    const baselineCombined = baseline?.summary?.results?.combined || null;
    const retunedCombined = retuned?.summary?.results?.combined || null;
    if (!baselineCombined || !retunedCombined) return null;

    return {
      roiDelta: (toNumber(retunedCombined.roi) != null && toNumber(baselineCombined.roi) != null)
        ? toNumber(retunedCombined.roi) - toNumber(baselineCombined.roi)
        : null,
      profitDelta: (toNumber(retunedCombined.profit_u) != null && toNumber(baselineCombined.profit_u) != null)
        ? toNumber(retunedCombined.profit_u) - toNumber(baselineCombined.profit_u)
        : null,
      stakeDelta: (toNumber(retunedCombined.stake_u) != null && toNumber(baselineCombined.stake_u) != null)
        ? toNumber(retunedCombined.stake_u) - toNumber(baselineCombined.stake_u)
        : null,
      betsDelta: (toNumber(retunedCombined.n) != null && toNumber(baselineCombined.n) != null)
        ? toNumber(retunedCombined.n) - toNumber(baselineCombined.n)
        : null,
    };
  }

  function availableBettingProfiles() {
    return ["baseline", "retuned"].filter((profile) => Boolean(state.bettingCards?.[profile]));
  }

  function bettingDayEntry(dateStr, profileKey) {
    const profile = String(profileKey || state.bettingProfile || "");
    const manifest = state.bettingCards?.[profile] || null;
    const days = Array.isArray(manifest?.days) ? manifest.days : [];
    return days.find((row) => String(row?.date || "") === String(dateStr || "")) || null;
  }

  function dayAvailability(day) {
    const dateStr = String(day?.date || "");
    const bettingCards = day?.betting_cards || {};
    const profiles = bettingCards?.profiles || {};
    const defaultProfile = String(bettingCards?.default_profile || state.bettingProfile || "");
    const seasonCard = profiles?.[state.bettingProfile] || profiles?.[defaultProfile] || bettingDayEntry(dateStr, state.bettingProfile);
    return {
      hasSeasonCard: Boolean(seasonCard?.available || seasonCard?.card_path || bettingCards?.available),
      hasLegacyCardsPage: Boolean(day?.legacy_cards_available ?? day?.cards_available),
    };
  }

  function dayBettingProfileEntry(day) {
    const bettingCards = day?.betting_cards || {};
    const profiles = bettingCards?.profiles || {};
    const defaultProfile = String(bettingCards?.default_profile || state.bettingProfile || "");
    const manifestDay = bettingDayEntry(String(day?.date || ""), state.bettingProfile);
    return profiles?.[state.bettingProfile]
      || profiles?.[defaultProfile]
      || manifestDay
      || null;
  }

  function selectedBettingManifest() {
    const profiles = availableBettingProfiles();
    if (!profiles.length) return null;
    if (profiles.includes(state.bettingProfile) && state.bettingCards?.[state.bettingProfile]) {
      return state.bettingCards[state.bettingProfile];
    }
    if (profiles.includes("retuned")) {
      state.bettingProfile = "retuned";
      return state.bettingCards.retuned;
    }
    state.bettingProfile = profiles[0];
    return state.bettingCards[state.bettingProfile];
  }

  function bettingProfileLabel(profileKey) {
    return BETTING_PROFILE_LABELS[profileKey] || profileKey;
  }

  function bettingCapsText(meta) {
    const caps = meta?.caps || {};
    const subcaps = meta?.hitter_subcaps || {};
    return [
      `Tot ${caps.totals ?? "-"}`,
      `ML ${caps.ml ?? "-"}`,
      `P ${caps.pitcher_props ?? "-"}`,
      `H ${caps.hitter_props ?? "-"}`,
      `HR ${subcaps.hitter_home_runs ?? "-"}`,
      `Hits ${subcaps.hitter_hits ?? "-"}`,
      `TB ${subcaps.hitter_total_bases ?? "-"}`,
      `Runs ${subcaps.hitter_runs ?? "-"}`,
      `RBIs ${subcaps.hitter_rbis ?? "-"}`,
    ].join(" | ");
  }

  function bettingHitterEdgeText(policy) {
    const overrides = policy?.hitter_edge_min_by_market || {};
    const parts = [];
    if (toNumber(policy?.hitter_edge_min) != null) {
      parts.push(`Base ${formatPercent(policy.hitter_edge_min, 1)}`);
    }
    if (toNumber(overrides?.hitter_runs) != null) {
      parts.push(`Runs ${formatPercent(overrides.hitter_runs, 1)}`);
    }
    if (toNumber(overrides?.hitter_rbis) != null) {
      parts.push(`RBIs ${formatPercent(overrides.hitter_rbis, 1)}`);
    }
    return parts.join(" | ") || "No submarket overrides";
  }

  function bettingComparisonText() {
    const deltas = bettingComparisonDeltas();
    if (!deltas) return "";

    return [
      `Retuned vs baseline: ${formatSignedPercentPoints(deltas.roiDelta, 1)} ROI`,
      `${formatUnits(deltas.profitDelta, 2)} profit`,
      `${formatUnits(deltas.stakeDelta, 2)} stake`,
      `${formatSigned(deltas.betsDelta, 0)} bets`,
    ].join(" | ");
  }

  function renderBettingCardsRecap() {
    if (!root.bettingCards) return;
    const selected = selectedBettingManifest();
    if (!selected) {
      root.bettingCards.innerHTML = '<div class="season-empty-copy">No season betting-card recap has been published yet.</div>';
      return;
    }

    const meta = selected.meta || {};
    const summary = selected.summary || {};
    const results = summary.results || {};
    const combined = results.combined || {};
    const hitter = results.hitter_props || {};
    const daily = summary.daily || {};
    const months = Array.isArray(selected.months) ? selected.months : [];
    const availableProfiles = availableBettingProfiles();
    const comparisonText = bettingComparisonText();
    const comparisonDeltas = bettingComparisonDeltas();
    const selectedCounts = summary.selected_counts || {};
    const ml = results.ml || {};
    const pitcher = results.pitcher_props || {};
    const bestDay = daily.best_day || {};
    const worstDay = daily.worst_day || {};
    const policy = meta.policy || {};

    const profileControls = availableProfiles.length > 1
      ? `<div class="season-betting-toolbar">${availableProfiles.map((profile) => `
          <button
            type="button"
            class="cards-filter-pill ${profile === state.bettingProfile ? "is-active" : ""}"
            data-betting-profile="${escapeHtml(profile)}"
          >
            ${escapeHtml(bettingProfileLabel(profile))}
          </button>`).join("")}</div>`
      : "";

    const profileSnapshot = `
      <section class="season-breakdown-card season-betting-overview-card">
        <div class="season-breakdown-title">Profile snapshot</div>
        <div class="season-inline-note">Exact-settled locked-policy cards reconstructed from the published season eval days.</div>
        <div class="season-stat-grid">
          ${bettingStatBox("Cap profile", String(meta.cap_profile || "-"), bettingCapsText(meta))}
          ${bettingStatBox(
            "Published reports",
            `${formatNumber(meta.processed_reports ?? summary.cards_processed, 0)}/${formatNumber(meta.available_reports ?? summary.cards, 0)}`,
            meta.partial ? "Partial season publication" : "Full published season"
          )}
          ${bettingStatBox(
            "Cards with bets",
            `${formatNumber(daily.cards_with_bets, 0)}/${formatNumber(daily.cards ?? summary.cards, 0)}`,
            `${formatNumber(daily.cards_without_bets, 0)} no-bet days`
          )}
          ${bettingStatBox(
            "Selected mix",
            formatNumber(selectedCounts.combined, 0),
            `Tot ${selectedCounts.totals ?? 0} | ML ${selectedCounts.ml ?? 0} | P ${selectedCounts.pitcher_props ?? 0} | H ${selectedCounts.hitter_props ?? 0}`
          )}
        </div>
      </section>`;

    const selectionOverview = `
      <section class="season-breakdown-card season-betting-overview-card">
        <div class="season-breakdown-title">Selection policy</div>
        <div class="season-inline-note">Active thresholds and caps used to rebuild the official daily cards for this profile.</div>
        <div class="season-stat-grid">
          ${bettingStatBox("Totals", `${String(policy.totals_side || "-")} side`, `Diff ${formatNumber(policy.totals_diff_min, 2)} | Cap ${meta?.caps?.totals ?? "-"}`)}
          ${bettingStatBox("Moneyline", `${String(policy.ml_side || "-")} side`, `Edge ${formatPercent(policy.ml_edge_min, 1)} | Cap ${meta?.caps?.ml ?? "-"}`)}
          ${bettingStatBox("Pitcher props", `${String(policy.pitcher_side || "-")} ${String(policy.pitcher_market || "-")}`, `Edge ${formatPercent(policy.pitcher_edge_min, 1)} | Cap ${meta?.caps?.pitcher_props ?? "-"}`)}
          ${bettingStatBox("Hitter caps", `${meta?.caps?.hitter_props ?? "-"} daily max`, `HR ${meta?.hitter_subcaps?.hitter_home_runs ?? "-"} | Hits ${meta?.hitter_subcaps?.hitter_hits ?? "-"} | TB ${meta?.hitter_subcaps?.hitter_total_bases ?? "-"} | Runs ${meta?.hitter_subcaps?.hitter_runs ?? "-"} | RBI ${meta?.hitter_subcaps?.hitter_rbis ?? "-"}`)}
          ${bettingStatBox("Hitter edges", bettingHitterEdgeText(policy), "Base floor plus any promoted submarket overrides")}
        </div>
      </section>`;

    const comparisonOverview = comparisonDeltas
      ? `
      <section class="season-breakdown-card season-betting-overview-card">
        <div class="season-breakdown-title">Profile comparison</div>
        <div class="season-inline-note">${escapeHtml(comparisonText)}</div>
        <div class="season-stat-grid">
          ${bettingStatBox("ROI delta", formatSignedPercentPoints(comparisonDeltas.roiDelta, 1), "Retuned minus baseline")}
          ${bettingStatBox("Profit delta", formatUnits(comparisonDeltas.profitDelta, 2), "Net season units")}
          ${bettingStatBox("Stake delta", formatUnits(comparisonDeltas.stakeDelta, 2), "Exposure change")}
          ${bettingStatBox("Bet delta", formatSigned(comparisonDeltas.betsDelta, 0), "Settled recommendation count")}
        </div>
      </section>`
      : "";

    const marketRows = BETTING_MARKET_ORDER.map((marketKey) => {
      const block = results?.[marketKey] || {};
      return `
        <tr>
          <td>${escapeHtml(BETTING_MARKET_LABELS[marketKey] || marketKey)}</td>
          <td>${escapeHtml(formatNumber(block.n, 0))}</td>
          <td>${escapeHtml(formatNumber(block.stake_u, 2))}</td>
          <td>${escapeHtml(formatUnits(block.profit_u, 2))}</td>
          <td>${escapeHtml(formatPercent(block.roi, 1))}</td>
        </tr>`;
    }).join("");

    const monthRows = months.map((month) => {
      const monthDaily = month?.daily || {};
      const monthResults = month?.results || {};
      const monthCombined = monthResults.combined || {};
      const monthHitter = monthResults.hitter_props || {};
      return `
        <tr>
          <td>${escapeHtml(month?.label || month?.month || "-")}</td>
          <td>${escapeHtml(formatNumber(monthDaily.cards, 0))}</td>
          <td>${escapeHtml(formatNumber(monthCombined.n, 0))}</td>
          <td>${escapeHtml(formatUnits(monthCombined.profit_u, 2))}</td>
          <td>${escapeHtml(formatPercent(monthCombined.roi, 1))}</td>
          <td>${escapeHtml(formatPercent(monthHitter.roi, 1))}</td>
        </tr>`;
    }).join("");

    root.bettingCards.innerHTML = `
      <div class="season-panel-head">
        <div>
          <div class="season-kicker">Recommendation engine</div>
          <div class="season-panel-title">Betting card recap</div>
        </div>
        ${profileControls}
      </div>
      <div class="season-inline-note">Season-level realized performance for reconstructed official locked-policy cards, presented in the same season eval view.</div>
      <div class="season-breakdown-grid season-betting-overview-grid">
        ${profileSnapshot}
        ${selectionOverview}
        ${comparisonOverview}
      </div>
      <section class="season-summary-grid season-betting-summary-grid">
        ${metricCard("Combined ROI", formatPercent(combined.roi, 1), `${formatUnits(combined.profit_u, 2)} on ${formatNumber(combined.stake_u, 2)}u`) }
        ${metricCard("Season profit", formatUnits(combined.profit_u, 2), `${formatNumber(combined.wins, 0)} wins | ${formatNumber(combined.losses, 0)} losses`) }
        ${metricCard("Stake placed", `${formatNumber(combined.stake_u, 2)}u`, `${formatNumber(combined.n, 0)} settled bets`) }
        ${metricCard("Hitter ROI", formatPercent(hitter.roi, 1), `${formatUnits(hitter.profit_u, 2)} on ${formatNumber(hitter.stake_u, 2)}u`) }
        ${metricCard("Moneyline ROI", formatPercent(ml.roi, 1), `${formatUnits(ml.profit_u, 2)} on ${formatNumber(ml.stake_u, 2)}u`) }
        ${metricCard("Pitcher ROI", formatPercent(pitcher.roi, 1), `${formatUnits(pitcher.profit_u, 2)} on ${formatNumber(pitcher.stake_u, 2)}u`) }
        ${metricCard("Daily mean", formatUnits(daily.mean_u, 2), `Median ${formatUnits(daily.median_u, 2)}`) }
        ${metricCard("Best day", formatUnits(bestDay.profit_u, 2), String(bestDay.date || "-")) }
        ${metricCard("Worst day", formatUnits(worstDay.profit_u, 2), String(worstDay.date || "-")) }
      </section>
      <div class="season-breakdown-grid">
        <section class="season-breakdown-card">
          <div class="season-breakdown-title">Settled market breakdown</div>
          <div class="season-calibration-table-wrap">
            <table class="season-calibration-table season-breakdown-table">
              <thead>
                <tr>
                  <th>Market</th>
                  <th>Bets</th>
                  <th>Stake</th>
                  <th>Profit</th>
                  <th>ROI</th>
                </tr>
              </thead>
              <tbody>${marketRows}</tbody>
            </table>
          </div>
        </section>
        <section class="season-breakdown-card">
          <div class="season-breakdown-title">Monthly card performance</div>
          <div class="season-calibration-table-wrap">
            <table class="season-calibration-table season-breakdown-table">
              <thead>
                <tr>
                  <th>Month</th>
                  <th>Cards</th>
                  <th>Bets</th>
                  <th>Profit</th>
                  <th>ROI</th>
                  <th>Hitter ROI</th>
                </tr>
              </thead>
              <tbody>${monthRows || '<tr><td colspan="6">No monthly betting-card recap is available.</td></tr>'}</tbody>
            </table>
          </div>
        </section>
      </div>`;
  }

  async function loadBettingCardsRecap() {
    if (!root.bettingCards) return;
    root.bettingCards.innerHTML = '<div class="cards-loading-state">Loading betting card recap...</div>';
    const profiles = ["baseline", "retuned"];
    const results = await Promise.allSettled(
      profiles.map((profile) => fetchJson(`/api/season/${encodeURIComponent(state.season)}/betting-cards?profile=${encodeURIComponent(profile)}`))
    );

    state.bettingCards = { baseline: null, retuned: null };
    results.forEach((result, index) => {
      if (result.status === "fulfilled" && result.value && result.value.found) {
        state.bettingCards[profiles[index]] = result.value;
      }
    });

    if (!selectedBettingManifest()) {
      root.bettingCards.innerHTML = '<div class="season-empty-copy">No season betting-card recap has been published yet.</div>';
      return;
    }
    renderHeader();
    renderDays();
    renderBettingCardsRecap();
  }

  function renderMonths() {
    if (!root.months) return;
    const allDays = Array.isArray(state.manifest?.days) ? state.manifest.days : [];
    const months = Array.isArray(state.manifest?.months) ? state.manifest.months : [];
    const options = [{ key: "all", label: `All ${allDays.length}` }].concat(
      months.map((row) => ({
        key: String(row?.month || ""),
        label: `${monthLabel(String(row?.month || ""))} ${row?.days ?? 0}`,
      }))
    );
    root.months.innerHTML = options
      .map((option) => `
        <button
          type="button"
          class="cards-filter-pill ${option.key === state.monthFilter ? "is-active" : ""}"
          data-month-filter="${escapeHtml(option.key)}"
        >
          ${escapeHtml(option.label)}
        </button>`)
      .join("");
  }

  function renderDays() {
    if (!root.days) return;
    const days = filteredDays();
    if (!days.length) {
      root.days.innerHTML = '<div class="season-empty-copy">No days match the current month filter.</div>';
      return;
    }
    root.days.innerHTML = days
      .map((day) => {
        const isActive = String(day?.date || "") === state.selectedDate;
        const moneyline = ((day?.full_game || {}).moneyline || {});
        const totals = ((day?.full_game || {}).totals || {});
        const availability = dayAvailability(day);
        const bettingProfile = dayBettingProfileEntry(day);
        const officialCount = Number(((bettingProfile?.selected_counts || {}).combined) ?? 0);
        const playableCount = Number(((bettingProfile?.playable_counts || {}).combined) ?? 0);
        let statusText = "Season eval only";
        let mutedClass = " is-muted";
        if (availability.hasSeasonCard && availability.hasLegacyCardsPage) {
          statusText = "Season card + daily cards page";
          mutedClass = "";
        } else if (availability.hasSeasonCard) {
          statusText = "Season card available";
          mutedClass = "";
        } else if (availability.hasLegacyCardsPage) {
          statusText = "Legacy daily cards page";
          mutedClass = "";
        }
        const dateStr = String(day?.date || "");
        const badgeMarkup = availability.hasSeasonCard
          ? `
            <div class="season-day-badges">
              ${officialCount > 0 ? `<button type="button" class="season-day-pill season-day-pill-action is-official" data-season-date="${escapeHtml(dateStr)}" data-season-day-picks="official" aria-label="Open ${escapeHtml(dateStr)} official picks">${escapeHtml(formatNumber(officialCount, 0))} on card</button>` : ""}
              ${playableCount > 0 ? `<button type="button" class="season-day-pill season-day-pill-action is-playable" data-season-date="${escapeHtml(dateStr)}" data-season-day-picks="playable" aria-label="Open ${escapeHtml(dateStr)} playable props">+${escapeHtml(formatNumber(playableCount, 0))} playable</button>` : ""}
              ${officialCount <= 0 && playableCount <= 0 ? '<span class="season-day-pill is-empty">No picks</span>' : ""}
            </div>`
          : "";
        return `
          <article class="season-day-entry">
            <button
              type="button"
              class="season-day-button ${isActive ? "is-active" : ""}"
              data-season-date="${escapeHtml(dateStr)}"
            >
              <div class="season-day-row">
                <div class="season-day-primary">${escapeHtml(dateStr)}</div>
                <span class="cards-chip">${escapeHtml(String(day?.games || 0))} games</span>
              </div>
              <div class="season-day-secondary">ML brier ${escapeHtml(formatNumber(moneyline.brier, 3))} | Total mae ${escapeHtml(formatNumber(totals.mae, 2))}</div>
            </button>
            ${badgeMarkup}
            <div class="season-inline-note${mutedClass}">${escapeHtml(statusText)}</div>
          </article>`;
      })
      .join("");
  }

  function actualScoreText(segment, awayAbbr, homeAbbr) {
    const actual = segment?.actual || {};
    const away = actual?.away;
    const home = actual?.home;
    if (away == null && home == null) return "Actual score unavailable";
    return `${awayAbbr || "Away"} ${away ?? "-"} - ${homeAbbr || "Home"} ${home ?? "-"}`;
  }

  function segmentCard(label, segment, awayAbbr, homeAbbr) {
    if (!segment || typeof segment !== "object") {
      return `
        <article class="season-segment-card">
          <div class="season-segment-title">${escapeHtml(label)}</div>
          <div class="season-empty-copy">No ${escapeHtml(label)} segment data.</div>
        </article>`;
    }
    const metrics = segment.metrics || {};
    return `
      <article class="season-segment-card">
        <div class="season-segment-head">
          <div class="season-segment-title">${escapeHtml(label)}</div>
          <span class="cards-chip">Tie ${escapeHtml(formatPercent(segment.tie_prob, 1))}</span>
        </div>
        <div class="season-segment-copy">${escapeHtml(actualScoreText(segment, awayAbbr, homeAbbr))}</div>
        <div class="season-prob-row">
          <div class="season-prob-box">
            <div class="season-prob-label">${escapeHtml(awayAbbr || "Away")}</div>
            <div class="season-prob-value">${escapeHtml(formatPercent(segment.away_win_prob, 1))}</div>
          </div>
          <div class="season-prob-box">
            <div class="season-prob-label">${escapeHtml(homeAbbr || "Home")}</div>
            <div class="season-prob-value">${escapeHtml(formatPercent(segment.home_win_prob, 1))}</div>
          </div>
        </div>
        <div class="season-stat-grid">
          <div class="season-stat-box">
            <div class="season-stat-label">Mean total</div>
            <div class="season-stat-value">${escapeHtml(formatNumber(segment.mean_total_runs, 2))}</div>
          </div>
          <div class="season-stat-box">
            <div class="season-stat-label">Abs total err</div>
            <div class="season-stat-value">${escapeHtml(formatNumber(metrics.abs_err_total_runs, 2))}</div>
          </div>
          <div class="season-stat-box">
            <div class="season-stat-label">Mean home margin</div>
            <div class="season-stat-value">${escapeHtml(formatSigned(segment.mean_run_margin_home_minus_away, 2))}</div>
          </div>
          <div class="season-stat-box">
            <div class="season-stat-label">Abs margin err</div>
            <div class="season-stat-value">${escapeHtml(formatNumber(metrics.abs_err_run_margin, 2))}</div>
          </div>
        </div>
      </article>`;
  }

  function starterCard(sideLabel, starterName, block) {
    const actual = block?.actual || {};
    const pred = block?.pred || {};
    const market = block?.market || null;
    const marketText = market
      ? `${String(market.selection || "").toUpperCase()} ${formatNumber(market.market_line, 1)} ${String(market.odds || "")}`
      : "No saved market line";
    return `
      <article class="season-starter-card">
        <div class="season-starter-head">
          <div class="season-starter-title">${escapeHtml(sideLabel)}</div>
          <span class="cards-chip">${escapeHtml(starterName || "Starter")}</span>
        </div>
        <div class="season-starter-copy">${escapeHtml(marketText)}</div>
        <div class="season-stat-grid">
          <div class="season-stat-box">
            <div class="season-stat-label">SO sim mean</div>
            <div class="season-stat-value">${escapeHtml(formatNumber(pred.so_mean, 2))}</div>
          </div>
          <div class="season-stat-box">
            <div class="season-stat-label">SO actual</div>
            <div class="season-stat-value">${escapeHtml(formatNumber(actual.so, 0))}</div>
          </div>
          <div class="season-stat-box">
            <div class="season-stat-label">Outs sim mean</div>
            <div class="season-stat-value">${escapeHtml(formatNumber(pred.outs_mean, 2))}</div>
          </div>
          <div class="season-stat-box">
            <div class="season-stat-label">Outs actual</div>
            <div class="season-stat-value">${escapeHtml(formatNumber(actual.outs, 0))}</div>
          </div>
        </div>
      </article>`;
  }

  function bettingMetricLabel(reco) {
    const market = String(reco?.market || "").toLowerCase();
    const prop = String(reco?.prop || "").toLowerCase();
    if (market.includes("home_runs") || prop.includes("home_runs")) return "HR";
    if (market.includes("total_bases") || prop.includes("total_bases")) return "TB";
    if (market.includes("rbis") || prop.includes("rbi")) return "RBI";
    if (market.includes("hitter_runs") || prop.includes("runs_scored")) return "Runs";
    if (market.includes("hitter_hits") || prop.endsWith("hits")) return "Hits";
    if (prop === "outs") return "Outs";
    return BETTING_MARKET_LABELS[market] || String(reco?.market_label || reco?.market || "Prop");
  }

  function bettingOfficialRows(betting) {
    const markets = betting?.markets || {};
    const rows = [];
    if (markets.totals) rows.push(markets.totals);
    if (markets.ml) rows.push(markets.ml);
    rows.push(...(Array.isArray(markets.pitcherProps) ? markets.pitcherProps : []));
    rows.push(...(Array.isArray(markets.hitterProps) ? markets.hitterProps : []));
    return rows;
  }

  function bettingPlayableRows(betting) {
    const markets = betting?.markets || {};
    return []
      .concat(Array.isArray(markets.extraPitcherProps) ? markets.extraPitcherProps : [])
      .concat(Array.isArray(markets.extraHitterProps) ? markets.extraHitterProps : []);
  }

  function bettingSelectedModelProb(reco) {
    const market = String(reco?.market || "").toLowerCase();
    const selection = String(reco?.selection || "").toLowerCase();
    if (market === "ml") {
      const modelProb = toNumber(reco?.model_prob);
      if (modelProb == null) return null;
      if (selection === "away") return 1 - modelProb;
      return modelProb;
    }
    const overProb = toNumber(reco?.model_prob_over);
    if (overProb == null) return null;
    if (selection === "under") return 1 - overProb;
    return overProb;
  }

  function bettingSelectedMarketProb(reco) {
    const selection = String(reco?.selection || "").toLowerCase();
    const direct = toNumber(reco?.selected_side_market_prob);
    if (direct != null) return direct;

    const marketNoVig = toNumber(reco?.market_no_vig_prob);
    if (marketNoVig != null) {
      if (String(reco?.market || "").toLowerCase() === "ml" && selection === "away") {
        return 1 - marketNoVig;
      }
      return marketNoVig;
    }

    const overProb = toNumber(reco?.market_no_vig_prob_over) ?? toNumber(reco?.market_prob_over);
    const underProb = toNumber(reco?.market_prob_under);
    if (selection === "under") {
      if (underProb != null) return underProb;
      if (overProb != null) return 1 - overProb;
      return null;
    }
    return overProb;
  }

  function bettingSelectedCounts(counts) {
    const raw = counts && typeof counts === "object" ? counts : {};
    const safe = {
      totals: Number(raw.totals || 0),
      ml: Number(raw.ml || 0),
      pitcher_props: Number(raw.pitcher_props || 0),
      hitter_props: Number(raw.hitter_props || 0),
      hitter_home_runs: Number(raw.hitter_home_runs || 0),
      hitter_hits: Number(raw.hitter_hits || 0),
      hitter_total_bases: Number(raw.hitter_total_bases || 0),
      hitter_runs: Number(raw.hitter_runs || 0),
      hitter_rbis: Number(raw.hitter_rbis || 0),
      combined: Number(raw.combined || 0),
    };
    if (safe.hitter_props <= 0) {
      safe.hitter_props = safe.hitter_home_runs + safe.hitter_hits + safe.hitter_total_bases + safe.hitter_runs + safe.hitter_rbis;
    }
    if (safe.combined <= 0) {
      safe.combined = safe.totals + safe.ml + safe.pitcher_props + safe.hitter_props;
    }
    return safe;
  }

  function dayPickGroups() {
    const groups = {
      official: [],
      props: [],
      playable: [],
      counts: {
        official: 0,
        props: 0,
        officialProps: 0,
        playable: 0,
        playablePitcher: 0,
        playableHitter: 0,
      },
    };
    const games = Array.isArray(state.day?.games) ? state.day.games : [];
    games.forEach((game, gameIndex) => {
      const betting = game?.betting || {};
      const awayAbbr = game?.away?.abbr || "Away";
      const homeAbbr = game?.home?.abbr || "Home";
      bettingOfficialRows(betting).forEach((reco, recoIndex) => {
        const item = { game, reco, awayAbbr, homeAbbr, bucket: "official", sortKey: `${gameIndex}-${recoIndex}` };
        groups.official.push(item);
        const market = String(reco?.market || "").toLowerCase();
        if (market === "pitcher_props" || market === "hitter_props") {
          groups.props.push(item);
        }
      });
      bettingPlayableRows(betting).forEach((reco, recoIndex) => {
        const item = { game, reco, awayAbbr, homeAbbr, bucket: "playable", sortKey: `${gameIndex}-${recoIndex}` };
        groups.playable.push(item);
        groups.props.push(item);
      });
      const counts = betting?.counts || {};
      groups.counts.playablePitcher += Number(counts.extra_pitcher || 0);
      groups.counts.playableHitter += Number(counts.extra_hitter || 0);
      groups.counts.officialProps += Number(counts.pitcher || 0) + Number(counts.hitter || 0);
    });
    groups.counts.official = groups.official.length;
    groups.counts.props = groups.props.length;
    groups.counts.playable = groups.playable.length;
    return groups;
  }

  function normalizedDayPicksMode(groups) {
    if (state.dayPicksMode === "props" && groups.props.length) return "props";
    if (state.dayPicksMode === "playable" && groups.playable.length) return "playable";
    if (state.dayPicksMode === "official" && groups.official.length) return "official";
    if (groups.props.length) return "props";
    if (groups.official.length) return "official";
    if (groups.playable.length) return "playable";
    return "props";
  }

  function setDayPicksMode(mode, options) {
    state.dayPicksMode = mode === "playable"
      ? "playable"
      : mode === "official"
        ? "official"
        : "props";
    renderDaySummary();
    renderDayPicksRecap();
    if (options?.scroll && root.dayPicks) {
      root.dayPicks.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  }

  async function activateDayPicks(dateStr, mode) {
    const nextDate = String(dateStr || "");
    if (!nextDate) return;
    state.dayPicksMode = mode === "playable"
      ? "playable"
      : mode === "official"
        ? "official"
        : "props";
    if (state.selectedDate === nextDate && state.day?.date === nextDate) {
      setDayPicksMode(state.dayPicksMode, { scroll: true });
      return;
    }
    await loadDay(nextDate);
    if (root.dayPicks) {
      root.dayPicks.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  }

  function dayPickGameAnchorId(gamePk) {
    const text = String(gamePk || "").trim();
    return text ? `season-game-${text}` : "";
  }

  function dayPickRowsMarkup(items, mode) {
    return items.map((item) => {
      const game = item.game || {};
      const reco = item.reco || {};
      const awayAbbr = item.awayAbbr || "Away";
      const homeAbbr = item.homeAbbr || "Home";
      const anchorId = dayPickGameAnchorId(game.game_pk);
      const settlement = reco?.settlement || null;
      const itemMode = String(item.bucket || (mode === "playable" ? "playable" : "official"));
      const isPlayable = itemMode === "playable";
      const statusText = bettingResultLabel(reco);
      const tone = bettingResultTone(reco);
      const profitText = settlement ? formatUnits(settlement.profit_u, 2) : "-";
      const actualText = settlement && settlement.actual != null
        ? `Actual ${formatLine(settlement.actual)}`
        : "Settlement unavailable";
      const gameLabel = `${awayAbbr} @ ${homeAbbr}`;
      const gameMeta = [
        game?.start_time ? `First pitch ${game.start_time}` : "",
        (game?.status?.abstract || "").trim(),
      ].filter(Boolean).join(" | ");
      return `
        <tr>
          <td>
            <div class="season-betting-cell-main">${anchorId ? `<a class="season-day-picks-link" href="#${escapeHtml(anchorId)}">${escapeHtml(gameLabel)}</a>` : escapeHtml(gameLabel)}</div>
            <div class="season-betting-cell-sub">${escapeHtml(gameMeta || `Game ${String(game.game_pk || "-")}`)}</div>
          </td>
          <td>
            <div class="season-betting-cell-main">${escapeHtml(BETTING_MARKET_LABELS[String(reco?.market || "").toLowerCase()] || bettingMetricLabel(reco))}</div>
            <div class="season-betting-cell-sub">${escapeHtml(isPlayable ? "Playable board" : "Official card")}</div>
          </td>
          <td>
            <div class="season-betting-cell-main">${escapeHtml(bettingSelectionLabel(reco, awayAbbr, homeAbbr))}</div>
            <div class="season-betting-cell-sub">${escapeHtml(bettingDetailText(reco))}</div>
          </td>
          <td>${escapeHtml(formatOdds(reco?.odds))}</td>
          <td>${escapeHtml(formatSignedPercentPoints(reco?.edge, 1))}</td>
          <td><span class="season-ticket-pill ${tone}">${escapeHtml(statusText)}</span></td>
          <td>
            <div class="season-betting-cell-main">${escapeHtml(profitText)}</div>
            <div class="season-betting-cell-sub">${escapeHtml(actualText)}</div>
          </td>
        </tr>`;
    }).join("");
  }

  function renderDayPicksRecap() {
    if (!root.dayPicks) return;
    if (!state.day) {
      root.dayPicks.innerHTML = '<div class="season-empty-copy">Pick a simulated date to inspect the day-level betting recap.</div>';
      return;
    }
    const betting = state.day.betting || {};
    if (!betting.found) {
      root.dayPicks.innerHTML = '<div class="season-empty-copy">No season betting card was published for this date.</div>';
      return;
    }

    const groups = dayPickGroups();
    const mode = normalizedDayPicksMode(groups);
    state.dayPicksMode = mode;
    const activeItems = mode === "playable"
      ? groups.playable
      : mode === "official"
        ? groups.official
        : groups.props;
    const profileLabel = bettingProfileLabel(betting.profile);
    const modeTitle = mode === "playable"
      ? "Playable props by date"
      : mode === "official"
        ? "Official picks by date"
        : "Today's props board";
    const modeCopy = mode === "playable"
      ? `${formatNumber(groups.counts.playable, 0)} playable props across the selected date under ${profileLabel}.`
      : mode === "official"
        ? `${formatNumber(groups.counts.official, 0)} on-card picks across the selected date under ${profileLabel}.`
        : `${formatNumber(groups.counts.props, 0)} props for the selected date under ${profileLabel}, combining on-card prop picks and extra playable candidates.`;

    root.dayPicks.innerHTML = `
      <div class="season-panel-head">
        <div>
          <div class="season-kicker">Selected day board</div>
          <div class="season-panel-title">${escapeHtml(modeTitle)}</div>
        </div>
        <div class="season-day-picks-toolbar">
          <button type="button" class="cards-filter-pill ${mode === "props" ? "is-active" : ""}" data-day-picks-filter="props">
            ${escapeHtml(`Props ${groups.counts.props}`)}
          </button>
          <button type="button" class="cards-filter-pill ${mode === "official" ? "is-active" : ""}" data-day-picks-filter="official">
            ${escapeHtml(`Official ${groups.counts.official}`)}
          </button>
          <button type="button" class="cards-filter-pill ${mode === "playable" ? "is-active" : ""}" data-day-picks-filter="playable">
            ${escapeHtml(`Playable ${groups.counts.playable}`)}
          </button>
        </div>
      </div>
      <div class="season-inline-note">${escapeHtml(modeCopy)}</div>
      <div class="season-inline-note">${escapeHtml(`Click a game matchup to jump to the full drilldown card below. Game rows carry first-pitch time from the daily cards payload, and both official picks and playable props are exact-settled from the final game feeds.`)}</div>
      ${activeItems.length ? `
        <div class="season-calibration-table-wrap">
          <table class="season-calibration-table season-day-picks-table">
            <thead>
              <tr>
                <th>Game</th>
                <th>Market</th>
                <th>Pick</th>
                <th>Odds</th>
                <th>Edge</th>
                <th>Status</th>
                <th>Profit</th>
              </tr>
            </thead>
            <tbody>${dayPickRowsMarkup(activeItems, mode)}</tbody>
          </table>
        </div>` : '<div class="season-empty-copy">No picks match the selected recap mode for this date.</div>'}
    `;
  }

  function seasonLiveLensPageUrl(dateStr) {
    const targetDate = String(dateStr || state.selectedDate || "");
    return `/season/${encodeURIComponent(state.season)}/live-lens?date=${encodeURIComponent(targetDate)}`;
  }

  function seasonLiveLensApiUrl(dateStr) {
    const targetDate = String(dateStr || state.selectedDate || "");
    return `/api/season/${encodeURIComponent(state.season)}/live-lens?date=${encodeURIComponent(targetDate)}`;
  }

  function liveLensPropLabel(prop) {
    return String(prop?.marketLabel || prop?.prop || prop?.market || "Prop").replace(/_/g, " ");
  }

  function liveLensStatusTone(status) {
    const token = String(status || "").toLowerCase();
    if (token === "win") return "is-win";
    if (token === "loss") return "is-loss";
    return "is-pending";
  }

  function liveLensTierTone(tier) {
    const token = String(tier || "").toLowerCase();
    if (token === "playable") return "is-playable";
    if (token === "live" || token === "current_market" || token === "live_registry") return "is-official";
    return "";
  }

  function liveLensTierLabel(tier) {
    const token = String(tier || "").toLowerCase();
    if (token === "playable") return "playable";
    if (token === "live" || token === "current_market" || token === "live_registry") return "live";
    if (token === "official") return "official";
    return token || "tracked";
  }

  function liveLensPickSummary(label, market, kind) {
    if (!market || !market.pick) return `${label} -`;
    const pickText = String(market.pick || "").toUpperCase();
    if (kind === "moneyline") {
      return `${label} ${pickText} ${formatSignedPercentPoints(market.edge, 1)}`;
    }
    if (kind === "spread") {
      return `${label} ${pickText} ${formatSigned(market.edge, 2)}`;
    }
    return `${label} ${pickText} ${formatSigned(market.edge, 2)}`;
  }

  function liveLensLaneMarkup(game) {
    const rows = Array.isArray(game?.gameLens) ? game.gameLens : [];
    if (!rows.length) return '<div class="season-empty-copy">No live game-lens rows available for this matchup.</div>';
    return `
      <div class="season-live-lens-lanes">
        ${rows.map((row) => {
          const moneyline = row?.markets?.moneyline || {};
          const spread = row?.markets?.spread || {};
          const total = row?.markets?.total || {};
          const pillClass = row?.closed ? 'season-day-pill is-empty' : (String(row?.key || '') === 'live' ? 'season-day-pill is-official' : 'season-day-pill');
          const summary = [
            liveLensPickSummary('ML', moneyline, 'moneyline'),
            liveLensPickSummary('Spread', spread, 'spread'),
            liveLensPickSummary('Total', total, 'total'),
          ].join(' | ');
          return `<span class="${pillClass}">${escapeHtml(`${String(row?.label || 'Lane')}: ${summary}`)}</span>`;
        }).join('')}
      </div>`;
  }

  function liveLensPropsTable(game) {
    const props = Array.isArray(game?.props) ? game.props : [];
    if (!props.length) {
      return '<div class="season-empty-copy">No tracked live-lens props for this matchup.</div>';
    }
    return `
      <div class="season-calibration-table-wrap">
        <table class="season-calibration-table season-live-lens-table">
          <thead>
            <tr>
              <th>Player</th>
              <th>Prop</th>
              <th>Tier</th>
              <th>Pick</th>
              <th>Live proj</th>
              <th>Live edge</th>
              <th>Pregame edge</th>
              <th>Odds</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            ${props.map((prop) => `
              <tr>
                <td>
                  <div class="season-betting-cell-main">${escapeHtml(prop.playerName || '-')}</div>
                  <div class="season-betting-cell-sub">${escapeHtml(String(prop.teamSide || '').toUpperCase() || 'Team')}</div>
                </td>
                <td>${escapeHtml(liveLensPropLabel(prop))}</td>
                <td><span class="season-ticket-pill ${liveLensTierTone(prop.tier || prop.source)}">${escapeHtml(liveLensTierLabel(prop.tier || prop.source))}</span></td>
                <td>${escapeHtml(`${String(prop.selection || '').toUpperCase()} ${formatLine(prop.line)}`)}</td>
                <td>${escapeHtml(formatLine(prop.liveProjection))}</td>
                <td>${escapeHtml(formatSigned(prop.liveEdge, 2))}</td>
                <td>${escapeHtml(formatSignedPercentPoints(prop.edge, 1))}</td>
                <td>${escapeHtml(formatOdds(prop.odds))}</td>
                <td><span class="season-ticket-pill ${liveLensStatusTone(prop.status)}">${escapeHtml(String(prop.status || 'pending'))}</span></td>
              </tr>`).join('')}
          </tbody>
        </table>
      </div>`;
  }

  function liveLensPropSection(title, copy, props, emptyCopy) {
    const rows = Array.isArray(props) ? props : [];
    return `
      <section class="live-lens-prop-section">
        <div class="live-lens-prop-head">
          <div>
            <div class="live-lens-prop-title">${escapeHtml(title)}</div>
            <div class="live-lens-prop-copy">${escapeHtml(copy)}</div>
          </div>
          <span class="season-day-pill">${escapeHtml(`${formatNumber(rows.length, 0)} rows`)}</span>
        </div>
        ${rows.length ? liveLensPropsTable({ props: rows }) : `<div class="season-empty-copy">${escapeHtml(emptyCopy)}</div>`}
      </section>`;
  }

  function liveLensPropSectionsMarkup(game) {
    const liveProps = Array.isArray(game?.liveProps) ? game.liveProps : [];
    const trackedProps = Array.isArray(game?.trackedProps) ? game.trackedProps : [];
    if (!liveProps.length && !trackedProps.length) {
      return '<div class="season-empty-copy">No live or tracked player props for this matchup.</div>';
    }
    return [
      liveLensPropSection(
        'Live opportunities',
        'Current live market lines ranked by live projection edge.',
        liveProps,
        'No current live prop opportunities for this matchup.'
      ),
      trackedProps.length ? liveLensPropSection(
        'Tracked pregame props',
        'Original official and playable pregame props, updated with current game state.',
        trackedProps,
        'No tracked pregame props for this matchup.'
      ) : ''
    ].join('');
  }

  function renderLiveLensPanel() {
    if (!root.liveLens) return;
    const selectedDate = String(state.selectedDate || "");
    const pageUrl = seasonLiveLensPageUrl(selectedDate);
    if (!selectedDate) {
      root.liveLens.innerHTML = '<div class="season-empty-copy">Pick a date to inspect season live-lens recommendations.</div>';
      return;
    }

    const payload = state.liveLens;
    if (!payload || !payload.found) {
      root.liveLens.innerHTML = `
        <div class="season-panel-head">
          <div>
            <div class="season-kicker">Live board</div>
            <div class="season-panel-title">Live lens recos</div>
          </div>
          <a class="cards-nav-pill" href="${escapeHtml(pageUrl)}">Open full live lens</a>
        </div>
        <div class="season-empty-copy">No live-lens slate was available for ${escapeHtml(selectedDate)}.</div>`;
      return;
    }

    const counts = payload.counts || {};
    const games = Array.isArray(payload.games) ? payload.games : [];
    const liveProps = games.flatMap((game) => (Array.isArray(game?.liveProps) ? game.liveProps : []));
    const trackedProps = games.flatMap((game) => (Array.isArray(game?.trackedProps) ? game.trackedProps : []));
    const officialProps = trackedProps.filter((prop) => String(prop?.tier || '').toLowerCase() !== 'playable').length;
    const playableProps = trackedProps.filter((prop) => String(prop?.tier || '').toLowerCase() === 'playable').length;

    root.liveLens.innerHTML = `
      <div class="season-panel-head">
        <div>
          <div class="season-kicker">Live board</div>
          <div class="season-panel-title">Live lens recos</div>
        </div>
        <a class="cards-nav-pill" href="${escapeHtml(pageUrl)}">Open full live lens</a>
      </div>
      <div class="season-inline-note">Intraday live-lens recommendations and live projections for ${escapeHtml(selectedDate)}. This is separate from the season locked-policy recon.</div>
      <section class="season-summary-grid season-live-lens-summary">
        ${metricCard('Games', formatNumber(counts.games, 0), `Live ${formatNumber(counts.live, 0)} | Final ${formatNumber(counts.final, 0)} | Pregame ${formatNumber(counts.pregame, 0)}`)}
        ${metricCard('Live opps', formatNumber(liveProps.length, 0), liveProps.length ? 'Current market-driven in-game opportunities' : 'No current live prop opportunities')}
        ${metricCard('Tracked props', formatNumber(trackedProps.length, 0), `${formatNumber(officialProps, 0)} official | ${formatNumber(playableProps, 0)} playable`)}
        ${metricCard('Live games', formatNumber(counts.live, 0), payload.hasLiveGames ? 'At least one game is currently live' : 'No active live games in this slate')}
        ${metricCard('Historical mode', payload.isHistorical ? 'Archive' : 'Live feed', payload.isHistorical ? 'Using archived feed snapshots where available' : 'Using current feed and snapshots')}
      </section>
      <div class="season-live-lens-games">
        ${games.map((game) => {
          const away = game?.matchup?.away || {};
          const home = game?.matchup?.home || {};
          const score = game?.matchup?.score || {};
          const liveText = String(game?.matchup?.liveText || '').trim();
          const status = game?.status || {};
          return `
            <section class="season-breakdown-card season-live-lens-game-card">
              <div class="season-panel-head">
                <div>
                  <div class="season-breakdown-title">${escapeHtml(String(away.abbr || away.name || 'Away'))} at ${escapeHtml(String(home.abbr || home.name || 'Home'))}</div>
                  <div class="season-inline-note">${escapeHtml(String(status.abstract || 'Scheduled'))} | ${escapeHtml(String(status.detailed || game.startTime || ''))}</div>
                </div>
                <div class="season-day-badges">
                  <span class="season-day-pill is-official">${escapeHtml(`${String(score.away ?? '-')} - ${String(score.home ?? '-')}`)}</span>
                  ${liveText ? `<span class="season-day-pill">${escapeHtml(liveText)}</span>` : ''}
                  <span class="season-day-pill is-playable">${escapeHtml(`${formatNumber((Array.isArray(game?.liveProps) ? game.liveProps.length : 0), 0)} live`)}</span>
                  ${Array.isArray(game?.trackedProps) && game.trackedProps.length ? `<span class="season-day-pill">${escapeHtml(`${formatNumber(game.trackedProps.length, 0)} tracked`)}</span>` : ''}
                </div>
              </div>
              ${liveLensLaneMarkup(game)}
              ${liveLensPropSectionsMarkup(game)}
            </section>`;
        }).join('')}
      </div>`;
  }

  async function loadLiveLens(dateStr) {
    if (!root.liveLens) return;
    const targetDate = String(dateStr || state.selectedDate || '');
    if (!targetDate) {
      state.liveLens = null;
      renderLiveLensPanel();
      return;
    }
    root.liveLens.innerHTML = '<div class="cards-loading-state">Loading live lens...</div>';
    try {
      const response = await fetch(seasonLiveLensApiUrl(targetDate), { cache: 'no-store' });
      if (response.status === 404) {
        state.liveLens = { found: false, date: targetDate };
        renderLiveLensPanel();
        return;
      }
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      state.liveLens = await response.json();
      renderLiveLensPanel();
    } catch (error) {
      const message = error && error.message ? error.message : 'Unknown error';
      root.liveLens.innerHTML = `
        <div class="season-panel-head">
          <div>
            <div class="season-kicker">Live board</div>
            <div class="season-panel-title">Live lens recos</div>
          </div>
          <a class="cards-nav-pill" href="${escapeHtml(seasonLiveLensPageUrl(targetDate))}">Open full live lens</a>
        </div>
        <div class="cards-empty-state season-error">Failed to load live-lens recos.<div class="season-inline-note">${escapeHtml(message)}</div></div>`;
    }
  }

  function bettingSelectionLabel(reco, awayAbbr, homeAbbr) {
    const market = String(reco?.market || "").toLowerCase();
    const selection = String(reco?.selection || "").toLowerCase();
    const selectionLabel = selection ? selection.replace(/^./, (m) => m.toUpperCase()) : "Pick";
    if (market === "ml") {
      const team = selection === "home" ? homeAbbr : selection === "away" ? awayAbbr : selectionLabel;
      return `${team || "Team"} ML`;
    }
    if (market === "totals") {
      return `${selection.toUpperCase() || "TOTAL"} ${formatLine(reco?.market_line)}`;
    }
    if (market === "pitcher_props") {
      return `${reco?.pitcher_name || "Pitcher"} ${selectionLabel} ${formatLine(reco?.market_line)} ${bettingMetricLabel(reco)}`;
    }
    return `${reco?.player_name || "Player"} ${selectionLabel} ${formatLine(reco?.market_line)} ${bettingMetricLabel(reco)}`;
  }

  function bettingDetailText(reco) {
    const market = String(reco?.market || "").toLowerCase();
    const bits = [];
    if (market === "totals" && toNumber(reco?.model_mean_total) != null) {
      bits.push(`Model total ${formatLine(reco?.model_mean_total)}`);
    }
    if (market === "pitcher_props" && toNumber(reco?.outs_mean) != null) {
      bits.push(`Mean ${formatLine(reco?.outs_mean)} outs`);
    }
    const modelProb = bettingSelectedModelProb(reco);
    if (modelProb != null) bits.push(`Model ${formatPercent(modelProb, 1)}`);
    const marketProb = bettingSelectedMarketProb(reco);
    if (marketProb != null) bits.push(`Market ${formatPercent(marketProb, 1)}`);
    if (toNumber(reco?.edge) != null) bits.push(`Edge ${formatSignedPercentPoints(reco?.edge, 1)}`);
    return bits.join(" | ") || "Saved card recommendation";
  }

  function bettingResultTone(reco) {
    const result = String(reco?.settlement?.result || "").toLowerCase();
    if (result === "win") return "is-win";
    if (result === "loss") return "is-loss";
    return "is-pending";
  }

  function bettingResultLabel(reco) {
    const result = String(reco?.settlement?.result || "").toLowerCase();
    if (result === "win") return "Win";
    if (result === "loss") return "Loss";
    return "Unresolved";
  }

  function bettingTableRows(rows, awayAbbr, homeAbbr) {
    return rows.map((reco) => {
      const settlement = reco?.settlement || null;
      const profitText = settlement ? formatUnits(settlement.profit_u, 2) : "-";
      const actualText = settlement && settlement.actual != null ? `Actual ${formatLine(settlement.actual)}` : "";
      const tierText = String(reco?.recommendation_tier || "") === "candidate" ? "Playable board" : "Official card";
      return `
        <tr>
          <td>
            <div class="season-betting-cell-main">${escapeHtml(BETTING_MARKET_LABELS[String(reco?.market || "").toLowerCase()] || bettingMetricLabel(reco))}</div>
            <div class="season-betting-cell-sub">${escapeHtml(tierText)}</div>
          </td>
          <td>
            <div class="season-betting-cell-main">${escapeHtml(bettingSelectionLabel(reco, awayAbbr, homeAbbr))}</div>
            <div class="season-betting-cell-sub">${escapeHtml(bettingDetailText(reco))}</div>
          </td>
          <td>${escapeHtml(formatOdds(reco?.odds))}</td>
          <td>${escapeHtml(formatSignedPercentPoints(reco?.edge, 1))}</td>
          <td><span class="season-ticket-pill ${bettingResultTone(reco)}">${escapeHtml(bettingResultLabel(reco))}</span></td>
          <td>
            <div class="season-betting-cell-main">${escapeHtml(profitText)}</div>
            <div class="season-betting-cell-sub">${escapeHtml(actualText || "Settlement unavailable")}</div>
          </td>
        </tr>`;
    }).join("");
  }

  function renderGameBetting(game, awayAbbr, homeAbbr) {
    const dayBetting = state.day?.betting || null;
    const profileLabel = bettingProfileLabel(dayBetting?.profile || state.bettingProfile);
    if (!dayBetting?.found) {
      return `
        <section class="season-game-betting-shell">
          <div class="season-panel-head season-game-betting-head">
            <div>
              <div class="season-kicker">Betting card</div>
              <div class="season-panel-title">No saved card detail</div>
            </div>
            <span class="cards-chip">${escapeHtml(profileLabel)}</span>
          </div>
          <div class="season-empty-copy">No reconstructed betting card was published for this date and profile.</div>
        </section>`;
    }

    const betting = game?.betting || null;
    const officialRows = bettingOfficialRows(betting);
    const playableRows = bettingPlayableRows(betting);
    const counts = betting?.counts || {};
    const officialResults = betting?.results || {};
    const officialCombined = officialResults.combined || {};
    const playableResults = betting?.playable_results || {};
    const playableCombined = playableResults.combined || {};
    const markets = betting?.markets || {};
    const hasAny = Boolean(betting?.flags?.hasAnyRecommendations);

    if (!hasAny) {
      return `
        <section class="season-game-betting-shell">
          <div class="season-panel-head season-game-betting-head">
            <div>
              <div class="season-kicker">Betting card</div>
              <div class="season-panel-title">No picks for this matchup</div>
            </div>
            <div class="season-game-betting-head-meta">
              <span class="cards-chip">${escapeHtml(profileLabel)}</span>
              <span class="season-inline-note">Cap ${escapeHtml(dayBetting?.cap_profile || "-")}</span>
            </div>
          </div>
          <div class="season-empty-copy">No official plays or extra playable candidates were logged for this game under the selected card profile.</div>
        </section>`;
    }

    const officialMix = [
      markets.totals ? "Tot 1" : "",
      markets.ml ? "ML 1" : "",
      counts.pitcher ? `P ${counts.pitcher}` : "",
      counts.hitter ? `H ${counts.hitter}` : "",
    ].filter(Boolean).join(" | ") || "No official mix";
    const playableMix = [
      counts.extra_pitcher ? `P ${counts.extra_pitcher}` : "",
      counts.extra_hitter ? `H ${counts.extra_hitter}` : "",
    ].filter(Boolean).join(" | ") || "No extra candidates";

    return `
      <section class="season-game-betting-shell">
        <div class="season-panel-head season-game-betting-head">
          <div>
            <div class="season-kicker">Betting card</div>
            <div class="season-panel-title">Official card and playable board</div>
          </div>
          <div class="season-game-betting-head-meta">
            <span class="cards-chip">${escapeHtml(profileLabel)}</span>
            <span class="season-inline-note">Cap ${escapeHtml(dayBetting?.cap_profile || "-")}</span>
          </div>
        </div>
        <div class="season-stat-grid season-game-betting-stats">
          ${bettingStatBox("Official card", formatNumber(counts.official, 0), officialMix)}
          ${bettingStatBox("Playable board", formatNumber(counts.playable, 0), playableMix)}
          ${bettingStatBox("Official P/L", formatUnits(officialCombined.profit_u, 2), `ROI ${formatPercent(officialCombined.roi, 1)} | ${formatNumber(officialCombined.wins, 0)}-${formatNumber(officialCombined.losses, 0)}`)}
          ${bettingStatBox("Playable P/L", formatUnits(playableCombined.profit_u, 2), `ROI ${formatPercent(playableCombined.roi, 1)} | ${formatNumber(playableCombined.wins, 0)}-${formatNumber(playableCombined.losses, 0)}`)}
        </div>
        <div class="season-game-betting-grid">
          <section class="season-breakdown-card season-game-betting-card">
            <div class="season-breakdown-title">Official card</div>
            ${officialRows.length ? `
              <div class="season-calibration-table-wrap">
                <table class="season-calibration-table season-game-betting-table">
                  <thead>
                    <tr>
                      <th>Market</th>
                      <th>Pick</th>
                      <th>Odds</th>
                      <th>Edge</th>
                      <th>Result</th>
                      <th>Profit</th>
                    </tr>
                  </thead>
                  <tbody>${bettingTableRows(officialRows, awayAbbr, homeAbbr)}</tbody>
                </table>
              </div>` : '<div class="season-empty-copy">No official recommendations were promoted for this matchup.</div>'}
          </section>
          <section class="season-breakdown-card season-game-betting-card">
            <div class="season-breakdown-title">Playable board</div>
            ${playableRows.length ? `
              <div class="season-calibration-table-wrap">
                <table class="season-calibration-table season-game-betting-table">
                  <thead>
                    <tr>
                      <th>Market</th>
                      <th>Pick</th>
                      <th>Odds</th>
                      <th>Edge</th>
                      <th>Status</th>
                      <th>Profit</th>
                    </tr>
                  </thead>
                  <tbody>${bettingTableRows(playableRows, awayAbbr, homeAbbr)}</tbody>
                </table>
              </div>` : '<div class="season-empty-copy">No additional playable candidates were saved for this matchup.</div>'}
          </section>
        </div>
      </section>`;
  }

  function renderDaySummary() {
    if (!root.dayTitle || !root.dayMeta || !root.dayActions || !root.dayMetrics) return;
    if (!state.day) {
      root.dayTitle.textContent = "No day selected";
      root.dayMeta.textContent = "Pick a simulated date from the rail to load the day report.";
      root.dayActions.innerHTML = "";
      root.dayMetrics.innerHTML = '<div class="season-empty-copy">No day summary available.</div>';
      return;
    }

    const meta = state.day.meta || {};
    const summary = state.day.summary || {};
    const aggregate = summary.aggregate || {};
    const fullGame = summary.full_game || {};
    const totals = fullGame.totals || {};
    const moneyline = fullGame.moneyline || {};
    const runline = fullGame.runline_fav_minus_1_5 || {};
    const starters = fullGame.pitcher_props_starters || {};
    const betting = state.day.betting || {};
    const bettingCombined = (betting.results || {}).combined || {};
    const bettingPlayableCombined = (betting.playable_results || {}).combined || {};
    const bettingCounts = bettingSelectedCounts(betting.selected_counts || {});
    const bettingProfile = betting.found ? bettingProfileLabel(betting.profile) : null;
    const hasLegacyCardsPage = Boolean(state.day.cards_available);
    const liveLensLink = state.selectedDate
      ? `<a class="cards-nav-pill" href="${escapeHtml(seasonLiveLensPageUrl(state.selectedDate))}">Open live lens</a>`
      : "";
    const pickGroups = dayPickGroups();
    const activePicksMode = normalizedDayPicksMode(pickGroups);
    state.dayPicksMode = activePicksMode;

    root.dayTitle.textContent = formatDateLong(state.day.date);
    root.dayMeta.textContent = [
      `Source ${state.day.source_file || "report"}`,
      `${state.day.games?.length || 0} games`,
      `${meta.sims_per_game || "-"} sims per game`,
      bettingProfile ? `${bettingProfile} official ${formatUnits(bettingCombined.profit_u, 2)}` : "No betting card detail",
    ].join(" | ");
    if (hasLegacyCardsPage) {
      root.dayActions.innerHTML = `${liveLensLink}<a class="cards-nav-pill" href="${escapeHtml(state.day.cards_url)}">Open daily cards</a>`;
    } else if (betting.found) {
      root.dayActions.innerHTML = `${liveLensLink}<span class="season-inline-note">Season betting card is merged into the matchup cards below. No legacy daily cards page was published for this date.</span>`;
    } else {
      root.dayActions.innerHTML = `${liveLensLink}<span class="season-inline-note is-muted">No daily card artifacts for this date.</span>`;
    }
    const metrics = [
      metricCard("Games", String((aggregate.full || {}).games ?? state.day.games?.length ?? "-"), `Skipped ${meta.skipped_games ?? 0}`),
      metricCard("Moneyline brier", formatNumber(moneyline.brier, 3), `Accuracy ${formatPercent(moneyline.accuracy, 1)}`),
      metricCard("Totals mae", formatNumber(totals.mae, 2), `RMSE ${formatNumber(totals.rmse, 2)}`),
      metricCard("Runline accuracy", formatPercent(runline.accuracy, 1), `Brier ${formatNumber(runline.brier, 3)}`),
      metricCard("Starter SO mae", formatNumber(starters.so_mae, 2), `RMSE ${formatNumber(starters.so_rmse, 2)}`),
      metricCard("Starter outs mae", formatNumber(starters.outs_mae, 2), `RMSE ${formatNumber(starters.outs_rmse, 2)}`),
    ];
    if (betting.found) {
      metrics.push(metricCard("Official P/L", formatUnits(bettingCombined.profit_u, 2), `ROI ${formatPercent(bettingCombined.roi, 1)}`));
      metrics.push(metricActionCard(
        "Today's props",
        formatNumber(pickGroups.counts.props || (bettingCounts.pitcher_props + bettingCounts.hitter_props + pickGroups.counts.playable), 0),
        `On card ${pickGroups.counts.officialProps} | Playable ${pickGroups.counts.playable}`,
        "props",
        { isActive: activePicksMode === "props", disabled: pickGroups.counts.props <= 0 }
      ));
      metrics.push(metricActionCard(
        "Official bets",
        formatNumber(pickGroups.counts.official || bettingCounts.combined || bettingCombined.n, 0),
        `Tot ${bettingCounts.totals} | ML ${bettingCounts.ml} | P ${bettingCounts.pitcher_props} | H ${bettingCounts.hitter_props}`,
        "official",
        { isActive: activePicksMode === "official", disabled: pickGroups.counts.official <= 0 }
      ));
      metrics.push(metricActionCard(
        "Playable props",
        formatNumber(pickGroups.counts.playable, 0),
        `P ${pickGroups.counts.playablePitcher} | H ${pickGroups.counts.playableHitter}`,
        "playable",
        { isActive: activePicksMode === "playable", disabled: pickGroups.counts.playable <= 0 }
      ));
      metrics.push(metricCard("Playable P/L", formatUnits(bettingPlayableCombined.profit_u, 2), `ROI ${formatPercent(bettingPlayableCombined.roi, 1)} | ${formatNumber(bettingPlayableCombined.n, 0)} bets`));
      metrics.push(metricCard("Card profile", bettingProfile || "-", `Cap ${betting.cap_profile || "-"}`));
    }
    root.dayMetrics.innerHTML = metrics.join("");
  }

  function renderGames() {
    if (!root.games) return;
    if (!state.day) {
      root.games.innerHTML = '<div class="season-empty-copy">No day report loaded.</div>';
      return;
    }
    const games = Array.isArray(state.day.games) ? state.day.games : [];
    if (!games.length) {
      root.games.innerHTML = '<div class="season-empty-copy">The selected report has no game rows.</div>';
      return;
    }
    root.games.innerHTML = games
      .map((game) => {
        const full = (game.segments || {}).full || {};
        const first5 = (game.segments || {}).first5 || {};
        const first3 = (game.segments || {}).first3 || {};
        const awayAbbr = game.away?.abbr || "Away";
        const homeAbbr = game.home?.abbr || "Home";
        const gameAnchorId = dayPickGameAnchorId(game.game_pk);
        const gameCardsLink = state.day.cards_available && game.game_pk
          ? `${state.day.cards_url}#game-card-${encodeURIComponent(game.game_pk)}`
          : "";
        const gameTimeBits = [
          game?.start_time ? `First pitch ${game.start_time}` : "",
          String(game?.status?.detailed || game?.status?.abstract || "").trim(),
        ].filter(Boolean).join(" | ");
        return `
          <article class="season-game-card"${gameAnchorId ? ` id="${escapeHtml(gameAnchorId)}"` : ""}>
            <div class="season-game-head">
              <div class="season-game-matchup">
                <div class="season-team-line">
                  <span class="season-team-code">${escapeHtml(awayAbbr)}</span>
                  <span class="season-team-name">${escapeHtml(game.away?.name || awayAbbr)}</span>
                </div>
                <div class="season-team-line">
                  <span class="season-team-code">${escapeHtml(homeAbbr)}</span>
                  <span class="season-team-name">${escapeHtml(game.home?.name || homeAbbr)}</span>
                </div>
                <div class="season-game-subcopy">${escapeHtml((game.starter_names || {}).away || "TBD")} vs ${escapeHtml((game.starter_names || {}).home || "TBD")}</div>
                ${gameTimeBits ? `<div class="season-game-time">${escapeHtml(gameTimeBits)}</div>` : ""}
              </div>
              <div class="season-scorebox">
                <div class="season-score-label">Actual final</div>
                <div class="season-score-main">${escapeHtml(actualScoreText(full, awayAbbr, homeAbbr))}</div>
                <div class="season-game-subcopy">Game ${escapeHtml(String(game.game_pk || ""))}</div>
              </div>
            </div>
            <div class="season-segments-grid">
              ${segmentCard("Full game", full, awayAbbr, homeAbbr)}
              ${segmentCard("First 5", first5, awayAbbr, homeAbbr)}
              ${segmentCard("First 3", first3, awayAbbr, homeAbbr)}
            </div>
            <div class="season-starters-grid">
              ${starterCard(awayAbbr + " starter", (game.starter_names || {}).away, (game.pitcher_props || {}).away)}
              ${starterCard(homeAbbr + " starter", (game.starter_names || {}).home, (game.pitcher_props || {}).home)}
            </div>
            ${renderGameBetting(game, awayAbbr, homeAbbr)}
            <div class="season-game-actions">
              ${gameCardsLink ? `<a class="cards-nav-pill" href="${escapeHtml(gameCardsLink)}">Open cards view</a>` : ""}
            </div>
          </article>`;
      })
      .join("");
  }

  function renderDay() {
    renderDaySummary();
    renderDayPicksRecap();
    renderGames();
    renderDays();
  }

  async function loadDay(dateStr) {
    if (!dateStr) return;
    state.selectedDate = String(dateStr);
    updateUrl(state.selectedDate);
    if (root.dayTitle) root.dayTitle.textContent = formatDateLong(state.selectedDate);
    if (root.dayMeta) root.dayMeta.textContent = "Loading day report...";
    if (root.dayPicks) root.dayPicks.innerHTML = '<div class="cards-loading-state">Loading day picks...</div>';
    if (root.liveLens) root.liveLens.innerHTML = '<div class="cards-loading-state">Loading live lens...</div>';
    if (root.games) root.games.innerHTML = '<div class="cards-loading-state">Loading games...</div>';
    try {
      state.day = await fetchJson(`/api/season/${encodeURIComponent(state.season)}/day/${encodeURIComponent(state.selectedDate)}?profile=${encodeURIComponent(state.bettingProfile)}`);
      renderDay();
      await loadLiveLens(state.selectedDate);
    } catch (error) {
      const message = error && error.message ? error.message : "Unknown error";
      if (root.dayMeta) root.dayMeta.textContent = `Failed to load ${state.selectedDate}.`;
      if (root.dayPicks) {
        root.dayPicks.innerHTML = `<div class="cards-empty-state season-error">Failed to load day picks.<div class="season-inline-note">${escapeHtml(message)}</div></div>`;
      }
      if (root.liveLens) {
        root.liveLens.innerHTML = `<div class="cards-empty-state season-error">Failed to load live lens.<div class="season-inline-note">${escapeHtml(message)}</div></div>`;
      }
      if (root.games) {
        root.games.innerHTML = `<div class="cards-empty-state season-error">Failed to load day report.<div class="season-inline-note">${escapeHtml(message)}</div></div>`;
      }
    }
  }

  function setMonthFilter(monthKey) {
    state.monthFilter = monthKey || "all";
    renderMonths();
    renderDays();
    const visible = filteredDays();
    if (!visible.length) return;
    if (!visible.some((row) => String(row?.date || "") === state.selectedDate)) {
      loadDay(String(visible[visible.length - 1]?.date || ""));
    }
  }

  async function loadManifest() {
    if (root.summary) root.summary.innerHTML = '<div class="cards-loading-state">Loading season recap...</div>';
    try {
      state.manifest = await fetchJson(`/api/season/${encodeURIComponent(state.season)}`);
      renderHeader();
      renderSummary();
      renderHitterCalibration();
      renderMonthlyBreakdown();
      const bettingCardsPromise = loadBettingCardsRecap();
      renderMonths();
      const days = Array.isArray(state.manifest?.days) ? state.manifest.days : [];
      if (!days.length) {
        await bettingCardsPromise;
        if (root.days) root.days.innerHTML = '<div class="season-empty-copy">No day reports were published for this season manifest.</div>';
        if (root.dayPicks) root.dayPicks.innerHTML = '<div class="season-empty-copy">No day picks recap is available.</div>';
        if (root.liveLens) root.liveLens.innerHTML = '<div class="season-empty-copy">No live-lens slate is available because the season manifest has no published days.</div>';
        if (root.games) root.games.innerHTML = '<div class="season-empty-copy">No game reports available.</div>';
        return;
      }
      if (!state.selectedDate || !days.some((row) => String(row?.date || "") === state.selectedDate)) {
        state.selectedDate = String(days[days.length - 1]?.date || "");
      }
      renderDays();
      await loadDay(state.selectedDate);
      await bettingCardsPromise;
    } catch (error) {
      const message = error && error.message ? error.message : "Unknown error";
      if (root.headerMeta) root.headerMeta.textContent = `Failed to load season ${state.season}.`;
      if (root.summary) {
        root.summary.innerHTML = `<div class="cards-empty-state season-error">Failed to load season recap.<div class="season-inline-note">${escapeHtml(message)}</div></div>`;
      }
      if (root.days) {
        root.days.innerHTML = '<div class="season-empty-copy">No season manifest found.</div>';
      }
      if (root.games) {
        root.games.innerHTML = '<div class="season-empty-copy">Publish a season manifest before opening this page.</div>';
      }
      if (root.liveLens) {
        root.liveLens.innerHTML = '<div class="season-empty-copy">Publish a season manifest before opening the season live-lens board.</div>';
      }
    }
  }

  if (root.months) {
    root.months.addEventListener("click", function (event) {
      const button = event.target.closest("[data-month-filter]");
      if (!button || !root.months.contains(button)) return;
      event.preventDefault();
      setMonthFilter(button.getAttribute("data-month-filter") || "all");
    });
  }

  if (root.days) {
    root.days.addEventListener("click", async function (event) {
      const pickButton = event.target.closest("[data-season-day-picks]");
      if (pickButton && root.days.contains(pickButton)) {
        event.preventDefault();
        await activateDayPicks(
          pickButton.getAttribute("data-season-date") || "",
          pickButton.getAttribute("data-season-day-picks") || "official"
        );
        return;
      }
      const button = event.target.closest("[data-season-date]");
      if (!button || !root.days.contains(button)) return;
      event.preventDefault();
      await loadDay(button.getAttribute("data-season-date") || "");
    });
  }

  if (root.dayMetrics) {
    root.dayMetrics.addEventListener("click", function (event) {
      const button = event.target.closest("[data-day-picks-filter]");
      if (!button || !root.dayMetrics.contains(button)) return;
      event.preventDefault();
      setDayPicksMode(String(button.getAttribute("data-day-picks-filter") || "official"), { scroll: true });
    });
  }

  if (root.dayPicks) {
    root.dayPicks.addEventListener("click", function (event) {
      const button = event.target.closest("[data-day-picks-filter]");
      if (!button || !root.dayPicks.contains(button)) return;
      event.preventDefault();
      setDayPicksMode(String(button.getAttribute("data-day-picks-filter") || "official"));
    });
  }

  if (root.bettingCards) {
    root.bettingCards.addEventListener("click", async function (event) {
      const button = event.target.closest("[data-betting-profile]");
      if (!button || !root.bettingCards.contains(button)) return;
      event.preventDefault();
      state.bettingProfile = String(button.getAttribute("data-betting-profile") || "");
      renderHeader();
      renderDays();
      renderBettingCardsRecap();
      if (state.selectedDate) {
        await loadDay(state.selectedDate);
      }
    });
  }

  loadManifest();
})();