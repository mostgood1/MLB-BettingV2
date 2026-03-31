(function () {
  const bootstrap = window.MLBBettingCardBootstrap || {};
  const state = {
    season: Number(bootstrap.season || 0),
    selectedDate: String(bootstrap.date || ""),
    profile: String(bootstrap.profile || "retuned"),
    monthFilter: "all",
    manifest: null,
    day: null,
  };

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

  const BETTING_PROP_LABELS = {
    strikeouts: "Strikeouts",
    outs: "Outs",
    earned_runs: "Earned runs",
    walks: "Walks",
    batters_faced: "Batters faced",
    pitches: "Pitches",
    hits: "Hits",
    home_runs: "HR",
    total_bases: "TB",
    rbis: "RBI",
    runs_scored: "Runs",
  };

  const BETTING_PROFILE_LABELS = {
    baseline: "Baseline recap",
    retuned: "Retuned live recap",
  };

  const root = {
    headerMeta: document.getElementById("bettingCardHeaderMeta"),
    statusPill: document.getElementById("bettingCardStatusPill"),
    profiles: document.getElementById("bettingCardProfiles"),
    summary: document.getElementById("bettingCardSummary"),
    months: document.getElementById("bettingCardMonths"),
    days: document.getElementById("bettingCardDays"),
    dayTitle: document.getElementById("bettingCardDayTitle"),
    dayMeta: document.getElementById("bettingCardDayMeta"),
    dayActions: document.getElementById("bettingCardDayActions"),
    dayMetrics: document.getElementById("bettingCardDayMetrics"),
    dayPicks: document.getElementById("bettingCardDayPicks"),
    games: document.getElementById("bettingCardGames"),
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

  function formatHalfInning(value) {
    const text = String(value || "").trim().toLowerCase();
    if (text === "top") return "Top";
    if (text === "bottom") return "Bot";
    return "";
  }

  function isLiveStatus(statusText) {
    const text = String(statusText || "").trim().toLowerCase();
    return text === "live" || text === "in progress" || text === "manager challenge";
  }

  function isFinalStatus(statusText) {
    return String(statusText || "").trim().toLowerCase() === "final";
  }

  function gameSortWeight(game) {
    const matchup = game?.matchup || {};
    const status = game?.status || {};
    const isLive = Boolean(matchup?.isLive) || isLiveStatus(status?.abstract);
    const isFinal = Boolean(matchup?.isFinal) || isFinalStatus(status?.abstract);
    if (isLive) return 0;
    if (isFinal) return 2;
    return 1;
  }

  function sortedGames(games) {
    return [...games].sort((left, right) => {
      const weightDelta = gameSortWeight(left) - gameSortWeight(right);
      if (weightDelta !== 0) return weightDelta;
      const dateDelta = String(left?.game_date || "").localeCompare(String(right?.game_date || ""));
      if (dateDelta !== 0) return dateDelta;
      const timeDelta = String(left?.start_time || "").localeCompare(String(right?.start_time || ""));
      if (timeDelta !== 0) return timeDelta;
      return Number(left?.game_pk || 0) - Number(right?.game_pk || 0);
    });
  }

  function compactGameStatus(game) {
    const matchup = game?.matchup || {};
    const status = game?.status || {};
    const detailed = String(matchup?.displayState || status?.detailed || status?.abstract || "").trim();
    if (matchup?.isLive) return detailed || "Live";
    if (matchup?.isFinal) return detailed || "Final";
    return detailed;
  }

  function compactLiveDetailBits(game) {
    const matchup = game?.matchup || {};
    if (!matchup?.isLive) return [];
    const bits = [];
    const half = formatHalfInning(matchup?.halfInning);
    const inning = Number(matchup?.inning || 0);
    if (half && inning > 0) bits.push(`${half} ${inning}`);
    else if (String(matchup?.liveText || "").trim()) bits.push(String(matchup.liveText).trim());

    const balls = matchup?.count?.balls;
    const strikes = matchup?.count?.strikes;
    if (balls != null && strikes != null) bits.push(`Count ${balls}-${strikes}`);

    const outs = matchup?.count?.outs;
    if (outs != null) bits.push(`${outs} out${Number(outs) === 1 ? "" : "s"}`);

    if (String(matchup?.batter || "").trim()) bits.push(`Batter ${String(matchup.batter).trim()}`);
    if (String(matchup?.pitcher || "").trim()) bits.push(`Pitcher ${String(matchup.pitcher).trim()}`);
    return bits;
  }

  function compactScoreText(game) {
    const matchup = game?.matchup || {};
    const score = game?.matchup?.score || {};
    const isVisibleState = Boolean(matchup?.isLive) || Boolean(matchup?.isFinal);
    const hasNonZeroScore = Number(score?.away || 0) !== 0 || Number(score?.home || 0) !== 0;
    if (!isVisibleState && !hasNonZeroScore) return "";
    if (score?.away == null && score?.home == null) return "";
    return `${game?.away?.abbr || "Away"} ${score?.away ?? "-"} - ${game?.home?.abbr || "Home"} ${score?.home ?? "-"}`;
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

  function updateUrl() {
    const url = new URL(window.location.href);
    if (state.selectedDate) url.searchParams.set("date", state.selectedDate);
    else url.searchParams.delete("date");
    if (state.profile) url.searchParams.set("profile", state.profile);
    else url.searchParams.delete("profile");
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

  function profileLabel(profileKey) {
    return BETTING_PROFILE_LABELS[profileKey] || profileKey;
  }

  function availableProfiles() {
    const profiles = Array.isArray(state.manifest?.available_profiles) ? state.manifest.available_profiles : [];
    return profiles.filter(Boolean);
  }

  function filteredDays() {
    const allDays = Array.isArray(state.manifest?.days) ? state.manifest.days : [];
    if (state.monthFilter === "all") return allDays;
    return allDays.filter((row) => String(row?.month || "") === state.monthFilter);
  }

  function bettingMetricLabel(reco) {
    const market = String(reco?.market || "").toLowerCase();
    const prop = String(reco?.prop || "").toLowerCase();
    if (BETTING_PROP_LABELS[prop]) return BETTING_PROP_LABELS[prop];
    if (market.includes("home_runs") || prop.includes("home_runs")) return "HR";
    if (market.includes("total_bases") || prop.includes("total_bases")) return "TB";
    if (market.includes("rbis") || prop.includes("rbi")) return "RBI";
    if (market.includes("hitter_runs") || prop.includes("runs_scored")) return "Runs";
    if (market.includes("hitter_hits") || prop.endsWith("hits")) return "Hits";
    return BETTING_MARKET_LABELS[market] || String(reco?.market_label || reco?.market || "Prop");
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
    const reasons = Array.isArray(reco?.reasons) ? reco.reasons : null;
    if (reasons && reasons.length) {
      const cleaned = reasons
        .map((row) => String(row == null ? "" : row).trim())
        .filter(Boolean);
      if (cleaned.length) return cleaned.slice(0, 3).join(" | ");
    }
    const reasonSummary = String(reco?.reason_summary || "").trim();
    if (reasonSummary) return reasonSummary;
    const market = String(reco?.market || "").toLowerCase();
    const bits = [];
    if (market === "totals" && toNumber(reco?.model_mean_total) != null) {
      bits.push(`Model total ${formatLine(reco?.model_mean_total)}`);
    }
    if (market === "pitcher_props") {
      if (toNumber(reco?.outs_mean) != null) bits.push(`Mean ${formatLine(reco?.outs_mean)} outs`);
      if (toNumber(reco?.so_mean) != null) bits.push(`Mean ${formatLine(reco?.so_mean)} strikeouts`);
    }
    const modelProb = bettingSelectedModelProb(reco);
    if (modelProb != null) bits.push(`Model ${formatPercent(modelProb, 1)}`);
    const marketProb = bettingSelectedMarketProb(reco);
    if (marketProb != null) bits.push(`Market ${formatPercent(marketProb, 1)}`);
    if (toNumber(reco?.edge) != null) bits.push(`Edge ${formatSignedPercentPoints(reco?.edge, 1)}`);
    return bits.join(" | ") || "Saved card recommendation";
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

  function seasonTicketPillStyle(tone) {
    const styles = [
      "display:inline-flex",
      "align-items:center",
      "justify-content:center",
      "min-height:28px",
      "padding:0 10px",
      "border-radius:999px",
      "border:1px solid rgba(132, 166, 196, 0.18)",
      "background:rgba(10, 21, 34, 0.88)",
      "color:var(--cards-text-soft)",
      "font-size:12px",
      "font-weight:700",
    ];
    if (tone === "is-win") {
      styles.push("border-color:rgba(49, 163, 84, 0.72)");
      styles.push("background:rgba(24, 99, 58, 0.95)");
      styles.push("color:#ecfff3");
      styles.push("box-shadow:inset 0 0 0 1px rgba(117, 255, 166, 0.18)");
    } else if (tone === "is-loss") {
      styles.push("border-color:rgba(198, 40, 40, 0.72)");
      styles.push("background:rgba(122, 22, 22, 0.95)");
      styles.push("color:#fff1f1");
      styles.push("box-shadow:inset 0 0 0 1px rgba(255, 132, 132, 0.16)");
    }
    return styles.join(";");
  }

  function renderHeader() {
    if (!root.headerMeta) return;
    const meta = state.manifest?.meta || {};
    const summary = state.manifest?.summary || {};
    const parts = [];
    parts.push(`${formatNumber((state.manifest?.days || []).length, 0)} official card days`);
    parts.push(`${formatNumber(summary?.settled_recommendations, 0)} settled official plays`);
    if (meta.first_date && meta.last_date) parts.push(`${meta.first_date} to ${meta.last_date}`);
    parts.push(profileLabel(state.profile));
    root.headerMeta.textContent = parts.join(" | ");
    if (root.statusPill) {
      root.statusPill.textContent = meta.partial ? "Partial" : "Complete";
      root.statusPill.classList.toggle("is-partial", Boolean(meta.partial));
    }
  }

  function renderProfiles() {
    if (!root.profiles) return;
    const profiles = availableProfiles();
    if (profiles.length <= 1) {
      root.profiles.innerHTML = "";
      return;
    }
    root.profiles.innerHTML = profiles.map((profile) => `
      <button
        type="button"
        class="cards-filter-pill ${profile === state.profile ? "is-active" : ""}"
        data-betting-card-profile="${escapeHtml(profile)}"
      >
        ${escapeHtml(profileLabel(profile))}
      </button>`).join("");
  }

  function renderSummary() {
    if (!root.summary) return;
    const summary = state.manifest?.summary || {};
    const combined = summary?.results?.combined || summary?.combined || {};
    const daily = summary?.daily || {};
    const selectedCounts = summary?.selected_counts || {};
    const bestDay = daily?.best_day || {};
    const worstDay = daily?.worst_day || {};
    root.summary.innerHTML = [
      metricCard("Card days", formatNumber(summary?.cards, 0), `${profileLabel(state.profile)} official-card dates`),
      metricCard("Official ROI", formatPercent(combined?.roi, 1), `${formatUnits(combined?.profit_u, 2)} on ${formatNumber(combined?.stake_u, 2)}u`),
      metricCard("Season profit", formatUnits(combined?.profit_u, 2), `${formatNumber(combined?.wins, 0)} wins | ${formatNumber(combined?.losses, 0)} losses`),
      metricCard("Settled bets", formatNumber(combined?.n, 0), `${formatNumber(summary?.unresolved_recommendations, 0)} unresolved`),
      metricCard("Daily mean", formatUnits(daily?.mean_u, 2), `Median ${formatUnits(daily?.median_u, 2)}`),
      metricCard("Best day", formatUnits(bestDay?.profit_u, 2), String(bestDay?.date || "-")),
      metricCard("Worst day", formatUnits(worstDay?.profit_u, 2), String(worstDay?.date || "-")),
      metricCard("Selection mix", formatNumber(selectedCounts?.combined, 0), `Tot ${selectedCounts?.totals ?? 0} | ML ${selectedCounts?.ml ?? 0} | P ${selectedCounts?.pitcher_props ?? 0} | H ${selectedCounts?.hitter_props ?? 0}`),
    ].join("");
  }

  function renderMonths() {
    if (!root.months) return;
    const months = Array.isArray(state.manifest?.months) ? state.manifest.months : [];
    const allDays = Array.isArray(state.manifest?.days) ? state.manifest.days : [];
    const options = [{ key: "all", label: `All ${allDays.length}` }].concat(
      months.map((row) => ({
        key: String(row?.month || ""),
        label: `${monthLabel(String(row?.month || ""))} ${row?.days ?? 0}`,
      }))
    );
    root.months.innerHTML = options.map((option) => `
      <button
        type="button"
        class="cards-filter-pill ${option.key === state.monthFilter ? "is-active" : ""}"
        data-betting-card-month="${escapeHtml(option.key)}"
      >
        ${escapeHtml(option.label)}
      </button>`).join("");
  }

  function renderDays() {
    if (!root.days) return;
    const days = filteredDays();
    if (!days.length) {
      root.days.innerHTML = '<div class="season-empty-copy">No official card days match the current month filter.</div>';
      return;
    }
    root.days.innerHTML = days.map((day) => {
      const isActive = String(day?.date || "") === state.selectedDate;
      const counts = day?.selected_counts || {};
      const combined = (day?.results || {}).combined || {};
      const unresolved = Number(day?.unresolved_n || 0);
      const badge = unresolved > 0
        ? `<span class="season-day-pill is-empty">${escapeHtml(formatNumber(unresolved, 0))} unresolved</span>`
        : `<span class="season-day-pill is-official">${escapeHtml(formatNumber(counts?.combined, 0))} picks</span>`;
      return `
        <article class="season-day-entry">
          <button
            type="button"
            class="season-day-button ${isActive ? "is-active" : ""}"
            data-betting-card-date="${escapeHtml(String(day?.date || ""))}"
          >
            <div class="season-day-row">
              <div class="season-day-primary">${escapeHtml(String(day?.date || ""))}</div>
              <span class="cards-chip">${escapeHtml(formatUnits(combined?.profit_u, 2))}</span>
            </div>
            <div class="season-day-secondary">ROI ${escapeHtml(formatPercent(combined?.roi, 1))} | ${escapeHtml(formatNumber(combined?.n, 0))} settled</div>
          </button>
          <div class="season-day-badges">${badge}</div>
        </article>`;
    }).join("");
  }

  function dayOfficialItems() {
    const games = Array.isArray(state.day?.games) ? state.day.games : [];
    const out = [];
    games.forEach((game, gameIndex) => {
      const awayAbbr = game?.away?.abbr || "Away";
      const homeAbbr = game?.home?.abbr || "Home";
      bettingOfficialRows(game?.betting || {}).forEach((reco, recoIndex) => {
        out.push({ game, reco, awayAbbr, homeAbbr, sortKey: `${gameIndex}-${recoIndex}` });
      });
    });
    return out;
  }

  function dayPickRowsMarkup(items) {
    return items.map((item) => {
      const game = item.game || {};
      const reco = item.reco || {};
      const settlement = reco?.settlement || null;
      const tone = bettingResultTone(reco);
      const statusText = bettingResultLabel(reco);
      const profitText = settlement ? formatUnits(settlement.profit_u, 2) : "-";
      const actualText = settlement && settlement.actual != null ? `Actual ${formatLine(settlement.actual)}` : "Settlement unavailable";
      const gameLabel = `${item.awayAbbr} @ ${item.homeAbbr}`;
      const gameMeta = [
        game?.start_time ? `First pitch ${game.start_time}` : "",
        String(game?.status?.abstract || "").trim(),
      ].filter(Boolean).join(" | ");
      return `
        <tr>
          <td>
            <div class="season-betting-cell-main">${escapeHtml(gameLabel)}</div>
            <div class="season-betting-cell-sub">${escapeHtml(gameMeta || `Game ${String(game.game_pk || "-")}`)}</div>
          </td>
          <td>
            <div class="season-betting-cell-main">${escapeHtml(BETTING_MARKET_LABELS[String(reco?.market || "").toLowerCase()] || bettingMetricLabel(reco))}</div>
            <div class="season-betting-cell-sub">Official card</div>
          </td>
          <td>
            <div class="season-betting-cell-main">${escapeHtml(bettingSelectionLabel(reco, item.awayAbbr, item.homeAbbr))}</div>
            <div class="season-betting-cell-sub">${escapeHtml(bettingDetailText(reco))}</div>
          </td>
          <td>${escapeHtml(formatOdds(reco?.odds))}</td>
          <td>${escapeHtml(formatSignedPercentPoints(reco?.edge, 1))}</td>
          <td><span class="season-ticket-pill ${tone}" style="${seasonTicketPillStyle(tone)}">${escapeHtml(statusText)}</span></td>
          <td>
            <div class="season-betting-cell-main">${escapeHtml(profitText)}</div>
            <div class="season-betting-cell-sub">${escapeHtml(actualText)}</div>
          </td>
        </tr>`;
    }).join("");
  }

  function dayPickCardsMarkup(items) {
    return items.map((item) => {
      const game = item.game || {};
      const reco = item.reco || {};
      const settlement = reco?.settlement || null;
      const tone = bettingResultTone(reco);
      const statusText = bettingResultLabel(reco);
      const profitText = settlement ? formatUnits(settlement.profit_u, 2) : "-";
      const actualText = settlement && settlement.actual != null ? `Actual ${formatLine(settlement.actual)}` : "Settlement unavailable";
      const gameLabel = `${item.awayAbbr} @ ${item.homeAbbr}`;
      const gameMeta = [
        game?.start_time ? `First pitch ${game.start_time}` : "",
        String(game?.status?.abstract || "").trim(),
      ].filter(Boolean).join(" | ");
      return `
        <article class="betting-card-mobile-entry">
          <div class="betting-card-mobile-head">
            <div>
              <div class="betting-card-mobile-title">${escapeHtml(gameLabel)}</div>
              <div class="season-inline-note">${escapeHtml(gameMeta || `Game ${String(game.game_pk || "-")}`)}</div>
            </div>
            <span class="season-ticket-pill ${tone}" style="${seasonTicketPillStyle(tone)}">${escapeHtml(statusText)}</span>
          </div>
          <div class="betting-card-mobile-grid">
            <div class="betting-card-mobile-stat">
              <div class="betting-card-mobile-label">Market</div>
              <div class="betting-card-mobile-value">${escapeHtml(BETTING_MARKET_LABELS[String(reco?.market || "").toLowerCase()] || bettingMetricLabel(reco))}</div>
            </div>
            <div class="betting-card-mobile-stat">
              <div class="betting-card-mobile-label">Odds</div>
              <div class="betting-card-mobile-value">${escapeHtml(formatOdds(reco?.odds))}</div>
            </div>
            <div class="betting-card-mobile-stat">
              <div class="betting-card-mobile-label">Edge</div>
              <div class="betting-card-mobile-value">${escapeHtml(formatSignedPercentPoints(reco?.edge, 1))}</div>
            </div>
            <div class="betting-card-mobile-stat">
              <div class="betting-card-mobile-label">Profit</div>
              <div class="betting-card-mobile-value">${escapeHtml(profitText)}</div>
            </div>
          </div>
          <div class="betting-card-mobile-pick">${escapeHtml(bettingSelectionLabel(reco, item.awayAbbr, item.homeAbbr))}</div>
          <div class="season-inline-note">${escapeHtml(bettingDetailText(reco))}</div>
          <div class="season-inline-note">${escapeHtml(actualText)}</div>
        </article>`;
    }).join("");
  }

  function renderDayPicks() {
    if (!root.dayPicks) return;
    if (!state.day) {
      root.dayPicks.innerHTML = '<div class="season-empty-copy">Pick an official-card date to inspect the day-level recap.</div>';
      return;
    }
    const items = dayOfficialItems();
    const profileText = profileLabel(state.day?.profile || state.profile);
    root.dayPicks.innerHTML = `
      <div class="season-panel-head">
        <div>
          <div class="season-kicker">Selected day board</div>
          <div class="season-panel-title">Official picks by date</div>
        </div>
      </div>
      <div class="season-inline-note">${escapeHtml(`${formatNumber(items.length, 0)} official betting-card picks across the selected date under ${profileText}.`)}</div>
      <div class="season-inline-note">Only official promoted plays are shown here. Playable extras are intentionally omitted from this simplified view.</div>
      ${items.length ? `
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
            <tbody>${dayPickRowsMarkup(items)}</tbody>
          </table>
        </div>
        <div class="betting-card-mobile-list">${dayPickCardsMarkup(items)}</div>` : '<div class="season-empty-copy">No official plays were logged for this date.</div>'}
    `;
  }

  function renderDaySummary() {
    if (!root.dayTitle || !root.dayMeta || !root.dayActions || !root.dayMetrics) return;
    if (!state.day) {
      root.dayTitle.textContent = "No day selected";
      root.dayMeta.textContent = "Pick an official-card date from the rail to load the day recap.";
      root.dayActions.innerHTML = "";
      root.dayMetrics.innerHTML = '<div class="season-empty-copy">No official card metrics available.</div>';
      return;
    }

    const combined = (state.day?.results || {}).combined || {};
    const counts = state.day?.selected_counts || {};
    const games = Array.isArray(state.day?.games) ? state.day.games : [];
    root.dayTitle.textContent = formatDateLong(state.day.date);
    root.dayMeta.textContent = [
      `${games.length} games`,
      `${formatNumber(counts?.combined, 0)} official picks`,
      profileLabel(state.day?.profile || state.profile),
      String(state.day?.source_kind || "season_manifest"),
    ].join(" | ");
    root.dayActions.innerHTML = `
      <a class="cards-nav-pill" href="/season/${encodeURIComponent(state.season)}?date=${encodeURIComponent(state.day.date)}">Full season recap</a>
      ${state.day?.cards_url ? `<a class="cards-nav-pill" href="${escapeHtml(state.day.cards_url)}">Open daily cards</a>` : ""}`;
    root.dayMetrics.innerHTML = [
      metricCard("Games", formatNumber(games.length, 0), "Matchups with official-card action"),
      metricCard("Official bets", formatNumber(counts?.combined, 0), `Tot ${counts?.totals ?? 0} | ML ${counts?.ml ?? 0} | P ${counts?.pitcher_props ?? 0} | H ${counts?.hitter_props ?? 0}`),
      metricCard("Profit", formatUnits(combined?.profit_u, 2), `${formatNumber(combined?.wins, 0)} wins | ${formatNumber(combined?.losses, 0)} losses`),
      metricCard("ROI", formatPercent(combined?.roi, 1), `${formatNumber(combined?.stake_u, 2)}u staked`),
      metricCard("Settled", formatNumber(combined?.n, 0), `${formatNumber(state.day?.summary?.unresolved_n, 0)} unresolved`),
      metricCard("Cap profile", String(state.day?.cap_profile || "-"), "Official locked-policy card only"),
    ].join("");
  }

  function renderGameRows(rows, awayAbbr, homeAbbr) {
    return rows.map((reco) => {
      const tone = bettingResultTone(reco);
      const settlement = reco?.settlement || null;
      const profitText = settlement ? formatUnits(settlement.profit_u, 2) : "-";
      const actualText = settlement && settlement.actual != null ? `Actual ${formatLine(settlement.actual)}` : "Settlement unavailable";
      return `
        <tr>
          <td>
            <div class="season-betting-cell-main">${escapeHtml(BETTING_MARKET_LABELS[String(reco?.market || "").toLowerCase()] || bettingMetricLabel(reco))}</div>
            <div class="season-betting-cell-sub">Official card</div>
          </td>
          <td>
            <div class="season-betting-cell-main">${escapeHtml(bettingSelectionLabel(reco, awayAbbr, homeAbbr))}</div>
            <div class="season-betting-cell-sub">${escapeHtml(bettingDetailText(reco))}</div>
          </td>
          <td>${escapeHtml(formatOdds(reco?.odds))}</td>
          <td>${escapeHtml(formatSignedPercentPoints(reco?.edge, 1))}</td>
          <td><span class="season-ticket-pill ${tone}" style="${seasonTicketPillStyle(tone)}">${escapeHtml(bettingResultLabel(reco))}</span></td>
          <td>
            <div class="season-betting-cell-main">${escapeHtml(profitText)}</div>
            <div class="season-betting-cell-sub">${escapeHtml(actualText)}</div>
          </td>
        </tr>`;
    }).join("");
  }

  function renderGameRowCards(rows, awayAbbr, homeAbbr) {
    return rows.map((reco) => {
      const tone = bettingResultTone(reco);
      const settlement = reco?.settlement || null;
      const profitText = settlement ? formatUnits(settlement.profit_u, 2) : "-";
      const actualText = settlement && settlement.actual != null ? `Actual ${formatLine(settlement.actual)}` : "Settlement unavailable";
      return `
        <article class="betting-card-mobile-entry">
          <div class="betting-card-mobile-head">
            <div>
              <div class="betting-card-mobile-title">${escapeHtml(BETTING_MARKET_LABELS[String(reco?.market || "").toLowerCase()] || bettingMetricLabel(reco))}</div>
              <div class="season-inline-note">Official card</div>
            </div>
            <span class="season-ticket-pill ${tone}" style="${seasonTicketPillStyle(tone)}">${escapeHtml(bettingResultLabel(reco))}</span>
          </div>
          <div class="betting-card-mobile-grid">
            <div class="betting-card-mobile-stat">
              <div class="betting-card-mobile-label">Pick</div>
              <div class="betting-card-mobile-value">${escapeHtml(bettingSelectionLabel(reco, awayAbbr, homeAbbr))}</div>
            </div>
            <div class="betting-card-mobile-stat">
              <div class="betting-card-mobile-label">Odds</div>
              <div class="betting-card-mobile-value">${escapeHtml(formatOdds(reco?.odds))}</div>
            </div>
            <div class="betting-card-mobile-stat">
              <div class="betting-card-mobile-label">Edge</div>
              <div class="betting-card-mobile-value">${escapeHtml(formatSignedPercentPoints(reco?.edge, 1))}</div>
            </div>
            <div class="betting-card-mobile-stat">
              <div class="betting-card-mobile-label">Profit</div>
              <div class="betting-card-mobile-value">${escapeHtml(profitText)}</div>
            </div>
          </div>
          <div class="season-inline-note">${escapeHtml(bettingDetailText(reco))}</div>
          <div class="season-inline-note">${escapeHtml(actualText)}</div>
        </article>`;
    }).join("");
  }

  function renderGames() {
    if (!root.games) return;
    if (!state.day) {
      root.games.innerHTML = '<div class="season-empty-copy">No official card games loaded.</div>';
      return;
    }
    const games = Array.isArray(state.day?.games) ? sortedGames(state.day.games) : [];
    if (!games.length) {
      root.games.innerHTML = '<div class="season-empty-copy">No official-card games were found for this date.</div>';
      return;
    }
    root.games.innerHTML = games.map((game) => {
      const awayAbbr = game?.away?.abbr || "Away";
      const homeAbbr = game?.home?.abbr || "Home";
      const officialRows = bettingOfficialRows(game?.betting || {});
      const combined = ((game?.betting || {}).results || {}).combined || {};
      const compactStatus = compactGameStatus(game);
      const scoreText = compactScoreText(game);
      const liveDetailBits = compactLiveDetailBits(game);
      const gameTimeBits = [
        game?.start_time ? `First pitch ${game.start_time}` : "",
        compactStatus,
      ].filter(Boolean).join(" | ");
      return `
        <article class="season-game-card">
          <div class="season-game-head">
            <div class="season-game-matchup">
              <div class="season-team-line">
                <span class="season-team-code">${escapeHtml(awayAbbr)}</span>
                <span class="season-team-name">${escapeHtml(game?.away?.name || awayAbbr)}</span>
              </div>
              <div class="season-team-line">
                <span class="season-team-code">${escapeHtml(homeAbbr)}</span>
                <span class="season-team-name">${escapeHtml(game?.home?.name || homeAbbr)}</span>
              </div>
              <div class="season-game-subcopy">${escapeHtml(String(game?.starter_names?.away || "TBD"))} vs ${escapeHtml(String(game?.starter_names?.home || "TBD"))}</div>
              ${gameTimeBits ? `<div class="season-game-time">${escapeHtml(gameTimeBits)}</div>` : ""}
              ${scoreText ? `<div class="season-game-scoreline">${escapeHtml(scoreText)}</div>` : ""}
              ${liveDetailBits.length ? `<div class="season-game-live-grid">${liveDetailBits.map((bit) => `<span class="season-game-live-pill">${escapeHtml(bit)}</span>`).join("")}</div>` : ""}
            </div>
            <div class="season-scorebox">
              <div class="season-score-label">Official card</div>
              <div class="season-score-main">${escapeHtml(formatUnits(combined?.profit_u, 2))}</div>
              <div class="season-game-subcopy">ROI ${escapeHtml(formatPercent(combined?.roi, 1))} | ${escapeHtml(formatNumber(officialRows.length, 0))} picks</div>
            </div>
          </div>
          <section class="season-game-betting-shell">
            <div class="season-stat-grid season-game-betting-stats">
              ${metricCard("Picks", formatNumber(officialRows.length, 0), "Official card only")}
              ${metricCard("Profit", formatUnits(combined?.profit_u, 2), `${formatNumber(combined?.wins, 0)} wins | ${formatNumber(combined?.losses, 0)} losses`)}
              ${metricCard("ROI", formatPercent(combined?.roi, 1), `${formatNumber(combined?.stake_u, 2)}u staked`)}
              ${metricCard("Settled", formatNumber(combined?.n, 0), `Game ${formatNumber(game?.game_pk, 0)}`)}
            </div>
            <section class="season-breakdown-card season-game-betting-card">
              <div class="season-breakdown-title">Official card</div>
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
                  <tbody>${renderGameRows(officialRows, awayAbbr, homeAbbr)}</tbody>
                </table>
              </div>
              <div class="betting-card-mobile-list">${renderGameRowCards(officialRows, awayAbbr, homeAbbr)}</div>
            </section>
          </section>
        </article>`;
    }).join("");
  }

  function renderDay() {
    renderDaySummary();
    renderDayPicks();
    renderGames();
    renderDays();
  }

  async function loadDay(dateStr) {
    if (!dateStr) return;
    state.selectedDate = String(dateStr);
    updateUrl();
    if (root.dayMeta) root.dayMeta.textContent = "Loading official card detail...";
    if (root.dayMetrics) root.dayMetrics.innerHTML = '<div class="cards-loading-state">Loading day metrics...</div>';
    if (root.dayPicks) root.dayPicks.innerHTML = '<div class="cards-loading-state">Loading official picks...</div>';
    if (root.games) root.games.innerHTML = '<div class="cards-loading-state">Loading official card games...</div>';
    try {
      state.day = await fetchJson(`/api/season/${encodeURIComponent(state.season)}/betting-card/day/${encodeURIComponent(state.selectedDate)}?profile=${encodeURIComponent(state.profile)}`);
      renderDay();
    } catch (error) {
      const message = error && error.message ? error.message : "Unknown error";
      if (root.dayMeta) root.dayMeta.textContent = `Failed to load ${state.selectedDate}.`;
      if (root.dayMetrics) root.dayMetrics.innerHTML = `<div class="cards-empty-state season-error">Failed to load day metrics.<div class="season-inline-note">${escapeHtml(message)}</div></div>`;
      if (root.dayPicks) root.dayPicks.innerHTML = `<div class="cards-empty-state season-error">Failed to load official picks.<div class="season-inline-note">${escapeHtml(message)}</div></div>`;
      if (root.games) root.games.innerHTML = `<div class="cards-empty-state season-error">Failed to load official games.<div class="season-inline-note">${escapeHtml(message)}</div></div>`;
    }
  }

  async function loadManifest() {
    if (root.summary) root.summary.innerHTML = '<div class="cards-loading-state">Loading official betting-card recap...</div>';
    try {
      state.manifest = await fetchJson(`/api/season/${encodeURIComponent(state.season)}/betting-card?profile=${encodeURIComponent(state.profile)}`);
      state.profile = String(state.manifest?.profile || state.profile || "retuned");
      renderHeader();
      renderProfiles();
      renderSummary();
      renderMonths();
      const days = Array.isArray(state.manifest?.days) ? state.manifest.days : [];
      if (!days.length) {
        if (root.days) root.days.innerHTML = '<div class="season-empty-copy">No official betting-card days are available for this profile.</div>';
        if (root.dayMetrics) root.dayMetrics.innerHTML = '<div class="season-empty-copy">No official-card dates are available to inspect.</div>';
        if (root.dayPicks) root.dayPicks.innerHTML = '<div class="season-empty-copy">No official picks recap is available.</div>';
        if (root.games) root.games.innerHTML = '<div class="season-empty-copy">No official-card games available.</div>';
        return;
      }
      if (!state.selectedDate || !days.some((row) => String(row?.date || "") === state.selectedDate)) {
        state.selectedDate = String(days[days.length - 1]?.date || "");
      }
      renderDays();
      await loadDay(state.selectedDate);
    } catch (error) {
      const message = error && error.message ? error.message : "Unknown error";
      if (root.headerMeta) root.headerMeta.textContent = `Failed to load season ${state.season} official betting-card recap.`;
      if (root.summary) root.summary.innerHTML = `<div class="cards-empty-state season-error">Failed to load official betting-card recap.<div class="season-inline-note">${escapeHtml(message)}</div></div>`;
      if (root.days) root.days.innerHTML = '<div class="season-empty-copy">No official betting-card manifest found.</div>';
      if (root.dayPicks) root.dayPicks.innerHTML = '<div class="season-empty-copy">No official picks recap available.</div>';
      if (root.games) root.games.innerHTML = '<div class="season-empty-copy">No official-card games available.</div>';
    }
  }

  if (root.profiles) {
    root.profiles.addEventListener("click", async function (event) {
      const button = event.target.closest("[data-betting-card-profile]");
      if (!button || !root.profiles.contains(button)) return;
      event.preventDefault();
      state.profile = String(button.getAttribute("data-betting-card-profile") || state.profile);
      await loadManifest();
    });
  }

  if (root.months) {
    root.months.addEventListener("click", function (event) {
      const button = event.target.closest("[data-betting-card-month]");
      if (!button || !root.months.contains(button)) return;
      event.preventDefault();
      state.monthFilter = String(button.getAttribute("data-betting-card-month") || "all");
      renderMonths();
      renderDays();
      const visible = filteredDays();
      if (visible.length && !visible.some((row) => String(row?.date || "") === state.selectedDate)) {
        loadDay(String(visible[visible.length - 1]?.date || ""));
      }
    });
  }

  if (root.days) {
    root.days.addEventListener("click", async function (event) {
      const button = event.target.closest("[data-betting-card-date]");
      if (!button || !root.days.contains(button)) return;
      event.preventDefault();
      await loadDay(String(button.getAttribute("data-betting-card-date") || ""));
    });
  }

  loadManifest();
})();