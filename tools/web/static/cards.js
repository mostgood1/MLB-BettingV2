(function () {
  const bootstrap = window.MLBCardsBootstrap || {};
  const state = {
    date: String(bootstrap.date || ""),
    payload: null,
    cards: [],
    filter: "all",
    cardNodes: new Map(),
    stripNodes: new Map(),
    details: new Map(),
    livePollers: new Map(),
    autoRefreshHandle: null,
    loadingCards: false,
  };

  const AUTO_REFRESH_MS = 15000;

  const root = {
    headerMeta: document.getElementById("cardsHeaderMeta"),
    dateBadge: document.getElementById("cardsDateBadge"),
    dateInput: document.getElementById("cardsDateInput"),
    prevDateLink: document.getElementById("cardsPrevDateLink"),
    nextDateLink: document.getElementById("cardsNextDateLink"),
    sourceMeta: document.getElementById("cardsSourceMeta"),
    filters: document.getElementById("cardsFilters"),
    scoreboard: document.getElementById("cardsScoreboard"),
    grid: document.getElementById("cardsGrid"),
  };

  const ACTUAL_BATTING_COLUMNS = ["name", "pos", "AB", "R", "H", "RBI", "BB", "SO", "HR", "TB"];
  const ACTUAL_PITCHING_COLUMNS = ["name", "IP", "H", "R", "ER", "BB", "SO", "HR", "BF", "P"];
  const SIM_BATTING_COLUMNS = ["name", "pos", "PA", "AB", "H", "R", "RBI", "BB", "SO", "HR", "TB"];
  const SIM_PITCHING_COLUMNS = ["name", "IP", "H", "R", "BB", "SO", "HR", "BF", "P"];
  const AGGREGATE_SIM_BATTING_COLUMNS = ["name", "pos", "AB", "H", "R", "RBI", "BB", "SO", "HR", "TB"];
  const AGGREGATE_SIM_PITCHING_COLUMNS = ["name", "IP", "H", "R", "BB", "SO", "HR", "BF", "P"];

  function simBattingColumns(sim) {
    return sim?.boxscoreMode === "aggregate" ? AGGREGATE_SIM_BATTING_COLUMNS : SIM_BATTING_COLUMNS;
  }

  function simPitchingColumns(sim) {
    return sim?.boxscoreMode === "aggregate" ? AGGREGATE_SIM_PITCHING_COLUMNS : SIM_PITCHING_COLUMNS;
  }

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

  function formatLine(value) {
    const num = toNumber(value);
    if (num == null) return "-";
    return Number.isInteger(num) ? String(num) : num.toFixed(1);
  }

  function formatOdds(value) {
    const text = String(value || "").trim();
    return text || "-";
  }

  function normalizedAmericanOdds(value) {
    const num = Number(value);
    return Number.isFinite(num) ? Math.round(num) : null;
  }

  function propOddsAllowed(reco, maxFavoriteOdds = -200) {
    const odds = normalizedAmericanOdds(reco?.odds);
    if (odds == null) return true;
    if (odds >= 0) return true;
    return odds >= maxFavoriteOdds;
  }

  function filterPropRowsByOdds(rows, maxFavoriteOdds = -200) {
    return (Array.isArray(rows) ? rows : []).filter((row) => propOddsAllowed(row, maxFavoriteOdds));
  }

  function isResolvedLiveProp(row) {
    const actual = toNumber(row?.actual ?? row?.actual_value);
    const line = toNumber(row?.market_line);
    if (actual == null || line == null) return false;
    return actual > line + 1e-9;
  }

  function americanOddsImpliedProb(value) {
    const text = String(value || "").trim();
    if (!text) return null;
    const odds = Number(text);
    if (!Number.isFinite(odds) || odds === 0) return null;
    if (odds > 0) return 100 / (odds + 100);
    return Math.abs(odds) / (Math.abs(odds) + 100);
  }

  function normalizeTwoWay(firstProb, secondProb) {
    const first = toNumber(firstProb);
    const second = toNumber(secondProb);
    if (first == null || second == null) return { first: null, second: null };
    const denom = first + second;
    if (!(denom > 0)) return { first: null, second: null };
    return { first: first / denom, second: second / denom };
  }

  function logisticWinProb(homeMargin) {
    const margin = toNumber(homeMargin);
    if (margin == null) return null;
    return 1 / (1 + Math.exp(-0.65 * margin));
  }

  function formatTimestampShort(value) {
    const text = String(value || '').trim();
    if (!text) return '-';
    const parsed = new Date(text);
    if (Number.isNaN(parsed.getTime())) return text;
    return new Intl.DateTimeFormat(undefined, {
      month: 'numeric',
      day: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
    }).format(parsed);
  }

  function shiftIsoDate(value, days) {
    const text = String(value || "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) return "";
    const shifted = new Date(`${text}T00:00:00Z`);
    if (Number.isNaN(shifted.getTime())) return "";
    shifted.setUTCDate(shifted.getUTCDate() + Number(days || 0));
    return shifted.toISOString().slice(0, 10);
  }

  function hasPredictionBlock(value) {
    return !!(value && typeof value === "object" && Object.keys(value).length);
  }

  function normalizeName(value) {
    return String(value || "")
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
  }

  function parseIpToOuts(value) {
    const text = String(value || "").trim();
    if (!text) return null;
    const parts = text.split(".");
    const whole = Number(parts[0]);
    const frac = Number(parts[1] || 0);
    if (!Number.isFinite(whole) || !Number.isFinite(frac)) return null;
    return (whole * 3) + frac;
  }

  function statusClass(statusText) {
    const text = String(statusText || "").toLowerCase();
    if (text.includes("live") || text.includes("in progress")) return "is-live";
    if (text.includes("final")) return "is-final";
    return "";
  }

  function marketRows(card, key) {
    return Array.isArray(card?.markets?.[key]) ? card.markets[key] : [];
  }

  function trackedGameLines(card) {
    return card?.trackedGameLines && typeof card.trackedGameLines === "object" ? card.trackedGameLines : {};
  }

  function extraMarketRows(card, key) {
    return Array.isArray(card?.markets?.[key]) ? card.markets[key] : [];
  }

  function propSortCompare(left, right) {
    const edgeDelta = (toNumber(right?.edge) || -999) - (toNumber(left?.edge) || -999);
    if (edgeDelta !== 0) return edgeDelta;
    return (toNumber(left?.rank) || 999) - (toNumber(right?.rank) || 999);
  }

  function officialPropRows(card) {
    return filterPropRowsByOdds(marketRows(card, "pitcherProps")
      .concat(marketRows(card, "hitterProps")))
      .slice()
      .sort(propSortCompare);
  }

  function extraPropRows(card) {
    return filterPropRowsByOdds(extraMarketRows(card, "extraPitcherProps")
      .concat(extraMarketRows(card, "extraHitterProps")))
      .slice()
      .sort(propSortCompare);
  }

  function allPropRows(card) {
    return officialPropRows(card).concat(extraPropRows(card));
  }

  function livePropRows(detail) {
    const rows = filterPropRowsByOdds(Array.isArray(detail?.sim?.livePropRows) ? detail.sim.livePropRows : []);
    const isFinal = gameProgress(detail?.snapshot, null).isFinal;
    return isFinal ? rows : rows.filter((row) => !isResolvedLiveProp(row));
  }

  function hasLivePropPayload(detail) {
    return Array.isArray(detail?.sim?.livePropRows);
  }

  function hasAnyProps(card) {
    return allPropRows(card).length > 0;
  }

  function hasAnyRecs(card) {
    return !!(card?.flags?.hasAnyRecommendations);
  }

  function cardStatus(card) {
    return String(card?.status?.abstract || "").trim();
  }

  function matchesFilter(card, filterKey) {
    if (filterKey === "official") return hasAnyRecs(card);
    if (filterKey === "props") return hasAnyProps(card);
    if (filterKey === "live") return cardStatus(card).toLowerCase() === "live";
    if (filterKey === "final") return cardStatus(card).toLowerCase() === "final";
    return true;
  }

  function buildFilters(cards) {
    const liveCount = cards.filter((card) => matchesFilter(card, "live")).length;
    const finalCount = cards.filter((card) => matchesFilter(card, "final")).length;
    const officialCount = cards.filter((card) => matchesFilter(card, "official")).length;
    const propsCount = cards.filter((card) => matchesFilter(card, "props")).length;
    return [
      { key: "all", label: `All ${cards.length}` },
      { key: "official", label: `Official ${officialCount}` },
      { key: "props", label: `Props ${propsCount}` },
      { key: "live", label: `Live ${liveCount}` },
      { key: "final", label: `Final ${finalCount}` },
    ];
  }

  function slateCounts(cards) {
    const liveCount = cards.filter((card) => matchesFilter(card, "live")).length;
    const finalCount = cards.filter((card) => matchesFilter(card, "final")).length;
    const officialCount = cards.filter((card) => matchesFilter(card, "official")).length;
    const simCount = cards.filter((card) => (
      hasPredictionBlock(card?.predictions?.full)
      || hasPredictionBlock(card?.predictions?.first5)
      || hasPredictionBlock(card?.predictions?.first3)
    )).length;
    return {
      liveCount,
      finalCount,
      officialCount,
      simCount,
      upcomingCount: Math.max(cards.length - liveCount - finalCount, 0),
    };
  }

  function sourceMetaPill(label, variant = "") {
    const classes = ["cards-source-meta-pill"];
    if (variant) classes.push(`is-${variant}`);
    return `<span class="${classes.join(" ")}">${escapeHtml(String(label || ""))}</span>`;
  }

  function marketCountSummary(card) {
    const parts = [];
    if (card?.markets?.totals) parts.push("TOTAL");
    if (card?.markets?.ml) parts.push("ML");
    if (marketRows(card, "pitcherProps").length) parts.push(`${marketRows(card, "pitcherProps").length} P`);
    if (marketRows(card, "hitterProps").length) parts.push(`${marketRows(card, "hitterProps").length} H`);
    if (extraPropRows(card).length) parts.push(`+${extraPropRows(card).length} playable`);
    return parts.join(" · ") || "No market snapshot";
  }

  function starterText(card) {
    const awayStarter = card?.probable?.away?.fullName || "TBD";
    const homeStarter = card?.probable?.home?.fullName || "TBD";
    return `${awayStarter} vs ${homeStarter}`;
  }

  function ensureDetail(card) {
    const gamePk = Number(card.gamePk);
    if (!state.details.has(gamePk)) {
      const initialProp = officialPropRows(card)[0] || allPropRows(card)[0] || null;
      state.details.set(gamePk, {
        snapshot: null,
        sim: null,
        activeTab: "overview",
        selectedPropKey: initialProp ? propKey(initialProp) : null,
        propFilters: { board: "auto", side: "all", type: "all" },
      });
    }
    return state.details.get(gamePk);
  }

  function propKey(reco) {
    return [
      reco?.market || "",
      reco?.player_name || reco?.pitcher_name || "",
      reco?.team || reco?.team_side || "",
      reco?.selection || "",
      reco?.market_line || "",
      reco?.rank || "",
    ].join("|");
  }

  function metricLabel(reco) {
    const market = String(reco?.market || "").toLowerCase();
    const prop = String(reco?.prop || "").toLowerCase();
    if (market.includes("home_runs") || prop.includes("home_runs")) return "HR";
    if (market.includes("total_bases") || prop.includes("total_bases")) return "TB";
    if (market.includes("rbis") || prop.includes("rbi")) return "RBI";
    if (market.includes("hitter_runs") || prop.includes("runs_scored")) return "Runs";
    if (market.includes("hitter_hits") || prop.endsWith("hits")) return "Hits";
    if (prop === "strikeouts") return "K";
    if (market.includes("earned_runs") || prop === "earned_runs") return "ER";
    if (prop === "outs") return "Outs";
    return String(reco?.market_label || reco?.market || "Prop");
  }

  function isLiveStatus(statusText) {
    const text = String(statusText || "").trim().toLowerCase();
    return text.includes("live") || text.includes("in progress") || text.includes("manager challenge");
  }

  function gameProgress(snapshot, card) {
    const status = snapshot?.status || {};
    const abstract = String(status.abstractGameState || card?.status?.abstract || "").trim().toLowerCase();
    const detailed = String(status.detailedState || card?.status?.detailed || "").trim();
    if (abstract === "final") {
      return { fraction: 1, inning: 9, half: "final", outs: 3, label: detailed || "Final", isLive: false, isFinal: true };
    }
    if (!isLiveStatus(abstract)) {
      return { fraction: 0, inning: null, half: null, outs: 0, label: detailed || "Pregame", isLive: false, isFinal: false };
    }
    const current = snapshot?.current || {};
    const inning = Number(current?.inning) || 1;
    const half = String(current?.halfInning || "").trim().toLowerCase();
    const outsRaw = Number(current?.count?.outs ?? current?.outs ?? 0);
    const outs = Math.max(0, Math.min(2, Number.isFinite(outsRaw) ? outsRaw : 0));
    const outsRecorded = ((inning - 1) * 6) + (half === "bottom" ? 3 : 0) + outs;
    return {
      fraction: Math.max(0, Math.min(1, outsRecorded / 54)),
      inning,
      half,
      outs,
      label: half ? `${half.replace(/^./, (m) => m.toUpperCase())} ${inning}` : `Inning ${inning}`,
      isLive: true,
      isFinal: false,
    };
  }

  function projectLiveValue(actualValue, modelMean, progressFraction) {
    const actual = toNumber(actualValue) || 0;
    const mean = toNumber(modelMean);
    if (mean == null) return null;
    const progress = Math.max(0, Math.min(1, Number(progressFraction || 0)));
    const expectedToDate = mean * progress;
    const remaining = Math.max(mean - expectedToDate, 0);
    return Number((actual + remaining).toFixed(3));
  }

  function segmentProjection(config) {
    const pregameAway = toNumber(config?.pregameAway);
    const pregameHome = toNumber(config?.pregameHome);
    if (pregameAway == null || pregameHome == null) {
      return { away: null, home: null, total: null, homeMargin: null, closed: false };
    }
    const actualAway = toNumber(config?.actualAway) || 0;
    const actualHome = toNumber(config?.actualHome) || 0;
    const progressFraction = Math.max(0, Math.min(1, Number(config?.progressFraction || 0)));
    const targetFraction = Math.max(0, Math.min(1, Number(config?.targetInnings || 9) / 9));
    if (progressFraction > targetFraction + 1e-9) {
      return { away: null, home: null, total: null, homeMargin: null, closed: true };
    }
    const awayTarget = pregameAway * targetFraction;
    const homeTarget = pregameHome * targetFraction;
    const expectedAwayToDate = Math.min(pregameAway * progressFraction, awayTarget);
    const expectedHomeToDate = Math.min(pregameHome * progressFraction, homeTarget);
    const away = actualAway + Math.max(awayTarget - expectedAwayToDate, 0);
    const home = actualHome + Math.max(homeTarget - expectedHomeToDate, 0);
    return {
      away: Number(away.toFixed(2)),
      home: Number(home.toFixed(2)),
      total: Number((away + home).toFixed(2)),
      homeMargin: Number((home - away).toFixed(2)),
      closed: false,
    };
  }

  function baselineHomeWinProb(card, key) {
    const row = card?.predictions?.[key];
    if (!row || typeof row !== "object") return null;
    const away = toNumber(row.away_win_prob);
    const home = toNumber(row.home_win_prob);
    if (home == null) return null;
    const normalized = normalizeTwoWay(home, away);
    return normalized.first == null ? home : normalized.first;
  }

  function propModelMean(reco, simRow) {
    const rowValue = statValue(simRow, reco);
    if (rowValue != null) return rowValue;
    return toNumber(reco?.outs_mean);
  }

  function buildGameLensRows(card, detail) {
    const snapshot = detail?.snapshot || null;
    const sim = detail?.sim || null;
    const progress = gameProgress(snapshot, card);
    const predictedAway = toNumber(sim?.predicted?.away);
    const predictedHome = toNumber(sim?.predicted?.home);
    const actualAway = toNumber(snapshot?.teams?.away?.totals?.R) || 0;
    const actualHome = toNumber(snapshot?.teams?.home?.totals?.R) || 0;
    const lines = trackedGameLines(card);
    const h2h = lines.h2h || {};
    const spreads = lines.spreads || {};
    const totals = lines.totals || {};
    const moneylineHomeOdds = h2h.home_odds || h2h.homeOdds || null;
    const moneylineAwayOdds = h2h.away_odds || h2h.awayOdds || null;
    const marketHomeProb = normalizeTwoWay(
      americanOddsImpliedProb(moneylineHomeOdds),
      americanOddsImpliedProb(moneylineAwayOdds)
    ).first;
    const segments = [
      { key: "live", label: progress.label || "Live", innings: 9 },
      { key: "first3", label: "F3", innings: 3 },
      { key: "first5", label: "F5", innings: 5 },
      { key: "first7", label: "F7", innings: 7 },
      { key: "full", label: "Full", innings: 9 },
    ];

    return segments.map((segment) => {
      const projection = segmentProjection({
        pregameAway: predictedAway,
        pregameHome: predictedHome,
        actualAway,
        actualHome,
        progressFraction: progress.fraction,
        targetInnings: segment.innings,
      });
      const modelHomeProb = logisticWinProb(projection.homeMargin);
      const baselineProb = baselineHomeWinProb(card, segment.key);
      const totalLine = toNumber(totals.line);
      const totalEdge = projection.total == null || totalLine == null ? null : Number((projection.total - totalLine).toFixed(2));
      const spreadHomeLine = toNumber(spreads.home_line ?? spreads.homeLine);
      const spreadEdge = projection.homeMargin == null || spreadHomeLine == null ? null : Number((projection.homeMargin + spreadHomeLine).toFixed(2));
      const homeDelta = modelHomeProb == null || marketHomeProb == null ? null : modelHomeProb - marketHomeProb;
      const awayDelta = modelHomeProb == null || marketHomeProb == null ? null : (1 - modelHomeProb) - (1 - marketHomeProb);
      let moneylinePick = null;
      let moneylineEdge = null;
      if (homeDelta != null && awayDelta != null) {
        if (Math.abs(homeDelta) >= Math.abs(awayDelta) && homeDelta > 0) {
          moneylinePick = "home";
          moneylineEdge = Number(homeDelta.toFixed(3));
        } else if (awayDelta > 0) {
          moneylinePick = "away";
          moneylineEdge = Number(awayDelta.toFixed(3));
        }
      }
      return {
        key: segment.key,
        label: segment.label,
        closed: !!projection.closed,
        projection,
        baselineHomeWinProb: baselineProb,
        modelHomeWinProb: modelHomeProb,
        markets: {
          moneyline: {
            pick: moneylinePick,
            edge: moneylineEdge,
            homeOdds: moneylineHomeOdds,
            awayOdds: moneylineAwayOdds,
            marketHomeProb,
          },
          spread: {
            pick: spreadEdge == null ? null : (spreadEdge > 0 ? "home" : (spreadEdge < 0 ? "away" : null)),
            edge: spreadEdge,
            homeLine: spreadHomeLine,
            homeOdds: spreads.home_odds || spreads.homeOdds || null,
            awayOdds: spreads.away_odds || spreads.awayOdds || null,
          },
          total: {
            pick: totalEdge == null ? null : (totalEdge > 0 ? "over" : (totalEdge < 0 ? "under" : null)),
            edge: totalEdge,
            line: totalLine,
            overOdds: totals.over_odds || totals.overOdds || null,
            underOdds: totals.under_odds || totals.underOdds || null,
          },
        },
      };
    });
  }

  function renderGameLens(card, detail) {
    const rows = buildGameLensRows(card, detail);
    if (!rows.length) return '<div class="cards-empty-copy">No live game lens available.</div>';
    function pickSummary(label, pick, edge, kind) {
      if (!pick || edge == null) return `${label}: -`;
      if (kind === "moneyline") return `${label}: ${String(pick).toUpperCase()} ${formatSigned(edge * 100, 1)} pts`;
      return `${label}: ${String(pick).toUpperCase()} ${formatSigned(edge, 2)}`;
    }
    return rows.map((row) => {
      const projectionLine = row.closed || row.projection.total == null
        ? 'Segment closed'
        : `${card?.away?.abbr || 'Away'} ${formatLine(row.projection.away)} - ${card?.home?.abbr || 'Home'} ${formatLine(row.projection.home)} | Total ${formatLine(row.projection.total)}`;
      const ml = row.markets.moneyline;
      const spread = row.markets.spread;
      const total = row.markets.total;
      const marketLine = [
        total.line != null ? `Total ${formatLine(total.line)}` : null,
        spread.homeLine != null ? `Home ${formatSigned(spread.homeLine, 1)}` : null,
        ml.homeOdds || ml.awayOdds ? `${card?.away?.abbr || 'Away'} ${formatOdds(ml.awayOdds)} / ${card?.home?.abbr || 'Home'} ${formatOdds(ml.homeOdds)}` : null,
      ].filter(Boolean).join(' | ');
      return `
        <div class="cards-live-lens-card ${row.closed ? 'is-closed' : ''}">
          <div class="cards-live-lens-head">
            <div class="cards-live-lens-title-block">
              <strong>${escapeHtml(row.label)}</strong>
              <div class="cards-live-lens-subtitle">${escapeHtml(projectionLine)}</div>
            </div>
            <span class="cards-chip ${row.closed ? 'is-candidate' : ''}">${escapeHtml(row.closed ? 'Closed' : 'Projection')}</span>
          </div>
          <div class="cards-live-lens-summary-row">
            <div class="cards-data-pair"><span>Home win</span><strong>${escapeHtml(formatPercent(row.modelHomeWinProb, 1))}</strong></div>
            <div class="cards-data-pair"><span>Baseline</span><strong>${escapeHtml(formatPercent(row.baselineHomeWinProb, 1))}</strong></div>
          </div>
          <div class="cards-live-lens-picks">
            <div class="cards-live-lens-pick">${escapeHtml(pickSummary('ML', ml.pick, ml.edge, 'moneyline'))}</div>
            <div class="cards-live-lens-pick">${escapeHtml(pickSummary('Spread', spread.pick, spread.edge, 'spread'))}</div>
            <div class="cards-live-lens-pick">${escapeHtml(pickSummary('Total', total.pick, total.edge, 'total'))}</div>
          </div>
          <div class="cards-live-lens-market">${escapeHtml(marketLine || 'No tracked market line')}</div>
          ${row.closed || row.projection.homeMargin == null ? '' : `<div class="cards-live-lens-margin">${escapeHtml(`Projected margin: ${formatSigned(row.projection.homeMargin, 2)}`)}</div>`}
        </div>`;
    }).join('');
  }

  const PROP_TYPE_ORDER = ["outs", "hits", "runs", "rbi", "tb", "hr"];

  function propSelectionKey(reco) {
    const selection = String(reco?.selection || "").trim().toLowerCase();
    if (selection === "over" || selection === "under") return selection;
    return "other";
  }

  function propTypeInfo(reco) {
    const label = metricLabel(reco) || "Prop";
    const key = String(normalizeName(label) || "prop").replace(/\s+/g, "-");
    return { key, label };
  }

  function filteredPropRows(rows, filters) {
    const safeRows = Array.isArray(rows) ? rows : [];
    const safeFilters = filters || { side: "all", type: "all" };
    return safeRows.filter((row) => {
      const matchesSide = safeFilters.side === "all" || propSelectionKey(row) === safeFilters.side;
      const matchesType = safeFilters.type === "all" || propTypeInfo(row).key === safeFilters.type;
      return matchesSide && matchesType;
    });
  }

  function propSideCounts(rows) {
    const counts = { all: 0, over: 0, under: 0 };
    (Array.isArray(rows) ? rows : []).forEach((row) => {
      counts.all += 1;
      const side = propSelectionKey(row);
      if (side === "over" || side === "under") counts[side] += 1;
    });
    return counts;
  }

  function propTypeCounts(rows) {
    const counts = new Map();
    (Array.isArray(rows) ? rows : []).forEach((row) => {
      const info = propTypeInfo(row);
      counts.set(info.key, {
        key: info.key,
        label: info.label,
        count: (counts.get(info.key)?.count || 0) + 1,
      });
    });
    return counts;
  }

  function propTypeOptions(rows) {
    const byType = new Map();
    (Array.isArray(rows) ? rows : []).forEach((row) => {
      const info = propTypeInfo(row);
      if (!byType.has(info.key)) byType.set(info.key, info.label);
    });
    return Array.from(byType.entries())
      .map(([key, label]) => ({ key, label }))
      .sort((left, right) => {
        const leftOrder = PROP_TYPE_ORDER.indexOf(left.key);
        const rightOrder = PROP_TYPE_ORDER.indexOf(right.key);
        const leftRank = leftOrder === -1 ? 999 : leftOrder;
        const rightRank = rightOrder === -1 ? 999 : rightOrder;
        if (leftRank !== rightRank) return leftRank - rightRank;
        return left.label.localeCompare(right.label);
      });
  }

  function propFilterPillMarkup(config) {
    const disabledAttr = config.disabled ? " disabled" : "";
    return `
      <button
        type="button"
        class="cards-filter-pill cards-prop-filter-pill ${config.active ? "is-active" : ""}"
        data-prop-filter-kind="${escapeHtml(config.kind)}"
        data-prop-filter-value="${escapeHtml(config.value)}"${disabledAttr}
      >
        <span>${escapeHtml(config.label)}</span>
        <span class="cards-prop-filter-count">${escapeHtml(String(config.count || 0))}</span>
      </button>`;
  }

  function renderPropFilterControls(rows, detail, options = {}) {
    const filters = detail?.propFilters || { board: "auto", side: "all", type: "all" };
    const sideBaseRows = rows.filter((row) => filters.type === "all" || propTypeInfo(row).key === filters.type);
    const typeBaseRows = rows.filter((row) => filters.side === "all" || propSelectionKey(row) === filters.side);
    const sideCounts = propSideCounts(sideBaseRows);
    const typeCounts = propTypeCounts(typeBaseRows);
    const typeOptions = propTypeOptions(rows);
    const boardOptions = Array.isArray(options?.boardOptions) ? options.boardOptions : [];

    return `
      <div class="cards-prop-filter-shell">
        ${boardOptions.length ? `
          <div class="cards-prop-filter-group">
            <div class="cards-section-label">Board</div>
            <div class="cards-prop-filter-pills">
              ${boardOptions
                .map((option) => propFilterPillMarkup({
                  kind: "board",
                  value: option.value,
                  label: option.label,
                  count: option.count,
                  active: filters.board === option.value,
                  disabled: option.count === 0 && filters.board !== option.value,
                }))
                .join("")}
            </div>
          </div>` : ""}
        <div class="cards-prop-filter-group">
          <div class="cards-section-label">Side</div>
          <div class="cards-prop-filter-pills">
            ${[
              { value: "all", label: "All", count: sideCounts.all },
              { value: "over", label: "Over", count: sideCounts.over },
              { value: "under", label: "Under", count: sideCounts.under },
            ]
              .map((option) => propFilterPillMarkup({
                kind: "side",
                value: option.value,
                label: option.label,
                count: option.count,
                active: filters.side === option.value,
                disabled: option.count === 0 && filters.side !== option.value,
              }))
              .join("")}
          </div>
        </div>
        <div class="cards-prop-filter-group">
          <div class="cards-section-label">Prop type</div>
          <div class="cards-prop-filter-pills">
            ${[
              { key: "all", label: "All", count: typeBaseRows.length },
              ...typeOptions.map((option) => ({
                key: option.key,
                label: option.label,
                count: typeCounts.get(option.key)?.count || 0,
              })),
            ]
              .map((option) => propFilterPillMarkup({
                kind: "type",
                value: option.key,
                label: option.label,
                count: option.count,
                active: filters.type === option.key,
                disabled: option.count === 0 && filters.type !== option.key,
              }))
              .join("")}
          </div>
        </div>
      </div>`;
  }

  function marketLabelLong(reco) {
    const short = metricLabel(reco);
    if (String(reco?.market || "") === "pitcher_props") {
      return `${short} ${String(reco?.selection || "").replace(/^./, (m) => m.toUpperCase())} ${formatLine(reco?.market_line)}`;
    }
    return `${short} ${String(reco?.selection || "").replace(/^./, (m) => m.toUpperCase())} ${formatLine(reco?.market_line)}`;
  }

  function formatPropEdge(reco) {
    const edge = toNumber(reco?.edge);
    if (edge == null) return "-";
    return `${formatSigned(edge * 100, 1)} pts`;
  }

  function teamLogoMarkup(team, extraClass = "") {
    const classPrefix = extraClass ? `${extraClass} ` : "";
    if (team?.logo) {
      return `<img class="${classPrefix}cards-logo" src="${escapeHtml(team.logo)}" alt="${escapeHtml(team?.abbr || team?.name || "team")}" loading="lazy" />`;
    }
    return `<span class="${classPrefix}cards-logo-fallback">${escapeHtml(team?.abbr || "--")}</span>`;
  }

  function teamMarkup(team) {
    return `
      <div class="cards-team-line">
        ${teamLogoMarkup(team)}
        <div class="cards-team-meta">
          <strong class="cards-team-code">${escapeHtml(team?.abbr || "---")}</strong>
          <div class="cards-team-name">${escapeHtml(team?.name || "Unknown team")}</div>
        </div>
      </div>`;
  }

  function tileMarkup(config) {
    const attrs = config.tabTarget
      ? ` data-tab-target="${escapeHtml(config.tabTarget)}" tabindex="0" role="button"`
      : "";
    return `
      <div class="cards-market-tile"${attrs}>
        <div class="cards-market-top">
          <div class="cards-market-label">${escapeHtml(config.label)}</div>
          ${config.badge ? `<span class="cards-chip">${escapeHtml(config.badge)}</span>` : ""}
        </div>
        <div class="cards-market-main">${escapeHtml(config.main || "No play")}</div>
        <div class="cards-market-bottom">
          <div class="cards-market-sub">${escapeHtml(config.sub || "Off card")}</div>
        </div>
      </div>`;
  }

  function summarizeHitterMarkets(rows) {
    const counts = new Map();
    rows.forEach((row) => {
      const key = metricLabel(row);
      counts.set(key, (counts.get(key) || 0) + 1);
    });
    return Array.from(counts.entries())
      .map(([label, count]) => `${count} ${label}`)
      .join(" / ");
  }

  function propCountBadge(officialCount, extraCount) {
    if (officialCount && extraCount) return `${officialCount} official · +${extraCount}`;
    if (officialCount) return `${officialCount} official`;
    if (extraCount) return `${extraCount} playable`;
    return "Off card";
  }

  function propTierLabel(row) {
    if (row?.archived_for_reconciliation) return "Live reconciliation";
    if (row?.recommendation_tier === "live" || row?.source === "current_market" || row?.source === "live_registry") return "Live opportunity";
    return row?.recommendation_tier === "candidate" ? "Playable prop" : "Official pick";
  }

  function marketTiles(card) {
    const totals = card?.markets?.totals || null;
    const ml = card?.markets?.ml || null;
    const pitcherRows = filterPropRowsByOdds(marketRows(card, "pitcherProps"));
    const pitcherExtraRows = filterPropRowsByOdds(extraMarketRows(card, "extraPitcherProps"));
    const hitterRows = filterPropRowsByOdds(marketRows(card, "hitterProps"));
    const hitterExtraRows = filterPropRowsByOdds(extraMarketRows(card, "extraHitterProps"));

    const totalsTile = totals
      ? tileMarkup({
          label: "Game Total",
          badge: formatOdds(totals.odds),
          main: `${String(totals.selection || "").toUpperCase()} ${formatLine(totals.market_line)}`,
          sub: `Model ${formatLine(totals.model_mean_total)} | Edge ${formatSigned(toNumber(totals.edge), 2)}`,
        })
      : tileMarkup({ label: "Game Total", main: "No total play", sub: "Off card" });

    const mlTeam = ml
      ? (String(ml.selection || "").toLowerCase() === "home" ? card?.home?.abbr : card?.away?.abbr)
      : "";
    const mlTile = ml
      ? tileMarkup({
          label: "Moneyline",
          badge: formatOdds(ml.odds),
          main: `${escapeHtml(mlTeam)} ML`,
          sub: `Model ${formatPercent(ml.model_prob, 1)} | Market ${formatPercent(ml.market_no_vig_prob, 1)}`,
        })
      : tileMarkup({ label: "Moneyline", main: "No ML play", sub: "Off card" });

    const pitcherTop = pitcherRows[0] || pitcherExtraRows[0] || null;
    const pitcherTile = pitcherTop
      ? tileMarkup({
          label: "Pitcher Props",
          badge: propCountBadge(pitcherRows.length, pitcherExtraRows.length),
          main: `${escapeHtml(pitcherTop.pitcher_name || "Pitcher")} ${marketLabelLong(pitcherTop)}`,
          sub: pitcherRows.length
            ? `${pitcherExtraRows.length ? `${pitcherExtraRows.length} more playable | ` : ""}Mean ${formatLine(pitcherTop.outs_mean)} | Edge ${formatPropEdge(pitcherTop)}`
            : `Playable only | Mean ${formatLine(pitcherTop.outs_mean)} | Edge ${formatPropEdge(pitcherTop)}`,
          tabTarget: "props",
        })
      : tileMarkup({ label: "Pitcher Props", main: "No pitcher props", sub: "Off card", tabTarget: "props" });

    const hitterTop = hitterRows[0] || hitterExtraRows[0] || null;
    const hitterTile = hitterTop
      ? tileMarkup({
          label: "Hitter Props",
          badge: propCountBadge(hitterRows.length, hitterExtraRows.length),
          main: `${escapeHtml(hitterTop.player_name || "Player")} ${marketLabelLong(hitterTop)}`,
          sub: hitterRows.length
            ? (summarizeHitterMarkets(hitterRows) || `Edge ${formatPropEdge(hitterTop)}`)
            : `Playable only | ${summarizeHitterMarkets(hitterExtraRows) || `Edge ${formatPropEdge(hitterTop)}`}`,
          tabTarget: "props",
        })
      : tileMarkup({ label: "Hitter Props", main: "No hitter props", sub: "Off card", tabTarget: "props" });

    return `${totalsTile}${mlTile}${pitcherTile}${hitterTile}`;
  }

  function probabilityRows(card) {
    const entries = [
      { label: "Full game", row: card?.predictions?.full || null },
      { label: "First 5", row: card?.predictions?.first5 || null },
      { label: "First 3", row: card?.predictions?.first3 || null },
    ];
    return entries
      .map((entry) => {
        const away = toNumber(entry?.row?.away_win_prob) || 0;
        const home = toNumber(entry?.row?.home_win_prob) || 0;
        const tie = toNumber(entry?.row?.tie_prob);
        const meta = [
          `${card?.away?.abbr || "Away"} ${formatPercent(away, 1)}`,
          `${card?.home?.abbr || "Home"} ${formatPercent(home, 1)}`,
          tie != null && tie > 0 ? `Tie ${formatPercent(tie, 1)}` : "",
        ]
          .filter(Boolean)
          .join(" | ");
        return `
          <div class="cards-prob-row">
            <div class="cards-prob-label">${escapeHtml(entry.label)}</div>
            <div class="cards-prob-bar" style="--away-pct:${Math.max(10, away * 100).toFixed(1)}%; --home-pct:${Math.max(10, home * 100).toFixed(1)}%;">
              <div class="cards-prob-away"></div>
              <div class="cards-prob-home"></div>
            </div>
            <div class="cards-mini-copy">${escapeHtml(meta || "Probabilities unavailable")}</div>
          </div>`;
      })
      .join("");
  }

  function calloutMarkup(card) {
    const totals = card?.markets?.totals || null;
    const ml = card?.markets?.ml || null;
    const pitcherTop = filterPropRowsByOdds(marketRows(card, "pitcherProps"))[0] || null;
    const hitterTop = filterPropRowsByOdds(marketRows(card, "hitterProps"))[0] || null;
    const extraCount = extraPropRows(card).length;
    const items = [];

    if (totals) {
      items.push(`
        <li class="cards-callout">
          <strong>Total</strong>
          <span class="cards-callout-copy">${escapeHtml(String(totals.selection || "").toUpperCase())} ${escapeHtml(formatLine(totals.market_line))} ${escapeHtml(formatOdds(totals.odds))}</span>
        </li>`);
    }
    if (ml) {
      const pickedTeam = String(ml.selection || "").toLowerCase() === "home" ? card?.home?.abbr : card?.away?.abbr;
      items.push(`
        <li class="cards-callout">
          <strong>ML</strong>
          <span class="cards-callout-copy">${escapeHtml(pickedTeam || "Team")} ${escapeHtml(formatOdds(ml.odds))} | model ${escapeHtml(formatPercent(ml.model_prob, 1))}</span>
        </li>`);
    }
    if (pitcherTop) {
      items.push(`
        <li>
          <button type="button" class="cards-callout cards-callout-button" data-tab-target="props" data-prop-key="${escapeHtml(propKey(pitcherTop))}" data-prop-board="pregame">
            <strong>Pitcher</strong>
            <span class="cards-callout-copy">${escapeHtml(pitcherTop.pitcher_name || "Pitcher")} ${escapeHtml(marketLabelLong(pitcherTop))} | ${escapeHtml(formatOdds(pitcherTop.odds))}</span>
          </button>
        </li>`);
    }
    if (hitterTop) {
      items.push(`
        <li>
          <button type="button" class="cards-callout cards-callout-button" data-tab-target="props" data-prop-key="${escapeHtml(propKey(hitterTop))}" data-prop-board="pregame">
            <strong>Top hitter</strong>
            <span class="cards-callout-copy">${escapeHtml(hitterTop.player_name || "Player")} ${escapeHtml(marketLabelLong(hitterTop))} | ${escapeHtml(formatOdds(hitterTop.odds))}</span>
          </button>
        </li>`);
    }
    if (extraCount) {
      items.push(`
        <li class="cards-callout">
          <strong>Playable board</strong>
          <span class="cards-callout-copy">+${escapeHtml(String(extraCount))} additional prop lanes qualified but were not promoted to the official card.</span>
        </li>`);
    }
    if (!items.length) {
      items.push(`
        <li class="cards-callout">
          <strong>Market board</strong>
          <span class="cards-callout-copy">No saved game or player market snapshot was captured for this matchup, so only model outputs are available.</span>
        </li>`);
    }
    return items.join("");
  }

  function createCardNode(card) {
    const detail = ensureDetail(card);
    const officialPropCount = officialPropRows(card).length;
    const extraPropCount = extraPropRows(card).length;
    const article = document.createElement("article");
    article.className = "cards-game-card";
    article.id = `game-card-${card.gamePk}`;
    article.dataset.gamePk = String(card.gamePk);
    article.innerHTML = `
      <div class="cards-strip-head">
        <div class="cards-head-left">
          ${teamMarkup(card.away)}
          <span class="cards-score-divider">@</span>
          ${teamMarkup(card.home)}
        </div>
        <div class="cards-status-cluster">
          <span class="cards-status-badge ${escapeHtml(statusClass(card.status?.abstract))}" data-role="status-badge">${escapeHtml(card.status?.abstract || "Scheduled")}</span>
          <div class="cards-start-time" data-role="status-detail">${escapeHtml(statusDetailText(null, card) || card.startTime || "")}</div>
          <a class="cards-game-link" href="/game/${encodeURIComponent(card.gamePk)}?date=${encodeURIComponent(state.date)}">Open game view</a>
        </div>
      </div>

      <div class="cards-score-ribbon">
        <div class="cards-score-side">
          <div class="cards-score-label">Away</div>
          <div class="cards-score-number" data-role="away-score">-</div>
          <strong>${escapeHtml(card.away?.abbr || "AWY")}</strong>
        </div>
        <div class="cards-score-divider">at</div>
        <div class="cards-score-side">
          <div class="cards-score-label">Home</div>
          <div class="cards-score-number" data-role="home-score">-</div>
          <strong>${escapeHtml(card.home?.abbr || "HME")}</strong>
        </div>
        <div class="cards-score-meta">
          <div class="cards-live-line" data-role="live-line">Loading live box...</div>
          <div class="cards-sim-line" data-role="sim-line">Loading sim box...</div>
          <div class="cards-mini-copy">Probables: ${escapeHtml(starterText(card))}</div>
        </div>
      </div>

      <div class="cards-market-row">
        ${marketTiles(card)}
      </div>

      <div class="cards-tabs">
        <button class="cards-tab is-active" type="button" data-tab-target="overview">Game</button>
        <button class="cards-tab" type="button" data-tab-target="boxscore">Box Score</button>
        <button class="cards-tab" type="button" data-tab-target="props">Props</button>
      </div>

      <section class="cards-panel is-active" data-panel-id="overview">
        <div class="cards-overview-grid">
          <div class="cards-panel-card">
            <div class="cards-box-head">
              <div class="cards-table-title"><strong>Game lens</strong></div>
              <span class="cards-overview-badge">${escapeHtml(card.gameType || "MLB")}</span>
            </div>
            <div class="cards-live-lens-grid" data-role="game-lens"></div>
            <div class="cards-prob-grid">${probabilityRows(card)}</div>
            <div class="cards-mini-metrics">
              <div class="cards-mini-metric">
                <span class="cards-section-label">Away starter</span>
                <strong>${escapeHtml(card?.probable?.away?.fullName || "TBD")}</strong>
              </div>
              <div class="cards-mini-metric">
                <span class="cards-section-label">Home starter</span>
                <strong>${escapeHtml(card?.probable?.home?.fullName || "TBD")}</strong>
              </div>
              <div class="cards-mini-metric">
                <span class="cards-section-label">Official props</span>
                <strong>${escapeHtml(String(officialPropCount || 0))}</strong>
                <div class="cards-mini-copy">${escapeHtml(extraPropCount ? `+${extraPropCount} more playable` : "No extra playable props")}</div>
              </div>
            </div>
          </div>

          <div class="cards-panel-card">
            <div class="cards-box-head">
              <div class="cards-table-title"><strong>Official card</strong></div>
              <span class="cards-chip">${escapeHtml(marketCountSummary(card))}</span>
            </div>
            <ul class="cards-callout-list">${calloutMarkup(card)}</ul>
            <div data-role="prop-overview-lens"></div>
          </div>
        </div>
      </section>

      <section class="cards-panel" data-panel-id="boxscore">
        <div class="cards-box-grid">
          <div class="cards-panel-card cards-box-panel">
            <div class="cards-box-head">
              <div class="cards-table-title"><strong>Live / final box</strong></div>
              <span class="cards-overview-badge" data-role="actual-badge">${escapeHtml(card.status?.abstract || "Scheduled")}</span>
            </div>
            <div class="cards-box-totals" data-role="actual-totals">
              <div class="cards-empty-copy">Loading live box...</div>
            </div>
            <div data-role="actual-box"></div>
          </div>

          <div class="cards-panel-card cards-box-panel">
            <div class="cards-box-head">
              <div class="cards-table-title"><strong>Sim box</strong></div>
              <span class="cards-chip" data-role="sim-badge">Loading</span>
            </div>
            <div class="cards-box-totals" data-role="sim-totals">
              <div class="cards-empty-copy">Loading sim box...</div>
            </div>
            <div data-role="sim-box"></div>
          </div>
        </div>
      </section>

      <section class="cards-panel" data-panel-id="props">
        <div class="cards-props-grid">
          <div class="cards-panel-card">
            <div class="cards-box-head">
              <div class="cards-table-title"><strong>Props board</strong></div>
              <span class="cards-chip" data-role="prop-summary-chip">${escapeHtml(propCountBadge(officialPropCount, extraPropCount))}</span>
            </div>
            <div class="cards-prop-filter-shell" data-role="prop-filters"></div>
            <div class="cards-prop-groups" data-role="prop-groups"></div>
          </div>
          <div class="cards-lens-shell" data-role="prop-lens"></div>
        </div>
      </section>`;

    attachCardEvents(article, card);
    renderPropSections(card, article);
    activateTab(article, detail?.activeTab || "overview");
    return article;
  }

  function attachCardEvents(node, card) {
    node.addEventListener("click", function (event) {
      const tabButton = event.target.closest("[data-tab-target]");
      if (tabButton && node.contains(tabButton)) {
        activateTab(node, tabButton.getAttribute("data-tab-target"));
      }
      const propFilterButton = event.target.closest(".cards-prop-filter-pill");
      if (propFilterButton && node.contains(propFilterButton)) {
        event.preventDefault();
        setPropFilter(
          card,
          propFilterButton.getAttribute("data-prop-filter-kind"),
          propFilterButton.getAttribute("data-prop-filter-value")
        );
        return;
      }
      const propButton = event.target.closest("[data-prop-key]");
      if (propButton && node.contains(propButton)) {
        event.preventDefault();
        const boardValue = propButton.getAttribute("data-prop-board");
        if (boardValue) {
          setPropFilter(card, "board", boardValue);
        }
        selectProp(card, propButton.getAttribute("data-prop-key"));
        activateTab(node, "props");
      }
    });

    node.addEventListener("keydown", function (event) {
      const isActivateKey = event.key === "Enter" || event.key === " ";
      if (!isActivateKey) return;
      const target = event.target.closest("[data-tab-target]");
      if (target && node.contains(target)) {
        event.preventDefault();
        activateTab(node, target.getAttribute("data-tab-target"));
      }
    });
  }

  function activateTab(node, panelId) {
    const gamePk = Number(node?.dataset?.gamePk || 0);
    if (gamePk && state.details.has(gamePk)) {
      state.details.get(gamePk).activeTab = panelId;
    }
    node.querySelectorAll(".cards-tab").forEach((button) => {
      button.classList.toggle("is-active", button.getAttribute("data-tab-target") === panelId);
    });
    node.querySelectorAll(".cards-panel").forEach((panel) => {
      panel.classList.toggle("is-active", panel.getAttribute("data-panel-id") === panelId);
    });
  }

  function renderTableHtml(columns, rows) {
    const safeRows = Array.isArray(rows) ? rows : [];
    if (!safeRows.length) return `<div class="cards-empty-copy">No rows available.</div>`;
    const head = columns.map((col) => `<th>${escapeHtml(col)}</th>`).join("");
    const body = safeRows
      .map((row) => {
        const cells = columns
          .map((col) => `<td>${escapeHtml(row?.[col] == null ? "" : row[col])}</td>`)
          .join("");
        return `<tr>${cells}</tr>`;
      })
      .join("");
    return `
      <div class="cards-table-wrap">
        <table class="cards-table">
          <thead><tr>${head}</tr></thead>
          <tbody>${body}</tbody>
        </table>
      </div>`;
  }

  function linescoreValue(value) {
    return value == null || value === "" ? "-" : String(value);
  }

  function linescoreSummaryMarkup(rows, options = {}) {
    const compactClass = options.compact ? " is-compact" : "";
    return `
      <div class="cards-linescore${compactClass}">
        <div class="cards-linescore-head">
          <span class="cards-linescore-team-label">Team</span>
          <span class="cards-linescore-stat-head">R</span>
          <span class="cards-linescore-stat-head">H</span>
          <span class="cards-linescore-stat-head">E</span>
        </div>
        ${rows
          .map((row) => `
            <div class="cards-linescore-row">
              <div class="cards-linescore-team">
                ${options.showLogos ? teamLogoMarkup(row.team, options.logoClass || "") : ""}
                <strong>${escapeHtml(row.team?.abbr || row.label || "---")}</strong>
              </div>
              <span class="cards-linescore-stat">${escapeHtml(linescoreValue(row.totals?.R))}</span>
              <span class="cards-linescore-stat">${escapeHtml(linescoreValue(row.totals?.H))}</span>
              <span class="cards-linescore-stat">${escapeHtml(linescoreValue(row.totals?.E))}</span>
            </div>`)
          .join("")}
      </div>`;
  }

  function renderActualBox(card, detail, node) {
    const snapshot = detail.snapshot;
    const totalsNode = node.querySelector('[data-role="actual-totals"]');
    const boxNode = node.querySelector('[data-role="actual-box"]');
    const badgeNode = node.querySelector('[data-role="actual-badge"]');
    if (!totalsNode || !boxNode || !badgeNode) return;

    if (!snapshot || !snapshot.teams) {
      totalsNode.innerHTML = '<div class="cards-empty-copy">Live or final box is unavailable.</div>';
      boxNode.innerHTML = '<div class="cards-empty-copy">No live box tables loaded yet.</div>';
      return;
    }

    badgeNode.textContent = snapshot?.status?.abstractGameState || card?.status?.abstract || "Game";
    badgeNode.className = `cards-overview-badge ${statusClass(snapshot?.status?.abstractGameState)}`.trim();

    totalsNode.innerHTML = linescoreSummaryMarkup([
      { team: card?.away, label: card?.away?.abbr || "Away", totals: snapshot?.teams?.away?.totals || null },
      { team: card?.home, label: card?.home?.abbr || "Home", totals: snapshot?.teams?.home?.totals || null },
    ]);

    const awayBox = snapshot?.teams?.away?.boxscore || {};
    const homeBox = snapshot?.teams?.home?.boxscore || {};
    boxNode.innerHTML = `
      <div class="cards-box-panel">
        <div class="cards-table-head"><div class="cards-table-title">${escapeHtml(card?.away?.abbr || "Away")} batting</div></div>
        ${renderTableHtml(ACTUAL_BATTING_COLUMNS, awayBox.batting || [])}
        <div class="cards-table-head"><div class="cards-table-title">${escapeHtml(card?.away?.abbr || "Away")} pitching</div></div>
        ${renderTableHtml(ACTUAL_PITCHING_COLUMNS, awayBox.pitching || [])}
      </div>
      <div class="cards-box-panel">
        <div class="cards-table-head"><div class="cards-table-title">${escapeHtml(card?.home?.abbr || "Home")} batting</div></div>
        ${renderTableHtml(ACTUAL_BATTING_COLUMNS, homeBox.batting || [])}
        <div class="cards-table-head"><div class="cards-table-title">${escapeHtml(card?.home?.abbr || "Home")} pitching</div></div>
        ${renderTableHtml(ACTUAL_PITCHING_COLUMNS, homeBox.pitching || [])}
      </div>`;
  }

  function renderSimBox(card, detail, node) {
    const sim = detail.sim;
    const totalsNode = node.querySelector('[data-role="sim-totals"]');
    const boxNode = node.querySelector('[data-role="sim-box"]');
    const badgeNode = node.querySelector('[data-role="sim-badge"]');
    if (!totalsNode || !boxNode || !badgeNode) return;

    if (!sim || sim.found === false) {
      badgeNode.textContent = "No sim";
      totalsNode.innerHTML = '<div class="cards-empty-copy">No sim output found for this game.</div>';
      boxNode.innerHTML = '<div class="cards-empty-copy">Sim box tables unavailable.</div>';
      return;
    }

    const predictedAway = sim?.predicted?.away;
    const predictedHome = sim?.predicted?.home;
    const battingColumns = simBattingColumns(sim);
    const pitchingColumns = simPitchingColumns(sim);
    const battingLabel = sim?.boxscoreMode === "aggregate" ? "batting (mean)" : "batting (sim)";
    const pitchingLabel = sim?.pitchingScope === "starters_only"
      ? "pitching (starter mean)"
      : (sim?.boxscoreMode === "aggregate" ? "pitching (mean)" : "pitching (sim)");
    if (sim?.boxscoreMode === "aggregate") {
      badgeNode.textContent = sim?.simCount ? `Mean of ${sim.simCount} sims` : "Mean sim";
    } else {
      badgeNode.textContent = (predictedAway != null && predictedHome != null)
        ? `${card?.away?.abbr || "Away"} ${predictedAway} - ${card?.home?.abbr || "Home"} ${predictedHome}`
        : "Sim loaded";
    }

    const awayTotals = sim?.boxscore?.away?.totals || { R: predictedAway, H: "-", E: "-" };
    const homeTotals = sim?.boxscore?.home?.totals || { R: predictedHome, H: "-", E: "-" };
    totalsNode.innerHTML = linescoreSummaryMarkup([
      {
        team: card?.away,
        label: card?.away?.abbr || "Away",
        totals: { R: awayTotals?.R ?? predictedAway, H: awayTotals?.H ?? "-", E: awayTotals?.E ?? "-" },
      },
      {
        team: card?.home,
        label: card?.home?.abbr || "Home",
        totals: { R: homeTotals?.R ?? predictedHome, H: homeTotals?.H ?? "-", E: homeTotals?.E ?? "-" },
      },
    ]);

    const awayBox = sim?.boxscore?.away || {};
    const homeBox = sim?.boxscore?.home || {};
    boxNode.innerHTML = `
      <div class="cards-box-panel">
        <div class="cards-table-head"><div class="cards-table-title">${escapeHtml(card?.away?.abbr || "Away")} ${escapeHtml(battingLabel)}</div></div>
        ${renderTableHtml(battingColumns, awayBox.batting || [])}
        <div class="cards-table-head"><div class="cards-table-title">${escapeHtml(card?.away?.abbr || "Away")} ${escapeHtml(pitchingLabel)}</div></div>
        ${renderTableHtml(pitchingColumns, awayBox.pitching || [])}
      </div>
      <div class="cards-box-panel">
        <div class="cards-table-head"><div class="cards-table-title">${escapeHtml(card?.home?.abbr || "Home")} ${escapeHtml(battingLabel)}</div></div>
        ${renderTableHtml(battingColumns, homeBox.batting || [])}
        <div class="cards-table-head"><div class="cards-table-title">${escapeHtml(card?.home?.abbr || "Home")} ${escapeHtml(pitchingLabel)}</div></div>
        ${renderTableHtml(pitchingColumns, homeBox.pitching || [])}
      </div>`;
  }

  function propOwnerName(reco) {
    return String(reco?.player_name || reco?.pitcher_name || "").trim();
  }

  function propSide(card, reco) {
    const explicit = String(reco?.team_side || "").toLowerCase();
    if (explicit === "away" || explicit === "home") return explicit;
    const teamValue = String(reco?.team || "").trim().toLowerCase();
    const awayVals = [card?.away?.abbr, card?.away?.name];
    const homeVals = [card?.home?.abbr, card?.home?.name];
    if (awayVals.some((value) => String(value || "").trim().toLowerCase() === teamValue)) return "away";
    if (homeVals.some((value) => String(value || "").trim().toLowerCase() === teamValue)) return "home";
    return null;
  }

  function lookupRow(rowSet, playerName) {
    const rows = Array.isArray(rowSet) ? rowSet : [];
    const target = normalizeName(playerName);
    if (!target) return null;
    return rows.find((row) => normalizeName(row?.name) === target) || null;
  }

  function playerRows(card, detail, reco) {
    const name = propOwnerName(reco);
    const isPitcher = String(reco?.market || "") === "pitcher_props";
    const typeKey = isPitcher ? "pitching" : "batting";
    const side = propSide(card, reco);

    const actualTeams = detail?.snapshot?.teams || {};
    const simBox = detail?.sim?.boxscore || {};

    const searchActual = [];
    const searchSim = [];

    if (side === "away" || side === "home") {
      searchActual.push(actualTeams?.[side]?.boxscore?.[typeKey] || []);
      searchSim.push(simBox?.[side]?.[typeKey] || []);
    } else {
      searchActual.push(actualTeams?.away?.boxscore?.[typeKey] || [], actualTeams?.home?.boxscore?.[typeKey] || []);
      searchSim.push(simBox?.away?.[typeKey] || [], simBox?.home?.[typeKey] || []);
    }

    let actualRow = null;
    let simRow = null;
    for (const rows of searchActual) {
      actualRow = lookupRow(rows, name);
      if (actualRow) break;
    }
    for (const rows of searchSim) {
      simRow = lookupRow(rows, name);
      if (simRow) break;
    }
    return { actualRow, simRow, isPitcher };
  }

  function statValue(row, reco) {
    if (!row) return null;
    const market = String(reco?.market || "").toLowerCase();
    const prop = String(reco?.prop || "").toLowerCase();
    if (market.includes("home_runs") || prop.includes("home_runs")) return toNumber(row?.HR);
    if (market.includes("total_bases") || prop.includes("total_bases")) return toNumber(row?.TB);
    if (market.includes("rbis") || prop.includes("rbi")) return toNumber(row?.RBI);
    if (market.includes("hitter_runs") || prop.includes("runs_scored")) return toNumber(row?.R);
    if (market.includes("hitter_hits") || prop.endsWith("hits")) return toNumber(row?.H);
    if (prop === "strikeouts") return toNumber(row?.SO);
    if (market.includes("earned_runs") || prop === "earned_runs") return toNumber(row?.ER);
    if (prop === "outs") return toNumber(row?.OUTS) ?? parseIpToOuts(row?.IP);
    return null;
  }

  function propResultBadge(reco, actualValue, statusText) {
    if (actualValue == null) {
      const liveish = isLiveStatus(statusText);
      return { label: liveish ? "Live" : "Pending", className: "is-live" };
    }
    if (isLiveStatus(statusText)) return { label: "Live", className: "is-live" };
    const line = toNumber(reco?.market_line);
    const selection = String(reco?.selection || "over").toLowerCase();
    if (line == null) return { label: "Pending", className: "is-live" };
    const delta = actualValue - line;
    if (Math.abs(delta) < 1e-9) return { label: "Push", className: "is-push" };
    const didWin = selection === "under" ? actualValue < line : actualValue > line;
    return { label: didWin ? "Win" : "Loss", className: didWin ? "" : "is-loss" };
  }

  function propLensState(card, detail, reco) {
    if (!reco) return null;
    const rows = playerRows(card, detail, reco);
    const isLiveOpportunity = String(reco?.recommendation_tier || "").toLowerCase() === "live" || String(reco?.source || "") === "current_market";
    const backendActual = reco?.actual != null ? reco.actual : reco?.actual_value;
    const actualValue = isLiveOpportunity && backendActual != null ? toNumber(backendActual) : statValue(rows.actualRow, reco);
    const simValue = statValue(rows.simRow, reco);
    const modelMean = isLiveOpportunity && reco?.model_mean != null ? toNumber(reco.model_mean) : propModelMean(reco, rows.simRow);
    const progress = gameProgress(detail?.snapshot, card);
    const liveProjection = isLiveOpportunity && reco?.live_projection != null
      ? toNumber(reco.live_projection)
      : projectLiveValue(actualValue, modelMean, progress.fraction);
    const line = toNumber(reco?.market_line);
    const selection = String(reco?.selection || "").toLowerCase();
    let liveEdge = null;
    if (isLiveOpportunity && reco?.live_edge != null) {
      liveEdge = toNumber(reco.live_edge);
    } else if (liveProjection != null && line != null) {
      if (selection === "under") liveEdge = Number((line - liveProjection).toFixed(2));
      else if (selection === "over") liveEdge = Number((liveProjection - line).toFixed(2));
      else liveEdge = Number((liveProjection - line).toFixed(2));
    }
    const badge = propResultBadge(reco, actualValue, detail?.snapshot?.status?.abstractGameState || card?.status?.abstract);
    return {
      reco,
      rows,
      actualValue,
      simValue,
      modelMean,
      liveProjection,
      liveEdge,
      badge,
      tierLabel: propTierLabel(reco),
      lineLabel: `${String(reco?.selection || "over").replace(/^./, (m) => m.toUpperCase())} ${formatLine(reco?.market_line)}`,
    };
  }

  function renderPropOverviewLens(card, detail) {
    const livePayloadAvailable = hasLivePropPayload(detail);
    const liveRows = livePropRows(detail);
    const overviewRows = livePayloadAvailable ? liveRows : officialPropRows(card).concat(extraPropRows(card));
    const rankedRows = overviewRows
      .map((reco) => ({ reco, state: propLensState(card, detail, reco) }))
      .filter((entry) => entry.state && entry.state.reco)
      .sort((left, right) => {
        if (liveRows.length) {
          const leftLive = Math.abs(toNumber(left.state.liveEdge) || 0);
          const rightLive = Math.abs(toNumber(right.state.liveEdge) || 0);
          if (leftLive !== rightLive) return rightLive - leftLive;
        }
        const leftOfficial = left.state.tierLabel === "Official pick" ? 0 : 1;
        const rightOfficial = right.state.tierLabel === "Official pick" ? 0 : 1;
        if (leftOfficial !== rightOfficial) return leftOfficial - rightOfficial;
        const leftLive = Math.abs(toNumber(left.state.liveEdge) || 0);
        const rightLive = Math.abs(toNumber(right.state.liveEdge) || 0);
        if (leftLive !== rightLive) return rightLive - leftLive;
        const leftEdge = Math.abs(toNumber(left.reco?.edge) || 0);
        const rightEdge = Math.abs(toNumber(right.reco?.edge) || 0);
        if (leftEdge !== rightEdge) return rightEdge - leftEdge;
        return String(propOwnerName(left.reco) || "").localeCompare(String(propOwnerName(right.reco) || ""));
      })
      .slice(0, 6);

    const items = rankedRows.map((entry) => {
      const reco = entry.reco;
      const label = String(reco?.market || "") === "pitcher_props" ? "Pitcher live lens" : "Hitter live lens";
      return { label, state: entry.state };
    });

    if (!items.length) {
      return livePayloadAvailable
        ? '<div class="cards-empty-copy">No unresolved live prop opportunities remain for this game.</div>'
        : '<div class="cards-empty-copy">No tracked player props for live lens.</div>';
    }

    return `
      <div class="cards-prop-overview-grid">
        ${items.map((entry) => {
          const stateObj = entry.state;
          const reco = stateObj.reco;
          const liveEdgeClass = stateObj.liveEdge == null ? "" : (stateObj.liveEdge >= 0 ? "is-positive" : "is-negative");
          return `
            <div class="cards-prop-overview-card">
              <div class="cards-lens-head">
                <div>
                  <div class="cards-lens-label">${escapeHtml(entry.label)}</div>
                  <div class="cards-lens-main">${escapeHtml(propOwnerName(reco))}</div>
                  <div class="cards-subcopy">${escapeHtml(marketLabelLong(reco))}</div>
                </div>
                <span class="cards-lens-badge ${escapeHtml(stateObj.badge.className)}">${escapeHtml(stateObj.badge.label)}</span>
              </div>
              <div class="cards-prop-overview-metrics">
                <div class="cards-data-pair"><span>Actual</span><strong>${escapeHtml(stateObj.actualValue == null ? '-' : formatLine(stateObj.actualValue))}</strong></div>
                <div class="cards-data-pair"><span>Live proj</span><strong>${escapeHtml(stateObj.liveProjection == null ? '-' : formatLine(stateObj.liveProjection))}</strong></div>
                <div class="cards-data-pair"><span>Line</span><strong>${escapeHtml(stateObj.lineLabel)}</strong></div>
                <div class="cards-data-pair ${liveEdgeClass}"><span>Live edge</span><strong>${escapeHtml(stateObj.liveEdge == null ? '-' : formatSigned(stateObj.liveEdge, 2))}</strong></div>
              </div>
              <div class="cards-prop-overview-foot">
                <span>${escapeHtml(stateObj.modelMean == null ? 'Model mean -' : `Model mean ${formatLine(stateObj.modelMean)}`)}</span>
                <span>${escapeHtml(reco?.first_seen_at ? `Live since ${formatTimestampShort(reco.first_seen_at)}` : `${stateObj.tierLabel} | ${formatOdds(reco?.odds)}`)}</span>
              </div>
            </div>`;
        }).join('')}
      </div>`;
  }

  function propButtonMarkup(rows, selectedKey, tier) {
    if (!rows.length) return '<div class="cards-empty-copy">No rows.</div>';
    const buttonTierClass = tier === "candidate" ? "is-candidate" : (tier === "live" ? "is-live" : "is-official");
    const tierLabel = tier === "candidate" ? "Playable" : (tier === "live" ? "Live" : "Official");
    const boardValue = tier === "live" ? "live" : "pregame";
    return `<div class="cards-prop-list">${rows
      .map((row) => {
        const key = propKey(row);
        const isActive = key === selectedKey;
        return `
          <button type="button" class="cards-prop-button ${buttonTierClass} ${isActive ? "is-active" : ""}" data-prop-key="${escapeHtml(key)}" data-prop-board="${escapeHtml(boardValue)}">
            ${escapeHtml(propOwnerName(row) || "Player")} ${escapeHtml(marketLabelLong(row))}
            <small>${escapeHtml(`${tierLabel} | ${formatOdds(row?.odds)} | ${formatPropEdge(row)}`)}</small>
          </button>`;
      })
      .join("")}</div>`;
  }

  function setPropFilter(card, kind, value) {
    if (kind !== "board" && kind !== "side" && kind !== "type") return;
    const detail = ensureDetail(card);
    detail.propFilters = detail.propFilters || { board: "auto", side: "all", type: "all" };
    detail.propFilters[kind] = value || "all";
    const node = state.cardNodes.get(Number(card.gamePk));
    if (node) renderPropSections(card, node);
  }

  function renderPropSections(card, node) {
    const detail = ensureDetail(card);
    const filters = detail.propFilters || { board: "auto", side: "all", type: "all" };
    const groupsNode = node.querySelector('[data-role="prop-groups"]');
    const lensNode = node.querySelector('[data-role="prop-lens"]');
    const filtersNode = node.querySelector('[data-role="prop-filters"]');
    const summaryChip = node.querySelector('[data-role="prop-summary-chip"]');
    if (!groupsNode || !lensNode || !filtersNode || !summaryChip) return;

    const currentLiveRows = livePropRows(detail);
    const pitcherRows = marketRows(card, "pitcherProps");
    const hitterRows = marketRows(card, "hitterProps");
    const extraPitcherRows = extraMarketRows(card, "extraPitcherProps");
    const extraHitterRows = extraMarketRows(card, "extraHitterProps");
    const officialCount = pitcherRows.length + hitterRows.length;
    const extraCount = extraPitcherRows.length + extraHitterRows.length;
    const hasPregameRows = (officialCount + extraCount) > 0;
    const liveBoardAvailable = hasLivePropPayload(detail);
    const effectiveBoard = filters.board === "auto"
      ? (liveBoardAvailable ? "live" : "pregame")
      : filters.board;
    if (filters.board === "auto") {
      detail.propFilters.board = effectiveBoard;
    }
    const boardOptions = [
      { value: "live", label: "Live", count: currentLiveRows.length },
      { value: "pregame", label: "Pregame", count: officialCount + extraCount },
    ].filter((option) => option.count > 0 || option.value === effectiveBoard);

    if (hasLivePropPayload(detail)) {
      if (effectiveBoard === "pregame" && hasPregameRows) {
        filtersNode.innerHTML = renderPropFilterControls(allPropRows(card), detail, { boardOptions });
      } else {
      const filteredPitcherRows = filteredPropRows(pitcherRows, filters);
      const filteredHitterRows = filteredPropRows(hitterRows, filters);
      const filteredExtraPitcherRows = filteredPropRows(extraPitcherRows, filters);
      const filteredExtraHitterRows = filteredPropRows(extraHitterRows, filters);
      const filteredOfficialCount = filteredPitcherRows.length + filteredHitterRows.length;
      const filteredExtraCount = filteredExtraPitcherRows.length + filteredExtraHitterRows.length;
      const filteredLiveRows = filteredPropRows(currentLiveRows, filters);
      const filtersApplied = filters.side !== "all" || filters.type !== "all";
      filtersNode.innerHTML = renderPropFilterControls(currentLiveRows.length ? currentLiveRows : allPropRows(card), detail, { boardOptions });
      summaryChip.textContent = filtersApplied
        ? ((filteredLiveRows.length || filteredOfficialCount || filteredExtraCount)
          ? `${filteredLiveRows.length} live · ${filteredOfficialCount} official${filteredExtraCount ? ` · +${filteredExtraCount}` : ""}`
          : "No matches")
        : `${currentLiveRows.length} live opps`;

      if (!filteredLiveRows.length && !filteredOfficialCount && !filteredExtraCount) {
        groupsNode.innerHTML = currentLiveRows.length
          ? '<div class="cards-empty-copy">No live or pregame props match the current side and prop-type filters.</div>'
          : '<div class="cards-empty-copy">No unresolved live prop opportunities remain for this game, and no pregame props match the current filters.</div>';
        lensNode.innerHTML = `
          <div class="cards-lens-head">
            <div>
              <div class="cards-lens-label">Prop lens</div>
              <div class="cards-lens-main">No prop selected</div>
            </div>
            <span class="cards-lens-badge is-live">Live</span>
          </div>
          <div class="cards-callout-copy">${escapeHtml(currentLiveRows.length ? 'No current live or pregame props matched the active filters for this game.' : 'All current live prop opportunities for this game are already decided and no pregame props matched the active filters.')}</div>`;
        return;
      }

      const visibleRows = filteredLiveRows
        .concat(filteredPitcherRows)
        .concat(filteredHitterRows)
        .concat(filteredExtraPitcherRows)
        .concat(filteredExtraHitterRows);
      let selected = visibleRows.find((row) => propKey(row) === detail.selectedPropKey)
        || filteredLiveRows[0]
        || filteredPitcherRows[0]
        || filteredHitterRows[0]
        || filteredExtraPitcherRows[0]
        || filteredExtraHitterRows[0]
        || null;
      const selectedKey = selected ? propKey(selected) : detail.selectedPropKey;
      if (selected) detail.selectedPropKey = selectedKey;

      groupsNode.innerHTML = `
        ${filteredLiveRows.length ? `
          <div class="cards-prop-group">
            <div class="cards-box-head">
              <div class="cards-table-title"><strong>Live opportunities</strong></div>
              <span class="cards-chip is-live">${escapeHtml(String(filteredLiveRows.length))} plays</span>
            </div>
            <div class="cards-callout-copy">Current market odds ranked by live projection first, then model-vs-market edge.</div>
            ${propButtonMarkup(filteredLiveRows, selectedKey, "live")}
          </div>` : ""}
        ${filteredOfficialCount ? `
          <div class="cards-prop-group">
            <div class="cards-box-head">
              <div class="cards-table-title"><strong>Official picks</strong></div>
              <span class="cards-chip is-official">${escapeHtml(String(filteredOfficialCount))} plays</span>
            </div>
            ${filteredPitcherRows.length ? `
              <div class="cards-prop-stack">
                <div class="cards-section-label">Pitcher props</div>
                ${propButtonMarkup(filteredPitcherRows, selectedKey, "official")}
              </div>` : ""}
            ${filteredHitterRows.length ? `
              <div class="cards-prop-stack">
                <div class="cards-section-label">Hitter props</div>
                ${propButtonMarkup(filteredHitterRows, selectedKey, "official")}
              </div>` : ""}
          </div>` : ""}
        ${filteredExtraCount ? `
          <div class="cards-prop-group is-secondary">
            <div class="cards-box-head">
              <div class="cards-table-title"><strong>Other playable props</strong></div>
              <span class="cards-chip is-candidate">${escapeHtml(String(filteredExtraCount))} plays</span>
            </div>
            <div class="cards-callout-copy">Qualified lanes that did not make the official card after caps and one-prop-per-player selection.</div>
            ${filteredExtraPitcherRows.length ? `
              <div class="cards-prop-stack">
                <div class="cards-section-label">Pitcher props</div>
                ${propButtonMarkup(filteredExtraPitcherRows, selectedKey, "candidate")}
              </div>` : ""}
            ${filteredExtraHitterRows.length ? `
              <div class="cards-prop-stack">
                <div class="cards-section-label">Hitter props</div>
                ${propButtonMarkup(filteredExtraHitterRows, selectedKey, "candidate")}
              </div>` : ""}
          </div>` : ""}`;

      const stateObj = propLensState(card, detail, selected);
      const rows = stateObj.rows;
      const actualValue = stateObj.actualValue;
      const simValue = stateObj.simValue;
      const modelMean = stateObj.modelMean;
      const liveProjection = stateObj.liveProjection;
      const liveEdge = stateObj.liveEdge;
      const badge = stateObj.badge;

      const actualLabel = actualValue == null ? "-" : `${actualValue} ${metricLabel(selected)}`;
      const simLabel = simValue == null ? "-" : `${simValue} ${metricLabel(selected)}`;
      const lineLabel = `${String(selected?.selection || "over").replace(/^./, (m) => m.toUpperCase())} ${formatLine(selected?.market_line)}`;
      const tierLabel = stateObj.tierLabel;

      const detailPairs = [
        { label: "Tier", value: tierLabel },
        { label: "Actual", value: actualLabel },
        { label: "Sim row", value: simLabel },
        { label: "Model mean", value: modelMean == null ? "-" : `${formatLine(modelMean)} ${metricLabel(selected)}` },
        { label: "Live proj", value: liveProjection == null ? "-" : `${formatLine(liveProjection)} ${metricLabel(selected)}` },
        { label: "Live since", value: selected?.first_seen_at ? formatTimestampShort(selected.first_seen_at) : "-" },
        { label: "Opened at", value: selected?.first_seen_odds != null ? formatOdds(selected.first_seen_odds) : formatOdds(selected?.odds) },
        { label: "Line", value: lineLabel },
        { label: "Live edge", value: liveEdge == null ? "-" : formatSigned(liveEdge, 2) },
        { label: "Odds", value: formatOdds(selected?.odds) },
        { label: "Edge", value: formatPropEdge(selected) },
        {
          label: "Model",
          value: selected?.outs_mean != null
            ? `${formatLine(selected.outs_mean)} outs mean`
            : `${formatPercent(selected?.model_prob_over, 1)} over`,
        },
      ];

      const actualColumns = rows.isPitcher ? ACTUAL_PITCHING_COLUMNS : ACTUAL_BATTING_COLUMNS;
      const simColumns = rows.isPitcher ? simPitchingColumns(detail.sim) : simBattingColumns(detail.sim);

      lensNode.innerHTML = `
        <div class="cards-lens-head">
          <div>
            <div class="cards-lens-label">Prop lens</div>
            <div class="cards-lens-main">${escapeHtml(propOwnerName(selected))} - ${escapeHtml(marketLabelLong(selected))}</div>
            <div class="cards-subcopy">${escapeHtml(tierLabel)} | ${escapeHtml(card?.away?.abbr || "Away")} at ${escapeHtml(card?.home?.abbr || "Home")}</div>
          </div>
          <span class="cards-lens-badge ${escapeHtml(badge.className)}">${escapeHtml(badge.label)}</span>
        </div>

        <div class="cards-detail-grid">
          ${detailPairs
            .map((pair) => `
              <div class="cards-data-pair">
                <span>${escapeHtml(pair.label)}</span>
                <strong>${escapeHtml(pair.value)}</strong>
              </div>`)
            .join("")}
        </div>

        <div class="cards-box-grid">
          <div class="cards-panel-card cards-prop-stack">
            <div class="cards-table-head"><div class="cards-table-title">Live / final player row</div></div>
            ${rows.actualRow ? renderTableHtml(actualColumns, [rows.actualRow]) : '<div class="cards-empty-copy">No actual boxscore row matched this player yet.</div>'}
          </div>
          <div class="cards-panel-card cards-prop-stack">
            <div class="cards-table-head"><div class="cards-table-title">Sim player row</div></div>
            ${rows.simRow ? renderTableHtml(simColumns, [rows.simRow]) : '<div class="cards-empty-copy">No sim row matched this player.</div>'}
          </div>
        </div>`;
      return;
      }
    }

    const filteredPitcherRows = filteredPropRows(pitcherRows, filters);
    const filteredHitterRows = filteredPropRows(hitterRows, filters);
    const filteredExtraPitcherRows = filteredPropRows(extraPitcherRows, filters);
    const filteredExtraHitterRows = filteredPropRows(extraHitterRows, filters);
    const filteredOfficialCount = filteredPitcherRows.length + filteredHitterRows.length;
    const filteredExtraCount = filteredExtraPitcherRows.length + filteredExtraHitterRows.length;
    const visibleRows = filteredPitcherRows
      .concat(filteredHitterRows)
      .concat(filteredExtraPitcherRows)
      .concat(filteredExtraHitterRows);
    const filtersApplied = filters.side !== "all" || filters.type !== "all";

    filtersNode.innerHTML = officialCount || extraCount ? renderPropFilterControls(allPropRows(card), detail, { boardOptions }) : "";
    summaryChip.textContent = filtersApplied
      ? (filteredOfficialCount || filteredExtraCount ? propCountBadge(filteredOfficialCount, filteredExtraCount) : "No matches")
      : propCountBadge(officialCount, extraCount);

    if (!officialCount && !extraCount) {
      filtersNode.innerHTML = "";
      groupsNode.innerHTML = '<div class="cards-empty-copy">No playable player props for this matchup.</div>';
      lensNode.innerHTML = `
        <div class="cards-lens-head">
          <div>
            <div class="cards-lens-label">Prop lens</div>
            <div class="cards-lens-main">No player lens available</div>
          </div>
          <span class="cards-lens-badge is-live">Off card</span>
        </div>
        <div class="cards-callout-copy">This game has model probabilities and game-market tiles, but no official or secondary player props to drill into.</div>`;
      return;
    }

    if (!filteredOfficialCount && !filteredExtraCount) {
      groupsNode.innerHTML = '<div class="cards-empty-copy">No official or playable props match the current side and prop-type filters.</div>';
      lensNode.innerHTML = `
        <div class="cards-lens-head">
          <div>
            <div class="cards-lens-label">Prop lens</div>
            <div class="cards-lens-main">No filtered prop selected</div>
          </div>
          <span class="cards-lens-badge is-live">Refine filters</span>
        </div>
        <div class="cards-callout-copy">No official or playable props matched the current side and prop-type filters for this game.</div>`;
      return;
    }

    let selected = visibleRows.find((row) => propKey(row) === detail.selectedPropKey)
      || filteredPitcherRows[0]
      || filteredHitterRows[0]
      || filteredExtraPitcherRows[0]
      || filteredExtraHitterRows[0]
      || null;
    const selectedKey = selected ? propKey(selected) : detail.selectedPropKey;
    if (selected) detail.selectedPropKey = selectedKey;

    groupsNode.innerHTML = `
      ${filteredOfficialCount ? `
        <div class="cards-prop-group">
          <div class="cards-box-head">
            <div class="cards-table-title"><strong>Official picks</strong></div>
            <span class="cards-chip is-official">${escapeHtml(String(filteredOfficialCount))} plays</span>
          </div>
          ${filteredPitcherRows.length ? `
            <div class="cards-prop-stack">
              <div class="cards-section-label">Pitcher props</div>
              ${propButtonMarkup(filteredPitcherRows, selectedKey, "official")}
            </div>` : ""}
          ${filteredHitterRows.length ? `
            <div class="cards-prop-stack">
              <div class="cards-section-label">Hitter props</div>
              ${propButtonMarkup(filteredHitterRows, selectedKey, "official")}
            </div>` : ""}
        </div>` : ""}
      ${filteredExtraCount ? `
        <div class="cards-prop-group is-secondary">
          <div class="cards-box-head">
            <div class="cards-table-title"><strong>Other playable props</strong></div>
            <span class="cards-chip is-candidate">${escapeHtml(String(filteredExtraCount))} plays</span>
          </div>
          <div class="cards-callout-copy">Qualified lanes that did not make the official card after caps and one-prop-per-player selection.</div>
          ${filteredExtraPitcherRows.length ? `
            <div class="cards-prop-stack">
              <div class="cards-section-label">Pitcher props</div>
              ${propButtonMarkup(filteredExtraPitcherRows, selectedKey, "candidate")}
            </div>` : ""}
          ${filteredExtraHitterRows.length ? `
            <div class="cards-prop-stack">
              <div class="cards-section-label">Hitter props</div>
              ${propButtonMarkup(filteredExtraHitterRows, selectedKey, "candidate")}
            </div>` : ""}
        </div>` : ""}`;

    if (!selected) {
      lensNode.innerHTML = `
        <div class="cards-lens-head">
          <div>
            <div class="cards-lens-label">Prop lens</div>
            <div class="cards-lens-main">No filtered prop selected</div>
          </div>
          <span class="cards-lens-badge is-live">Refine filters</span>
        </div>
        <div class="cards-callout-copy">No official or playable props matched the current side and prop-type filters for this game.</div>`;
      return;
    }

    const stateObj = propLensState(card, detail, selected);
    const rows = stateObj.rows;
    const actualValue = stateObj.actualValue;
    const simValue = stateObj.simValue;
    const modelMean = stateObj.modelMean;
    const liveProjection = stateObj.liveProjection;
    const liveEdge = stateObj.liveEdge;
    const badge = stateObj.badge;

    const actualLabel = actualValue == null ? "-" : `${actualValue} ${metricLabel(selected)}`;
    const simLabel = simValue == null ? "-" : `${simValue} ${metricLabel(selected)}`;
    const lineLabel = `${String(selected?.selection || "over").replace(/^./, (m) => m.toUpperCase())} ${formatLine(selected?.market_line)}`;
    const tierLabel = stateObj.tierLabel;

    const detailPairs = [
      { label: "Tier", value: tierLabel },
      { label: "Actual", value: actualLabel },
      { label: "Sim row", value: simLabel },
      { label: "Model mean", value: modelMean == null ? "-" : `${formatLine(modelMean)} ${metricLabel(selected)}` },
      { label: "Live proj", value: liveProjection == null ? "-" : `${formatLine(liveProjection)} ${metricLabel(selected)}` },
      { label: "Live since", value: selected?.first_seen_at ? formatTimestampShort(selected.first_seen_at) : "-" },
      { label: "Opened at", value: selected?.first_seen_odds != null ? formatOdds(selected.first_seen_odds) : formatOdds(selected?.odds) },
      { label: "Line", value: lineLabel },
      { label: "Live edge", value: liveEdge == null ? "-" : formatSigned(liveEdge, 2) },
      { label: "Odds", value: formatOdds(selected?.odds) },
      { label: "Edge", value: formatPropEdge(selected) },
      {
        label: "Model",
        value: selected?.outs_mean != null
          ? `${formatLine(selected.outs_mean)} outs mean`
          : `${formatPercent(selected?.model_prob_over, 1)} over`,
      },
    ];

    const actualColumns = rows.isPitcher ? ACTUAL_PITCHING_COLUMNS : ACTUAL_BATTING_COLUMNS;
    const simColumns = rows.isPitcher ? simPitchingColumns(detail.sim) : simBattingColumns(detail.sim);

    lensNode.innerHTML = `
      <div class="cards-lens-head">
        <div>
          <div class="cards-lens-label">Prop lens</div>
          <div class="cards-lens-main">${escapeHtml(propOwnerName(selected))} - ${escapeHtml(marketLabelLong(selected))}</div>
          <div class="cards-subcopy">${escapeHtml(tierLabel)} | ${escapeHtml(card?.away?.abbr || "Away")} at ${escapeHtml(card?.home?.abbr || "Home")}</div>
        </div>
        <span class="cards-lens-badge ${escapeHtml(badge.className)}">${escapeHtml(badge.label)}</span>
      </div>

      <div class="cards-detail-grid">
        ${detailPairs
          .map((pair) => `
            <div class="cards-data-pair">
              <span>${escapeHtml(pair.label)}</span>
              <strong>${escapeHtml(pair.value)}</strong>
            </div>`)
          .join("")}
      </div>

      <div class="cards-box-grid">
        <div class="cards-panel-card cards-prop-stack">
          <div class="cards-table-head"><div class="cards-table-title">Live / final player row</div></div>
          ${rows.actualRow ? renderTableHtml(actualColumns, [rows.actualRow]) : '<div class="cards-empty-copy">No actual boxscore row matched this player yet.</div>'}
        </div>
        <div class="cards-panel-card cards-prop-stack">
          <div class="cards-table-head"><div class="cards-table-title">Sim player row</div></div>
          ${rows.simRow ? renderTableHtml(simColumns, [rows.simRow]) : '<div class="cards-empty-copy">No sim row matched this player.</div>'}
        </div>
      </div>`;
  }

  function selectProp(card, propKeyValue) {
    const detail = ensureDetail(card);
    detail.selectedPropKey = propKeyValue;
    const node = state.cardNodes.get(Number(card.gamePk));
    if (node) renderPropSections(card, node);
  }

  function statusDetailText(snapshot, card) {
    const st = snapshot?.status || {};
    const abstract = String(st.abstractGameState || card?.status?.abstract || "").trim();
    const detailed = String(st.detailedState || card?.status?.detailed || "").trim();
    if (detailed && detailed.toLowerCase() !== abstract.toLowerCase()) return detailed;
    return "";
  }

  function liveSummary(snapshot, card) {
    if (!snapshot || !snapshot.teams) return card?.startTime ? `Scheduled - ${card.startTime}` : "Live box unavailable";
    const statusText = String(snapshot?.status?.abstractGameState || card?.status?.abstract || "");
    const awayRuns = snapshot?.teams?.away?.totals?.R;
    const homeRuns = snapshot?.teams?.home?.totals?.R;
    const scoreLine = `${card?.away?.abbr || "Away"} ${awayRuns ?? "-"} - ${card?.home?.abbr || "Home"} ${homeRuns ?? "-"}`;
    if (statusText.toLowerCase() === "live") {
      const inning = snapshot?.current?.inning;
      const half = String(snapshot?.current?.halfInning || "");
      const batter = snapshot?.current?.batter?.fullName || "";
      const pitcher = snapshot?.current?.pitcher?.fullName || "";
      const matchup = batter || pitcher ? `${batter}${batter && pitcher ? " vs " : ""}${pitcher}` : "";
      const inningLine = inning ? `${half} ${inning}` : "Live";
      return `${scoreLine} | ${inningLine}${matchup ? ` | ${matchup}` : ""}`;
    }
    if (statusText.toLowerCase() === "final") return `Final | ${scoreLine}`;
    return `${statusText || "Scheduled"} | ${scoreLine}`;
  }

  function simSummary(sim, card) {
    if (!sim || sim.found === false) return "Sim unavailable";
    const away = sim?.predicted?.away;
    const home = sim?.predicted?.home;
    if (away == null || home == null) return "Sim loaded";
    const prefix = sim?.boxscoreMode === "aggregate"
      ? `Sim mean${sim?.simCount ? ` (${sim.simCount})` : ""}`
      : "Sim final";
    return `${prefix} | ${card?.away?.abbr || "Away"} ${away} - ${card?.home?.abbr || "Home"} ${home}`;
  }

  function createStripNode(card) {
    const anchor = document.createElement("a");
    anchor.className = "cards-strip-card";
    anchor.href = `#game-card-${encodeURIComponent(card.gamePk)}`;
    anchor.dataset.gamePk = String(card.gamePk);
    anchor.innerHTML = `
      <div class="cards-strip-head">
        <span>${escapeHtml(card.status?.abstract || "Game")}</span>
        <span data-role="strip-detail">${escapeHtml(statusDetailText(null, card) || card.startTime || "")}</span>
      </div>
      <div class="cards-linescore is-compact">
        <div class="cards-linescore-head">
          <span class="cards-linescore-team-label">Team</span>
          <span class="cards-linescore-stat-head">R</span>
          <span class="cards-linescore-stat-head">H</span>
          <span class="cards-linescore-stat-head">E</span>
        </div>
        <div class="cards-linescore-row">
          <div class="cards-linescore-team">
            ${teamLogoMarkup(card.away, "cards-strip-logo")}
            <strong>${escapeHtml(card.away?.abbr || "Away")}</strong>
          </div>
          <span class="cards-linescore-stat" data-role="strip-away-r">-</span>
          <span class="cards-linescore-stat" data-role="strip-away-h">-</span>
          <span class="cards-linescore-stat" data-role="strip-away-e">-</span>
        </div>
        <div class="cards-linescore-row">
          <div class="cards-linescore-team">
            ${teamLogoMarkup(card.home, "cards-strip-logo")}
            <strong>${escapeHtml(card.home?.abbr || "Home")}</strong>
          </div>
          <span class="cards-linescore-stat" data-role="strip-home-r">-</span>
          <span class="cards-linescore-stat" data-role="strip-home-h">-</span>
          <span class="cards-linescore-stat" data-role="strip-home-e">-</span>
        </div>
      </div>
      <div class="cards-strip-meta">${escapeHtml(marketCountSummary(card))}</div>`;
    return anchor;
  }

  function updateDateControls() {
    const currentDate = String(state.date || "").trim();
    const nav = state.payload?.nav || {};
    if (root.dateBadge) root.dateBadge.textContent = currentDate || "Slate";
    if (root.dateInput) {
      root.dateInput.value = currentDate;
      root.dateInput.min = String(nav.minDate || "").trim();
      root.dateInput.max = String(nav.maxDate || "").trim();
    }

    const prevDate = String(nav.prevDate || "").trim() || shiftIsoDate(currentDate, -1);
    const nextDate = String(nav.nextDate || "").trim() || shiftIsoDate(currentDate, 1);
    if (root.prevDateLink) {
      root.prevDateLink.href = prevDate ? `/?date=${encodeURIComponent(prevDate)}` : "/";
      root.prevDateLink.classList.toggle("is-disabled", !prevDate);
      root.prevDateLink.setAttribute("aria-disabled", prevDate ? "false" : "true");
    }
    if (root.nextDateLink) {
      root.nextDateLink.href = nextDate ? `/?date=${encodeURIComponent(nextDate)}` : "/";
      root.nextDateLink.classList.toggle("is-disabled", !nextDate);
      root.nextDateLink.setAttribute("aria-disabled", nextDate ? "false" : "true");
    }
  }

  function renderHeaderMeta() {
    if (!root.headerMeta) return;
    if (!state.cards.length) {
      root.headerMeta.textContent = `No games scheduled for ${state.date || "the selected date"}.`;
      return;
    }

    const counts = slateCounts(state.cards);
    const parts = [`${state.cards.length} ${state.cards.length === 1 ? "game" : "games"} on the slate`];
    if (counts.officialCount) parts.push(`${counts.officialCount} with official plays`);
    else if (!state.payload?.hasSampleData) parts.push("schedule only");
    if (counts.liveCount) parts.push(`${counts.liveCount} live`);
    if (counts.finalCount) parts.push(`${counts.finalCount} final`);
    root.headerMeta.textContent = parts.join(" | ");
  }

  function renderSourceMeta() {
    if (!root.sourceMeta) return;
    const payload = state.payload || {};
    const counts = slateCounts(state.cards);
    const pills = [];
    const currentDate = payload.date || state.date;
    const marketAvailability = payload.marketAvailability || {};
    const gameLines = marketAvailability.gameLines || {};
    const pitcherProps = marketAvailability.pitcherProps || {};
    const hitterProps = marketAvailability.hitterProps || {};
    const lineupHealth = payload.lineupHealth || {};
    const workflow = payload.workflow || {};
    const marketWarnings = Array.isArray(marketAvailability.warnings) ? marketAvailability.warnings : [];

    if (currentDate) pills.push(`<span class="cards-source-pill">${escapeHtml(currentDate)}</span>`);
    pills.push(sourceMetaPill(`${String(state.cards.length)} games`));

    const statusBits = [];
    if (counts.upcomingCount) statusBits.push(`${counts.upcomingCount} upcoming`);
    if (counts.liveCount) statusBits.push(`${counts.liveCount} live`);
    if (counts.finalCount) statusBits.push(`${counts.finalCount} final`);
    if (statusBits.length) pills.push(sourceMetaPill(statusBits.join(" / ")));
    if (counts.officialCount) pills.push(sourceMetaPill(`${counts.officialCount} with official plays`));
    if (counts.simCount) pills.push(sourceMetaPill(`${counts.simCount} ${counts.simCount === 1 ? "game" : "games"} with sim projections`));
    if (Number(workflow.simsPerGame || 0) > 0) pills.push(sourceMetaPill(`${workflow.simsPerGame} sims/game`));

    if (gameLines.exists) {
      const lineCounts = gameLines.counts || {};
      const lineParts = [];
      if (Number(lineCounts.h2h_games || 0) > 0) lineParts.push(`ML ${lineCounts.h2h_games}`);
      if (Number(lineCounts.totals_games || 0) > 0) lineParts.push(`Tot ${lineCounts.totals_games}`);
      if (Number(lineCounts.spreads_games || 0) > 0) lineParts.push(`Spr ${lineCounts.spreads_games}`);
      pills.push(sourceMetaPill(lineParts.join(" / ") || "Lines file present", gameLines.available ? "success" : "warning"));
    } else if (payload.hasSampleData) {
      pills.push(sourceMetaPill("Markets pending", "warning"));
    }

    if (pitcherProps.exists) {
      const players = Number((pitcherProps.counts || {}).players || 0);
      pills.push(sourceMetaPill(`Pitcher props ${players}`, players > 0 ? "success" : "warning"));
    }
    if (hitterProps.exists) {
      const players = Number((hitterProps.counts || {}).players || 0);
      pills.push(sourceMetaPill(`Hitter props ${players}`, players > 0 ? "success" : "warning"));
    }
    if (marketWarnings.length) pills.push(sourceMetaPill(marketWarnings[0], "warning"));

    if (Number(lineupHealth.adjustedTeams || 0) > 0) {
      pills.push(sourceMetaPill(`Lineups adjusted ${lineupHealth.adjustedTeams}`, "warning"));
    }
    if (Number(lineupHealth.partialTeams || 0) > 0) {
      pills.push(sourceMetaPill(`Lineups partial ${lineupHealth.partialTeams}`, "warning"));
    }
    if (Number(workflow.warningCount || 0) > 0) {
      pills.push(sourceMetaPill(`Workflow warnings ${workflow.warningCount}`, "warning"));
    }
    if (Number(workflow.errorCount || 0) > 0) {
      pills.push(sourceMetaPill(`Workflow errors ${workflow.errorCount}`, "danger"));
    }

    if (payload?.view?.mode === "season_archive") pills.push(sourceMetaPill("Season archive", "soft"));
    if (!payload.hasSampleData) pills.push(sourceMetaPill("Schedule only", "soft"));

    root.sourceMeta.innerHTML = pills.join("");
  }

  function renderFilters() {
    if (!root.filters) return;
    const filters = buildFilters(state.cards);
    root.filters.innerHTML = filters
      .map((filter) => `
        <button type="button" class="cards-filter-pill ${filter.key === state.filter ? "is-active" : ""}" data-filter-key="${escapeHtml(filter.key)}">
          ${escapeHtml(filter.label)}
        </button>`)
      .join("");
    root.filters.querySelectorAll("[data-filter-key]").forEach((button) => {
      button.addEventListener("click", function () {
        state.filter = button.getAttribute("data-filter-key") || "all";
        renderFilters();
        applyFilter();
      });
    });
  }

  function renderScoreboard() {
    if (!root.scoreboard) return;
    root.scoreboard.innerHTML = "";
    state.stripNodes.clear();
    state.cards.forEach((card) => {
      const node = createStripNode(card);
      state.stripNodes.set(Number(card.gamePk), node);
      root.scoreboard.appendChild(node);
    });
    applyFilter();
  }

  function renderCards() {
    if (!root.grid) return;
    root.grid.innerHTML = "";
    state.cardNodes.clear();
    state.cards.forEach((card) => {
      const node = createCardNode(card);
      state.cardNodes.set(Number(card.gamePk), node);
      root.grid.appendChild(node);
    });
    applyFilter();
  }

  function applyFilter() {
    state.cards.forEach((card) => {
      const visible = matchesFilter(card, state.filter);
      const cardNode = state.cardNodes.get(Number(card.gamePk));
      const stripNode = state.stripNodes.get(Number(card.gamePk));
      if (cardNode) cardNode.hidden = !visible;
      if (stripNode) stripNode.hidden = !visible;
    });
  }

  async function fetchJson(url) {
    const response = await fetch(url, { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return await response.json();
  }

  async function loadSnapshot(card, isRefresh) {
    const detail = ensureDetail(card);
    try {
      const snapshot = await fetchJson(`/api/game/${encodeURIComponent(card.gamePk)}/snapshot?date=${encodeURIComponent(state.date)}`);
      detail.snapshot = snapshot;
      syncCard(card);
      if (!isRefresh) maybeStartPolling(card, snapshot);
    } catch (_error) {
      if (!isRefresh) syncCard(card);
    }
  }

  async function loadSim(card) {
    const detail = ensureDetail(card);
    try {
      const sim = await fetchJson(`/api/game/${encodeURIComponent(card.gamePk)}/sim?date=${encodeURIComponent(state.date)}`);
      detail.sim = sim;
      syncCard(card);
    } catch (_error) {
      detail.sim = { found: false };
      syncCard(card);
    }
  }

  function maybeStartPolling(card, snapshot) {
    const gamePk = Number(card.gamePk);
    const isLive = String(snapshot?.status?.abstractGameState || "").toLowerCase() === "live";
    if (!isLive || state.livePollers.has(gamePk)) return;
    const handle = window.setInterval(() => {
      loadSnapshot(card, true);
    }, 20000);
    state.livePollers.set(gamePk, handle);
  }

  function syncStrip(card, detail) {
    const stripNode = state.stripNodes.get(Number(card.gamePk));
    if (!stripNode) return;
    const awayRuns = stripNode.querySelector('[data-role="strip-away-r"]');
    const awayHits = stripNode.querySelector('[data-role="strip-away-h"]');
    const awayErrors = stripNode.querySelector('[data-role="strip-away-e"]');
    const homeRuns = stripNode.querySelector('[data-role="strip-home-r"]');
    const homeHits = stripNode.querySelector('[data-role="strip-home-h"]');
    const homeErrors = stripNode.querySelector('[data-role="strip-home-e"]');
    const detailNode = stripNode.querySelector('[data-role="strip-detail"]');
    const snapshot = detail.snapshot;
    if (awayRuns) awayRuns.textContent = linescoreValue(snapshot?.teams?.away?.totals?.R);
    if (awayHits) awayHits.textContent = linescoreValue(snapshot?.teams?.away?.totals?.H);
    if (awayErrors) awayErrors.textContent = linescoreValue(snapshot?.teams?.away?.totals?.E);
    if (homeRuns) homeRuns.textContent = linescoreValue(snapshot?.teams?.home?.totals?.R);
    if (homeHits) homeHits.textContent = linescoreValue(snapshot?.teams?.home?.totals?.H);
    if (homeErrors) homeErrors.textContent = linescoreValue(snapshot?.teams?.home?.totals?.E);
    if (detailNode) detailNode.textContent = statusDetailText(snapshot, card) || card.startTime || "";
  }

  function syncCard(card) {
    const node = state.cardNodes.get(Number(card.gamePk));
    const detail = ensureDetail(card);
    if (!node) return;

    const statusBadge = node.querySelector('[data-role="status-badge"]');
    const statusDetail = node.querySelector('[data-role="status-detail"]');
    const awayScore = node.querySelector('[data-role="away-score"]');
    const homeScore = node.querySelector('[data-role="home-score"]');
    const liveLine = node.querySelector('[data-role="live-line"]');
    const simLine = node.querySelector('[data-role="sim-line"]');
    const gameLens = node.querySelector('[data-role="game-lens"]');
    const propOverviewLens = node.querySelector('[data-role="prop-overview-lens"]');
    const snapshot = detail.snapshot;
    const sim = detail.sim;

    if (statusBadge) {
      const text = snapshot?.status?.abstractGameState || card?.status?.abstract || "Game";
      statusBadge.textContent = text;
      statusBadge.className = `cards-status-badge ${statusClass(text)}`.trim();
    }
    if (statusDetail) statusDetail.textContent = statusDetailText(snapshot, card) || card.startTime || "";
    if (awayScore) awayScore.textContent = snapshot?.teams?.away?.totals?.R ?? "-";
    if (homeScore) homeScore.textContent = snapshot?.teams?.home?.totals?.R ?? "-";
    if (liveLine) liveLine.textContent = liveSummary(snapshot, card);
    if (simLine) simLine.textContent = simSummary(sim, card);
    if (gameLens) gameLens.innerHTML = renderGameLens(card, detail);
    if (propOverviewLens) propOverviewLens.innerHTML = renderPropOverviewLens(card, detail);

    renderActualBox(card, detail, node);
    renderSimBox(card, detail, node);
    renderPropSections(card, node);
    syncStrip(card, detail);
  }

  async function hydrateCards() {
    await Promise.all(
      state.cards.map(async (card) => {
        await Promise.allSettled([loadSnapshot(card, false), loadSim(card)]);
      })
    );
  }

  function sameSlate(nextCards, currentCards) {
    const left = Array.isArray(currentCards) ? currentCards : [];
    const right = Array.isArray(nextCards) ? nextCards : [];
    if (left.length !== right.length) return false;
    for (let index = 0; index < left.length; index += 1) {
      if (Number(left[index]?.gamePk) !== Number(right[index]?.gamePk)) return false;
    }
    return true;
  }

  async function loadCards(options = {}) {
    const silent = !!options.silent;
    if (state.loadingCards) return;
    state.loadingCards = true;
    if (!silent) {
      if (root.grid) root.grid.innerHTML = '<div class="cards-loading-state">Loading cards...</div>';
      if (root.scoreboard) root.scoreboard.innerHTML = '<div class="cards-loading-strip">Loading scoreboard...</div>';
    }

    try {
      const payload = await fetchJson(`/api/cards?date=${encodeURIComponent(state.date)}`);
      const nextPayload = payload || {};
      const nextDate = String(nextPayload?.date || state.date || "");
      const nextCards = Array.isArray(nextPayload?.cards) ? nextPayload.cards : [];
      const slateUnchanged = sameSlate(nextCards, state.cards);

      state.payload = nextPayload;
      state.date = nextDate;
      state.cards = nextCards;

      updateDateControls();
      renderHeaderMeta();
      renderSourceMeta();
      renderFilters();

      if (!silent || !slateUnchanged) {
        renderScoreboard();
        renderCards();
      } else {
        applyFilter();
      }

      if (!state.cards.length && root.grid) {
        root.grid.innerHTML = `<div class="cards-empty-state">No games available for ${escapeHtml(state.date)}.</div>`;
        return;
      }

      await hydrateCards();
    } catch (error) {
      const message = error && error.message ? error.message : "Unknown error";
      if (root.headerMeta) {
        root.headerMeta.textContent = `Failed to load ${state.date || "the selected slate"}.`;
      }
      if (root.sourceMeta) {
        root.sourceMeta.innerHTML = "<span>Load failed</span>";
      }
      if (root.grid) {
        root.grid.innerHTML = `<div class="cards-empty-state">Failed to load cards.<div class="cards-mini-copy">${escapeHtml(message)}</div></div>`;
      }
      if (root.scoreboard) {
        root.scoreboard.innerHTML = `<div class="cards-loading-strip">Failed to load scoreboard.</div>`;
      }
    } finally {
      state.loadingCards = false;
    }
  }

  function installAutoRefresh() {
    if (state.autoRefreshHandle) {
      window.clearInterval(state.autoRefreshHandle);
    }
    state.autoRefreshHandle = window.setInterval(() => {
      loadCards({ silent: true });
    }, AUTO_REFRESH_MS);
  }

  function installErrorHandlers() {
    window.addEventListener("error", function (event) {
      if (!event || (!event.error && !event.message)) return;
      if (!root.grid) return;
      const message = event?.error?.message || event?.message || "Unknown client error";
      if (!message) return;
      root.grid.innerHTML = `<div class="cards-empty-state">Render error<div class="cards-mini-copy">${escapeHtml(message)}</div></div>`;
    });

    window.addEventListener("unhandledrejection", function (event) {
      if (!root.grid) return;
      const reason = event?.reason;
      const message = reason && reason.message ? reason.message : String(reason || "Unknown async error");
      root.grid.innerHTML = `<div class="cards-empty-state">Render error<div class="cards-mini-copy">${escapeHtml(message)}</div></div>`;
    });
  }

  installErrorHandlers();
  updateDateControls();
  installAutoRefresh();
  loadCards({ silent: false });
})();