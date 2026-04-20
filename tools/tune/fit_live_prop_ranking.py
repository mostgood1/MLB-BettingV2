from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.live_prop_ranking import DEFAULT_LIVE_PROP_FEATURES, build_live_prop_feature_map
from tools.web.flask_frontend import _live_stat_value, _load_live_lens_snapshot, _lookup_boxscore_row


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(path)


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(dict(row), separators=(",", ":")))
            handle.write("\n")
    tmp.replace(path)


def _safe_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number):
        return None
    return float(number)


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None


def _clip_prob(p: float, eps: float = 1e-12) -> float:
    return float(min(1.0 - eps, max(eps, float(p))))


def _sigmoid(x: float) -> float:
    try:
        return float(1.0 / (1.0 + math.exp(-float(x))))
    except OverflowError:
        return 0.0 if float(x) < 0.0 else 1.0


def _logloss(ps: Sequence[float], ys: Sequence[int], ws: Optional[Sequence[float]] = None) -> float:
    weights = list(ws or [1.0] * len(ps))
    total = 0.0
    denom = 0.0
    for p, y, w in zip(ps, ys, weights):
        if not math.isfinite(float(w)) or float(w) <= 0.0:
            continue
        pp = _clip_prob(float(p))
        yy = 1 if int(y) == 1 else 0
        total += float(w) * (-(yy * math.log(pp) + (1 - yy) * math.log(1.0 - pp)))
        denom += float(w)
    return float(total / max(1e-12, denom))


def _brier(ps: Sequence[float], ys: Sequence[int], ws: Optional[Sequence[float]] = None) -> float:
    weights = list(ws or [1.0] * len(ps))
    total = 0.0
    denom = 0.0
    for p, y, w in zip(ps, ys, weights):
        if not math.isfinite(float(w)) or float(w) <= 0.0:
            continue
        yy = 1.0 if int(y) == 1 else 0.0
        total += float(w) * ((float(p) - yy) ** 2)
        denom += float(w)
    return float(total / max(1e-12, denom))


def _gaussian_solve(matrix: List[List[float]], vector: List[float]) -> Optional[List[float]]:
    n = len(vector)
    aug = [list(matrix[i]) + [float(vector[i])] for i in range(n)]
    for col in range(n):
        pivot = max(range(col, n), key=lambda row: abs(aug[row][col]))
        if abs(aug[pivot][col]) < 1e-12:
            return None
        if pivot != col:
            aug[col], aug[pivot] = aug[pivot], aug[col]
        pivot_val = aug[col][col]
        for j in range(col, n + 1):
            aug[col][j] /= pivot_val
        for row in range(n):
            if row == col:
                continue
            factor = aug[row][col]
            if factor == 0.0:
                continue
            for j in range(col, n + 1):
                aug[row][j] -= factor * aug[col][j]
    return [float(aug[i][n]) for i in range(n)]


def _result_label(selection: Any, line: Any, actual: Any) -> str:
    line_value = _safe_float(line)
    actual_value = _safe_float(actual)
    if line_value is None or actual_value is None:
        return "pending"
    if abs(float(actual_value) - float(line_value)) < 1e-9:
        return "push"
    selection_text = str(selection or "").strip().lower()
    if selection_text == "under":
        return "win" if float(actual_value) < float(line_value) else "loss"
    return "win" if float(actual_value) > float(line_value) else "loss"


def _load_first_observations(observation_path: Path) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    if not observation_path.exists() or not observation_path.is_file():
        return out
    try:
        lines = observation_path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return out
    for line in lines:
        text = str(line or "").strip()
        if not text:
            continue
        try:
            row = json.loads(text)
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        key = str(row.get("key") or "").strip()
        if key and key not in out:
            out[key] = row
    return out


def _final_actual_value(snapshot: Dict[str, Any], owner: str, market: str, prop: str) -> Optional[float]:
    row_type = "pitching" if str(market or "").strip().lower() == "pitcher_props" else "batting"
    teams = (snapshot or {}).get("teams") if isinstance(snapshot, dict) else {}
    actual_row = None
    for side in ("away", "home"):
        candidate = _lookup_boxscore_row((((teams.get(side) or {}).get("boxscore") or {}).get(row_type) or []), owner)
        if candidate:
            actual_row = candidate
            break
    return _safe_float(_live_stat_value(actual_row, {"market": market, "prop": prop}))


def _build_first_observation_row(
    *,
    date_str: str,
    key: str,
    entry: Dict[str, Any],
    observation: Dict[str, Any],
    final_snapshot: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    game_pk = _safe_int(entry.get("gamePk"))
    if game_pk is None:
        return None
    owner = str(entry.get("owner") or "").strip()
    market = str(entry.get("market") or "").strip().lower()
    prop = str(entry.get("prop") or "").strip().lower()
    selection = str(entry.get("selection") or "").strip().lower()
    market_line = _safe_float(entry.get("marketLine"))
    if not owner or not market or not prop or selection not in {"over", "under"} or market_line is None:
        return None
    final_actual = _final_actual_value(final_snapshot, owner, market, prop)
    result = _result_label(selection, market_line, final_actual)
    if result not in {"win", "loss"}:
        return None
    first_snapshot = entry.get("firstSeenSnapshot") if isinstance(entry.get("firstSeenSnapshot"), dict) else {}
    observation_snapshot = observation.get("snapshot") if isinstance(observation.get("snapshot"), dict) else {}
    game_state = observation.get("gameState") if isinstance(observation.get("gameState"), dict) else {}
    score = game_state.get("score") if isinstance(game_state.get("score"), dict) else {}
    row: Dict[str, Any] = {
        "date": date_str,
        "key": key,
        "game_pk": game_pk,
        "gamePk": game_pk,
        "market": market,
        "prop": prop,
        "selection": selection,
        "market_line": market_line,
        "odds": observation_snapshot.get("odds") if observation_snapshot.get("odds") is not None else first_snapshot.get("odds"),
        "live_edge": observation_snapshot.get("liveEdge") if observation_snapshot.get("liveEdge") is not None else first_snapshot.get("liveEdge"),
        "live_projection": observation_snapshot.get("liveProjection") if observation_snapshot.get("liveProjection") is not None else first_snapshot.get("liveProjection"),
        "model_mean": observation_snapshot.get("modelMean") if observation_snapshot.get("modelMean") is not None else first_snapshot.get("modelMean"),
        "actual": final_actual,
        "actual_so_far": observation_snapshot.get("actual") if observation_snapshot.get("actual") is not None else first_snapshot.get("actual"),
        "owner": owner,
        "first_seen_at": entry.get("firstSeenAt"),
        "last_seen_at": entry.get("lastSeenAt"),
        "seen_count": entry.get("seenCount"),
        "team_side": observation.get("teamSide"),
        "progress_fraction": game_state.get("progressFraction"),
        "inning": game_state.get("inning"),
        "outs": game_state.get("outs"),
        "score_away": score.get("away"),
        "score_home": score.get("home"),
        "rank": observation.get("rank"),
        "reason_summary": observation_snapshot.get("reasonSummary"),
        "reasons": list(observation_snapshot.get("reasons") or []),
        "live_text": game_state.get("liveText"),
        "label": 1 if result == "win" else 0,
    }
    row.update(build_live_prop_feature_map(row))
    return row


def _build_report_fallback_row(
    *,
    date_str: str,
    game_pk: int,
    prop_row: Dict[str, Any],
    progress_fraction: Optional[float],
    inning: Optional[int],
    outs: Optional[int],
    score_away: Optional[int],
    score_home: Optional[int],
    rank: int,
) -> Optional[Dict[str, Any]]:
    owner = str(prop_row.get("playerName") or prop_row.get("owner") or "").strip()
    market = str(prop_row.get("market") or "").strip().lower()
    prop = str(prop_row.get("prop") or "").strip().lower()
    selection = str(prop_row.get("selection") or "").strip().lower()
    market_line = _safe_float(prop_row.get("line") if prop_row.get("line") is not None else prop_row.get("marketLine"))
    actual = _safe_float(prop_row.get("actual"))
    status = str(prop_row.get("status") or "").strip().lower()
    if not owner or not market or not prop or selection not in {"over", "under"} or market_line is None:
        return None
    if status not in {"win", "loss"}:
        derived = _result_label(selection, market_line, actual)
        if derived not in {"win", "loss"}:
            return None
        status = derived
    row: Dict[str, Any] = {
        "date": date_str,
        "key": f"report_fallback|{date_str}|{game_pk}|{owner.lower()}|{market}|{prop}|{selection}|{float(market_line):.3f}",
        "game_pk": int(game_pk),
        "gamePk": int(game_pk),
        "market": market,
        "prop": prop,
        "selection": selection,
        "market_line": market_line,
        "odds": prop_row.get("odds"),
        "live_edge": prop_row.get("liveEdge") if prop_row.get("liveEdge") is not None else prop_row.get("edge"),
        "live_projection": prop_row.get("liveProjection"),
        "model_mean": prop_row.get("modelMean") if prop_row.get("modelMean") is not None else prop_row.get("outsMean"),
        "actual": actual,
        "actual_so_far": actual,
        "owner": owner,
        "first_seen_at": None,
        "last_seen_at": None,
        "seen_count": 1,
        "team_side": prop_row.get("teamSide"),
        "progress_fraction": progress_fraction,
        "inning": inning,
        "outs": outs,
        "score_away": score_away,
        "score_home": score_home,
        "rank": int(rank),
        "reason_summary": None,
        "reasons": [],
        "live_text": None,
        "label": 1 if status == "win" else 0,
        "row_source": "report_fallback",
        "timing_quality": "final_report",
    }
    row.update(build_live_prop_feature_map(row))
    return row


def _iter_local_first_observation_rows(live_lens_dir: Path) -> Iterable[Dict[str, Any]]:
    registry_dir = live_lens_dir / "prop_registry"
    if not registry_dir.exists():
        return
    for registry_path in sorted(registry_dir.glob("live_prop_registry_*.json")):
        try:
            doc = _read_json(registry_path)
        except Exception:
            continue
        if not isinstance(doc, dict):
            continue
        entries = doc.get("entries") if isinstance(doc.get("entries"), dict) else {}
        if not isinstance(entries, dict) or not entries:
            continue
        suffix = registry_path.stem.replace("live_prop_registry_", "")
        observations = _load_first_observations(registry_dir / f"live_prop_observations_{suffix}.jsonl")
        if not observations:
            continue
        date_str = str(doc.get("date") or suffix.replace("_", "-")).strip()
        final_snapshots: Dict[int, Dict[str, Any]] = {}
        for game_pk in sorted({_safe_int(entry.get("gamePk")) for entry in entries.values() if isinstance(entry, dict)}):
            if game_pk is None:
                continue
            snapshot = _load_live_lens_snapshot(int(game_pk), date_str)
            status = str((((snapshot or {}).get("status") or {}).get("abstractGameState") or "")).strip().lower()
            if isinstance(snapshot, dict) and status == "final":
                final_snapshots[int(game_pk)] = snapshot
        for key, entry in entries.items():
            if not isinstance(entry, dict):
                continue
            observation = observations.get(str(key)) if isinstance(observations.get(str(key)), dict) else {}
            if not observation:
                continue
            game_pk = _safe_int(entry.get("gamePk"))
            if game_pk is None:
                continue
            final_snapshot = final_snapshots.get(int(game_pk))
            if not isinstance(final_snapshot, dict):
                continue
            row = _build_first_observation_row(
                date_str=date_str,
                key=str(key),
                entry=entry,
                observation=observation,
                final_snapshot=final_snapshot,
            )
            if row:
                yield row


def _iter_render_sync_first_observation_rows(live_lens_dir: Path, *, seen_keys: Optional[set[str]] = None) -> Iterable[Dict[str, Any]]:
    render_sync_dir = live_lens_dir / "render_sync"
    if not render_sync_dir.exists():
        return
    seen = seen_keys if isinstance(seen_keys, set) else set()
    for sync_path in sorted(render_sync_dir.glob("live_lens_reports_*.json")):
        try:
            payload = _read_json(sync_path)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        archive_rows = payload.get("firstObservationArchive") if isinstance(payload.get("firstObservationArchive"), list) else []
        if not archive_rows:
            continue
        date_str = str(payload.get("date") or sync_path.stem.replace("live_lens_reports_", "").replace("_", "-")).strip()
        final_snapshots: Dict[int, Dict[str, Any]] = {}
        game_pks = sorted({_safe_int(row.get("gamePk")) for row in archive_rows if isinstance(row, dict)})
        for game_pk in game_pks:
            if game_pk is None:
                continue
            snapshot = _load_live_lens_snapshot(int(game_pk), date_str)
            status = str((((snapshot or {}).get("status") or {}).get("abstractGameState") or "")).strip().lower()
            if isinstance(snapshot, dict) and status == "final":
                final_snapshots[int(game_pk)] = snapshot
        for archive_row in archive_rows:
            if not isinstance(archive_row, dict):
                continue
            key = str(archive_row.get("key") or "").strip()
            dedupe_key = f"{date_str}|{key}"
            if not key or dedupe_key in seen:
                continue
            game_pk = _safe_int(archive_row.get("gamePk"))
            if game_pk is None:
                continue
            final_snapshot = final_snapshots.get(int(game_pk))
            if not isinstance(final_snapshot, dict):
                continue
            entry = {
                "gamePk": archive_row.get("gamePk"),
                "owner": archive_row.get("owner"),
                "market": archive_row.get("market"),
                "prop": archive_row.get("prop"),
                "selection": archive_row.get("selection"),
                "marketLine": archive_row.get("marketLine"),
                "firstSeenAt": archive_row.get("firstSeenAt"),
                "lastSeenAt": archive_row.get("lastSeenAt"),
                "seenCount": archive_row.get("seenCount"),
                "firstSeenSnapshot": archive_row.get("firstSeenSnapshot"),
                "lastSeenSnapshot": archive_row.get("lastSeenSnapshot"),
            }
            observation = {
                "teamSide": archive_row.get("teamSide"),
                "rank": archive_row.get("rank"),
                "snapshotChanged": archive_row.get("snapshotChanged"),
                "changedFields": archive_row.get("changedFields"),
                "snapshot": archive_row.get("snapshot"),
                "gameState": archive_row.get("gameState"),
            }
            row = _build_first_observation_row(
                date_str=date_str,
                key=key,
                entry=entry,
                observation=observation,
                final_snapshot=final_snapshot,
            )
            if row:
                seen.add(dedupe_key)
                yield row


def _iter_first_observation_rows(live_lens_dir: Path) -> Iterable[Dict[str, Any]]:
    seen_keys: set[str] = set()
    for row in _iter_local_first_observation_rows(live_lens_dir):
        seen_keys.add(f"{str(row.get('date') or '')}|{str(row.get('key') or '')}")
        yield row
    for row in _iter_render_sync_first_observation_rows(live_lens_dir, seen_keys=seen_keys):
        yield row


def _iter_render_sync_report_fallback_rows(live_lens_dir: Path, *, seen_keys: Optional[set[str]] = None) -> Iterable[Dict[str, Any]]:
    render_sync_dir = live_lens_dir / "render_sync"
    if not render_sync_dir.exists():
        return
    seen = seen_keys if isinstance(seen_keys, set) else set()
    for sync_path in sorted(render_sync_dir.glob("live_lens_reports_*.json")):
        try:
            payload = _read_json(sync_path)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        if isinstance(payload.get("firstObservationArchive"), list) and payload.get("firstObservationArchive"):
            continue
        latest_report = payload.get("latestReport") if isinstance(payload.get("latestReport"), dict) else {}
        games = latest_report.get("games") if isinstance(latest_report.get("games"), list) else []
        if not games:
            continue
        date_str = str(payload.get("date") or sync_path.stem.replace("live_lens_reports_", "").replace("_", "-")).strip()
        for game in games:
            if not isinstance(game, dict):
                continue
            game_pk = _safe_int(game.get("gamePk"))
            if game_pk is None:
                continue
            progress = None
            inning = None
            outs = None
            score_away = None
            score_home = None
            game_lens_rows = game.get("gameLens") if isinstance(game.get("gameLens"), list) else []
            if game_lens_rows:
                sample_lens = next((row for row in game_lens_rows if isinstance(row, dict) and isinstance(row.get("progress"), dict)), None)
                if isinstance(sample_lens, dict):
                    progress_block = sample_lens.get("progress") if isinstance(sample_lens.get("progress"), dict) else {}
                    progress = _safe_float(progress_block.get("fraction"))
                    inning = _safe_int(progress_block.get("inning"))
                    outs = _safe_int(progress_block.get("outs"))
            matchup = game.get("matchup") if isinstance(game.get("matchup"), dict) else {}
            score = matchup.get("score") if isinstance(matchup.get("score"), dict) else {}
            score_away = _safe_int(score.get("away"))
            score_home = _safe_int(score.get("home"))
            props = game.get("props") if isinstance(game.get("props"), list) else []
            for index, prop_row in enumerate(props, start=1):
                if not isinstance(prop_row, dict):
                    continue
                owner = str(prop_row.get("playerName") or prop_row.get("owner") or "").strip().lower()
                market = str(prop_row.get("market") or "").strip().lower()
                prop = str(prop_row.get("prop") or "").strip().lower()
                selection = str(prop_row.get("selection") or "").strip().lower()
                line_value = _safe_float(prop_row.get("line") if prop_row.get("line") is not None else prop_row.get("marketLine"))
                dedupe_key = f"report_fallback|{date_str}|{int(game_pk)}|{owner}|{market}|{prop}|{selection}|{'' if line_value is None else f'{float(line_value):.3f}'}"
                if dedupe_key in seen:
                    continue
                row = _build_report_fallback_row(
                    date_str=date_str,
                    game_pk=int(game_pk),
                    prop_row=prop_row,
                    progress_fraction=progress,
                    inning=inning,
                    outs=outs,
                    score_away=score_away,
                    score_home=score_home,
                    rank=index,
                )
                if row:
                    seen.add(dedupe_key)
                    yield row


def _iter_registry_rows(live_lens_dir: Path) -> Iterable[Dict[str, Any]]:
    registry_dir = live_lens_dir / "prop_registry"
    if not registry_dir.exists():
        return
    for registry_path in sorted(registry_dir.glob("live_prop_registry_*.json")):
        try:
            doc = _read_json(registry_path)
        except Exception:
            continue
        if not isinstance(doc, dict):
            continue
        entries = doc.get("entries") if isinstance(doc.get("entries"), dict) else {}
        if not isinstance(entries, dict):
            continue
        suffix = registry_path.stem.replace("live_prop_registry_", "")
        observations = _load_first_observations(registry_dir / f"live_prop_observations_{suffix}.jsonl")
        date_str = str(doc.get("date") or suffix.replace("_", "-")).strip()
        for key, entry in entries.items():
            if not isinstance(entry, dict):
                continue
            first_snapshot = entry.get("firstSeenSnapshot") if isinstance(entry.get("firstSeenSnapshot"), dict) else {}
            last_snapshot = entry.get("lastSeenSnapshot") if isinstance(entry.get("lastSeenSnapshot"), dict) else {}
            result = _result_label(entry.get("selection"), entry.get("marketLine"), last_snapshot.get("actual"))
            if result not in {"win", "loss"}:
                continue
            observation = observations.get(str(key)) if isinstance(observations.get(str(key)), dict) else {}
            game_state = observation.get("gameState") if isinstance(observation.get("gameState"), dict) else {}
            score = game_state.get("score") if isinstance(game_state.get("score"), dict) else {}
            row: Dict[str, Any] = {
                "date": date_str,
                "key": key,
                "game_pk": entry.get("gamePk"),
                "gamePk": entry.get("gamePk"),
                "market": entry.get("market"),
                "prop": entry.get("prop"),
                "selection": entry.get("selection"),
                "market_line": entry.get("marketLine"),
                "odds": first_snapshot.get("odds"),
                "live_edge": first_snapshot.get("liveEdge"),
                "live_projection": first_snapshot.get("liveProjection"),
                "model_mean": first_snapshot.get("modelMean"),
                "actual": last_snapshot.get("actual"),
                "owner": entry.get("owner"),
                "first_seen_at": entry.get("firstSeenAt"),
                "last_seen_at": entry.get("lastSeenAt"),
                "seen_count": entry.get("seenCount"),
                "team_side": observation.get("teamSide"),
                "progress_fraction": game_state.get("progressFraction"),
                "inning": game_state.get("inning"),
                "outs": game_state.get("outs"),
                "score_away": score.get("away"),
                "score_home": score.get("home"),
                "label": 1 if result == "win" else 0,
            }
            row.update(build_live_prop_feature_map(row))
            yield row


def _split_rows_by_dates(rows: Sequence[Dict[str, Any]], val_last_dates: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if int(val_last_dates) <= 0:
        return list(rows), []
    dates = sorted({str(row.get("date") or "") for row in rows if str(row.get("date") or "")})
    if len(dates) <= int(val_last_dates):
        return list(rows), []
    val_dates = set(dates[-int(val_last_dates):])
    train = [row for row in rows if str(row.get("date") or "") not in val_dates]
    val = [row for row in rows if str(row.get("date") or "") in val_dates]
    return train, val


def _standardize_feature_rows(rows: Sequence[Dict[str, Any]], feature_names: Sequence[str]) -> Tuple[List[List[float]], Dict[str, float], Dict[str, float]]:
    centers: Dict[str, float] = {}
    scales: Dict[str, float] = {}
    matrix: List[List[float]] = []
    for name in feature_names:
        values = [float(_safe_float(row.get(name)) or 0.0) for row in rows]
        if values:
            mean = float(sum(values) / len(values))
            variance = float(sum((value - mean) ** 2 for value in values) / len(values))
            std = float(math.sqrt(max(variance, 1e-12)))
        else:
            mean = 0.0
            std = 1.0
        centers[str(name)] = mean
        scales[str(name)] = std if std > 1e-6 else 1.0
    for row in rows:
        vector: List[float] = []
        for name in feature_names:
            raw = float(_safe_float(row.get(name)) or 0.0)
            vector.append((raw - centers[str(name)]) / scales[str(name)])
        matrix.append(vector)
    return matrix, centers, scales


def _predict_matrix(matrix: Sequence[Sequence[float]], intercept: float, weights: Sequence[float]) -> List[float]:
    out: List[float] = []
    for row in matrix:
        score = float(intercept)
        for value, weight in zip(row, weights):
            score += float(value) * float(weight)
        out.append(float(_sigmoid(score)))
    return out


def _build_side_priors(rows: Sequence[Dict[str, Any]], *, alpha: float = 5.0, beta: float = 5.0) -> Dict[str, Dict[str, float]]:
    side_counts: Dict[str, List[int]] = {}
    for row in rows:
        selection = str(row.get("selection") or "").strip().lower()
        if selection not in {"over", "under"}:
            continue
        wins, total = side_counts.get(selection, [0, 0])
        total += 1
        if int(row.get("label") or 0) == 1:
            wins += 1
        side_counts[selection] = [wins, total]
    out: Dict[str, Dict[str, float]] = {}
    for selection, counts in side_counts.items():
        wins, total = counts
        probability = float((float(wins) + float(alpha)) / (float(total) + float(alpha) + float(beta)))
        out[selection] = {
            "wins": int(wins),
            "n": int(total),
            "prob": probability,
        }
    return out


def fit_logistic_linear(rows: Sequence[Dict[str, Any]], labels: Sequence[int], feature_names: Sequence[str], *, weights: Optional[Sequence[float]] = None, max_iters: int = 60, l2: float = 1e-2) -> Tuple[Dict[str, float], float, Dict[str, float], Dict[str, float], Dict[str, Any]]:
    matrix, centers, scales = _standardize_feature_rows(rows, feature_names)
    ys = [1 if int(y) == 1 else 0 for y in labels]
    ws = [float(w) for w in (weights or [1.0] * len(matrix))]
    dim = len(feature_names)
    beta = [0.0] * (dim + 1)

    def nll(current: Sequence[float]) -> float:
        total = 0.0
        for x_row, y, w in zip(matrix, ys, ws):
            if w <= 0.0:
                continue
            score = float(current[0])
            for value, weight in zip(x_row, current[1:]):
                score += float(value) * float(weight)
            p = _clip_prob(_sigmoid(score))
            total += float(w) * (-(y * math.log(p) + (1 - y) * math.log(1.0 - p)))
        total += 0.5 * float(l2) * sum(weight * weight for weight in current[1:])
        return float(total)

    before = nll(beta)
    iters = 0
    for iteration in range(int(max_iters)):
        grad = [0.0] * (dim + 1)
        hess = [[0.0] * (dim + 1) for _ in range(dim + 1)]
        for x_row, y, w in zip(matrix, ys, ws):
            if w <= 0.0:
                continue
            score = float(beta[0])
            for value, weight in zip(x_row, beta[1:]):
                score += float(value) * float(weight)
            p = _clip_prob(_sigmoid(score))
            err = float(p - y)
            row_full = [1.0] + list(x_row)
            for i in range(dim + 1):
                grad[i] += float(w) * err * float(row_full[i])
            scale = float(w) * float(p) * (1.0 - float(p))
            for i in range(dim + 1):
                for j in range(dim + 1):
                    hess[i][j] += scale * float(row_full[i]) * float(row_full[j])
        for index in range(1, dim + 1):
            grad[index] += float(l2) * float(beta[index])
            hess[index][index] += float(l2)
        step = _gaussian_solve(hess, [-value for value in grad])
        if not step:
            break
        base_nll = nll(beta)
        step_scale = 1.0
        improved = False
        while step_scale >= 1e-4:
            trial = [float(value) + step_scale * float(delta) for value, delta in zip(beta, step)]
            trial_nll = nll(trial)
            if trial_nll <= base_nll + 1e-10:
                beta = trial
                improved = True
                break
            step_scale *= 0.5
        if not improved:
            break
        iters = iteration + 1
        if max(abs(step_scale * float(delta)) for delta in step) < 1e-6:
            break

    weights_out = {str(name): float(beta[idx + 1]) for idx, name in enumerate(feature_names)}
    after = nll(beta)
    diag = {
        "n": len(rows),
        "iters": int(iters),
        "nll_before": float(before),
        "nll_after": float(after),
        "feature_names": [str(name) for name in feature_names],
        "l2": float(l2),
    }
    return weights_out, float(beta[0]), centers, scales, diag


def _build_cfg_block(rows: Sequence[Dict[str, Any]], feature_names: Sequence[str], *, val_rows: Optional[Sequence[Dict[str, Any]]] = None, l2: float, max_iters: int, l2_grid: Sequence[float]) -> Dict[str, Any]:
    labels = [int(row.get("label") or 0) for row in rows]
    weights = [1.0] * len(rows)
    best_block: Optional[Dict[str, Any]] = None
    best_logloss = float("inf")
    candidate_grid = [float(value) for value in l2_grid] if l2_grid else [float(l2)]
    if not candidate_grid:
        candidate_grid = [float(l2)]
    for l2_value in candidate_grid:
        weights_out, intercept, centers, scales, diag = fit_logistic_linear(rows, labels, feature_names, weights=weights, max_iters=int(max_iters), l2=float(l2_value))
        matrix, _, _ = _standardize_feature_rows(rows, feature_names)
        train_ps = _predict_matrix(matrix, intercept, [weights_out[str(name)] for name in feature_names])
        side_priors = _build_side_priors(rows)
        block = {
            "enabled": True,
            "mode": "logistic_linear",
            "intercept": float(intercept),
            "feature_names": [str(name) for name in feature_names],
            "weights": weights_out,
            "centers": centers,
            "scales": scales,
            "side_priors": side_priors,
            "prior_blend_k": 25.0,
            "prior_blend_cap": 0.75,
            "probability_floor": 0.03,
            "probability_ceiling": 0.97,
            "diag": {
                **diag,
                "train_logloss": float(_logloss(train_ps, labels)),
                "train_brier": float(_brier(train_ps, labels)),
                "side_priors": side_priors,
            },
        }
        if val_rows:
            val_matrix: List[List[float]] = []
            for row in val_rows:
                vector: List[float] = []
                for name in feature_names:
                    raw = float(_safe_float(row.get(name)) or 0.0)
                    center = float(centers.get(str(name)) or 0.0)
                    scale = float(scales.get(str(name)) or 1.0)
                    vector.append((raw - center) / (scale if abs(scale) >= 1e-9 else 1.0))
                val_matrix.append(vector)
            val_labels = [int(row.get("label") or 0) for row in val_rows]
            val_ps = _predict_matrix(val_matrix, intercept, [weights_out[str(name)] for name in feature_names])
            block["diag"]["val_logloss"] = float(_logloss(val_ps, val_labels))
            block["diag"]["val_brier"] = float(_brier(val_ps, val_labels))
            score = float(block["diag"]["val_logloss"])
        else:
            score = float(block["diag"]["train_logloss"])
        block["diag"]["selected_l2"] = float(l2_value)
        if score < best_logloss:
            best_logloss = score
            best_block = block
    return best_block or {
        "enabled": False,
        "mode": "logistic_linear",
        "intercept": 0.0,
        "feature_names": [str(name) for name in feature_names],
        "weights": {str(name): 0.0 for name in feature_names},
        "centers": {str(name): 0.0 for name in feature_names},
        "scales": {str(name): 1.0 for name in feature_names},
        "side_priors": _build_side_priors(rows),
        "prior_blend_k": 25.0,
        "prior_blend_cap": 0.75,
        "probability_floor": 0.03,
        "probability_ceiling": 0.97,
        "diag": {"n": len(rows)},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Fit live prop ranking/calibration models from final-settled first-observation live-lens data")
    parser.add_argument("--live-lens-dir", default="data/live_lens", help="Path to the live_lens directory")
    parser.add_argument("--out-config", default="data/tuning/live_prop_ranking/default.json", help="Output JSON config path")
    parser.add_argument("--out-dataset", default="", help="Optional JSONL path for extracted training rows")
    parser.add_argument("--min-n", type=int, default=50, help="Minimum settled rows required for a per-prop model")
    parser.add_argument("--val-last-dates", type=int, default=2, help="Hold out the last N settled dates for diagnostics")
    parser.add_argument("--include-report-fallback", choices=("on", "off"), default="off", help="Include compacted render_sync latestReport prop rows as supplemental final-report fallback training data")
    parser.add_argument("--l2", type=float, default=0.1, help="Default L2 regularization strength")
    parser.add_argument("--l2-grid", default="0.01,0.03,0.1,0.3,1.0,3.0", help="Comma-separated L2 values to try")
    parser.add_argument("--max-iters", type=int, default=60)
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[2]
    live_lens_dir = Path(args.live_lens_dir)
    if not live_lens_dir.is_absolute():
        live_lens_dir = (root / live_lens_dir).resolve()
    if not live_lens_dir.exists():
        raise SystemExit(f"Live lens dir not found: {live_lens_dir}")

    rows = list(_iter_first_observation_rows(live_lens_dir))
    if not rows:
        raise SystemExit("No final-settled first-observation live-lens rows found")
    rows.sort(key=lambda row: (str(row.get("date") or ""), str(row.get("first_seen_at") or ""), str(row.get("key") or "")))
    if str(args.include_report_fallback) == "on":
        seen_keys = {f"{str(row.get('date') or '')}|{str(row.get('key') or '')}" for row in rows}
        fallback_rows = list(_iter_render_sync_report_fallback_rows(live_lens_dir, seen_keys=seen_keys))
        rows.extend(fallback_rows)

    feature_names = list(DEFAULT_LIVE_PROP_FEATURES)
    train_rows, val_rows = _split_rows_by_dates(rows, int(args.val_last_dates))
    if not train_rows:
        train_rows = list(rows)
        val_rows = []

    l2_grid: List[float] = []
    for token in str(args.l2_grid).split(","):
        text = token.strip()
        if not text:
            continue
        try:
            l2_grid.append(float(text))
        except Exception:
            continue
    if not l2_grid:
        l2_grid = [float(args.l2)]

    config: Dict[str, Any] = {
        "enabled": True,
        "default": _build_cfg_block(train_rows, feature_names, val_rows=val_rows, l2=float(args.l2), max_iters=int(args.max_iters), l2_grid=l2_grid),
        "props": {},
        "diag": {
            "n_total": len(rows),
            "n_train": len(train_rows),
            "n_val": len(val_rows),
            "feature_names": feature_names,
            "val_last_dates": int(args.val_last_dates),
            "prop_counts": dict(Counter(str(row.get("prop") or "") for row in rows)),
            "date_counts": dict(Counter(str(row.get("date") or "") for row in rows)),
        },
    }

    prop_keys = sorted({str(row.get("prop") or "").strip().lower() for row in rows if str(row.get("prop") or "").strip()})
    for prop_key in prop_keys:
        prop_train = [row for row in train_rows if str(row.get("prop") or "").strip().lower() == prop_key]
        prop_val = [row for row in val_rows if str(row.get("prop") or "").strip().lower() == prop_key]
        if len(prop_train) < int(args.min_n):
            config["props"][prop_key] = {
                "enabled": False,
                "mode": "logistic_linear",
                "intercept": 0.0,
                "feature_names": feature_names,
                "weights": {name: 0.0 for name in feature_names},
                "centers": {name: 0.0 for name in feature_names},
                "scales": {name: 1.0 for name in feature_names},
                "diag": {"n": len(prop_train)},
            }
            continue
        config["props"][prop_key] = _build_cfg_block(prop_train, feature_names, val_rows=prop_val, l2=float(args.l2), max_iters=int(args.max_iters), l2_grid=l2_grid)

    out_config = Path(args.out_config)
    if not out_config.is_absolute():
        out_config = (root / out_config).resolve()
    _write_json(out_config, config)

    if str(args.out_dataset).strip():
        out_dataset = Path(str(args.out_dataset))
        if not out_dataset.is_absolute():
            out_dataset = (root / out_dataset).resolve()
        _write_jsonl(out_dataset, rows)
        print(f"Wrote dataset: {out_dataset}")

    print(f"Rows: {len(rows)} (train={len(train_rows)}, val={len(val_rows)})")
    print(f"Wrote config: {out_config}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())