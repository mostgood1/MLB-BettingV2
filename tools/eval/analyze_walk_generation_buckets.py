from __future__ import annotations

import argparse
import gzip
import json
from collections import Counter
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Optional, Tuple


BASES = ("1B", "2B", "3B")
WALK_EVENT_TYPES = {"walk", "intent_walk", "intentional_walk"}
INTENT_WALK_EVENT_TYPES = {"intent_walk", "intentional_walk"}
NON_PA_EVENT_TYPES = {
    "wild_pitch",
    "passed_ball",
    "balk",
    "stolen_base",
    "caught_stealing_2b",
    "caught_stealing_3b",
    "caught_stealing_home",
    "pickoff_1b",
    "pickoff_2b",
    "pickoff_3b",
    "pickoff_caught_stealing_2b",
    "pickoff_caught_stealing_3b",
    "pickoff_caught_stealing_home",
}
COUNT_BUCKETS = [
    "unintent_walks",
    "intent_walks",
    "starter_unintent_walks",
    "reliever_unintent_walks",
    "early_unintent_walks",
    "late_unintent_walks",
    "lead_off_walks",
    "two_out_walks",
    "runner_on_walks",
    "risp_walks",
    "bases_loaded_walks",
    "forced_run_walks",
    "three_zero_walks",
    "three_one_walks",
    "full_count_walks",
    "four_pitch_walks",
]
METRIC_KEYS = [
    "pred_bb",
    "actual_bb_box",
    "actual_unintent_bb",
    "bb_gap_raw",
    "bb_gap_unintent",
    "run_gap",
    "unintent_walk_rate",
    "three_ball_reach_rate",
    "three_ball_walk_conversion",
    "full_count_rate",
    "full_count_walk_conversion",
    "starter_walk_rate",
    "reliever_walk_rate",
    "starter_three_ball_reach_rate",
    "reliever_three_ball_reach_rate",
    "starter_three_ball_walk_conversion",
    "reliever_three_ball_walk_conversion",
    "runner_on_walk_share",
    "late_walk_share",
    "full_count_walk_share",
    "three_zero_walk_share",
    "three_one_walk_share",
    "intent_walk_share",
]
PROFILE_BUCKETS = [
    "reliever_unintent_walks",
    "late_unintent_walks",
    "runner_on_walks",
    "lead_off_walks",
    "full_count_walks",
    "three_zero_walks",
    "intent_walks",
]


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_gzip_json(path: Path) -> Any:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _lower(value: Any) -> str:
    return str(value or "").strip().lower()


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _mean_or_none(values: Iterable[Optional[float]]) -> Optional[float]:
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return None
    return round(mean(clean), 3)


def _rate(num: Any, den: Any) -> Optional[float]:
    den_i = _safe_int(den)
    if den_i <= 0:
        return None
    return round(_safe_float(num) / float(den_i), 3)


def _team_totals(game: Dict[str, Any], side: str) -> Tuple[float, int]:
    team = ((game.get("team_batting") or {}).get(side) or {})
    pred = team.get("pred") or {}
    actual = team.get("actual") or {}
    return (_safe_float(pred.get("BB_mean")), _safe_int(actual.get("BB")))


def _load_games(batch_dir: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for report_path in sorted(batch_dir.glob("sim_vs_actual_*.json")):
        report = _read_json(report_path)
        meta = report.get("meta") or {}
        date = str(meta.get("date") or report_path.stem.replace("sim_vs_actual_", ""))
        season = _safe_int(meta.get("season"))
        for game in report.get("games") or []:
            full = ((game.get("segments") or {}).get("full") or {})
            actual_runs = _safe_int((full.get("actual") or {}).get("away")) + _safe_int((full.get("actual") or {}).get("home"))
            pred_runs = _safe_float(full.get("mean_total_runs"))
            away_pred_bb, away_actual_bb = _team_totals(game, "away")
            home_pred_bb, home_actual_bb = _team_totals(game, "home")
            rows.append(
                {
                    "date": date,
                    "season": season,
                    "game_pk": _safe_int(game.get("game_pk")),
                    "away": ((game.get("away") or {}).get("abbr")) or ((game.get("away") or {}).get("name")) or "AWAY",
                    "home": ((game.get("home") or {}).get("abbr")) or ((game.get("home") or {}).get("name")) or "HOME",
                    "pred_runs": round(pred_runs, 3),
                    "actual_runs": actual_runs,
                    "run_gap": round(float(actual_runs) - pred_runs, 3),
                    "pred_bb": round(away_pred_bb + home_pred_bb, 3),
                    "actual_bb_box": away_actual_bb + home_actual_bb,
                }
            )
    return rows


def _feed_path(feed_root: Path, season: int, date: str, game_pk: int) -> Path:
    return feed_root / str(season) / date / f"{game_pk}.json.gz"


def _runner_start(movement: Dict[str, Any]) -> str:
    return str(movement.get("originBase") or movement.get("start") or "")


def _runner_end(movement: Dict[str, Any]) -> str:
    return str(movement.get("end") or "")


def _starting_pitcher_ids(feed: Dict[str, Any]) -> Dict[str, int]:
    teams = ((((feed.get("liveData") or {}).get("boxscore") or {}).get("teams")) or {})
    starters: Dict[str, int] = {}
    for side in ("away", "home"):
        team = teams.get(side) or {}
        starter_id = 0
        players = team.get("players") or {}
        for pitcher_id in team.get("pitchers") or []:
            player = players.get(f"ID{pitcher_id}") or {}
            pitching = (player.get("stats") or {}).get("pitching") or {}
            if _safe_int(pitching.get("gamesStarted")) > 0:
                starter_id = _safe_int(pitcher_id)
                break
        if not starter_id:
            starter_id = _safe_int(((team.get("pitchers") or [0])[0]))
        starters[side] = starter_id
    return starters


def _pitch_events(play: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [entry for entry in (play.get("playEvents") or []) if isinstance(entry, dict) and bool(entry.get("isPitch"))]


def _is_plate_appearance(play: Dict[str, Any], event_type: str) -> bool:
    if _pitch_events(play):
        return True
    if event_type in WALK_EVENT_TYPES or event_type == "hit_by_pitch":
        return True
    if event_type in NON_PA_EVENT_TYPES:
        return False
    matchup = play.get("matchup") or {}
    batter_id = _safe_int(((matchup.get("batter") or {}).get("id")))
    return batter_id > 0


def _walk_terminal_count_before_pitch(pitch_events: List[Dict[str, Any]]) -> Optional[Tuple[int, int]]:
    if len(pitch_events) >= 2:
        count = pitch_events[-2].get("count") or {}
        return (_safe_int(count.get("balls")), _safe_int(count.get("strikes")))
    if len(pitch_events) == 1:
        count = pitch_events[-1].get("count") or {}
        balls = max(0, _safe_int(count.get("balls")) - 1)
        strikes = _safe_int(count.get("strikes"))
        return (balls, strikes)
    return None


def _extract_walk_buckets(feed: Dict[str, Any]) -> Dict[str, Any]:
    counts: Counter[str] = Counter()
    starters = _starting_pitcher_ids(feed)
    plays = (((feed.get("liveData") or {}).get("plays") or {}).get("allPlays")) or []
    current_half: Tuple[int, bool] | None = None
    base_state: Dict[str, bool] = {}

    for play in plays:
        about = play.get("about") or {}
        inning = _safe_int(about.get("inning"))
        is_top = bool(about.get("isTopInning"))
        half_key = (inning, is_top)
        if half_key != current_half:
            current_half = half_key
            base_state = {}

        result = play.get("result") or {}
        event_type = _lower(result.get("eventType"))
        pitch_events = _pitch_events(play)
        is_pa = _is_plate_appearance(play, event_type)
        matchup = play.get("matchup") or {}
        pitcher_id = _safe_int(((matchup.get("pitcher") or {}).get("id")))
        defense_side = "home" if is_top else "away"
        starter_id = starters.get(defense_side, 0)
        is_starter_pitcher = bool(pitcher_id and pitcher_id == starter_id)

        if is_pa:
            counts["pa_total"] += 1
            counts["pa_vs_starter"] += 1 if is_starter_pitcher else 0
            counts["pa_vs_reliever"] += 0 if is_starter_pitcher else 1

            reached_3balls = any(_safe_int((entry.get("count") or {}).get("balls")) >= 3 for entry in pitch_events)
            reached_full_count = any(
                _safe_int((entry.get("count") or {}).get("balls")) == 3 and _safe_int((entry.get("count") or {}).get("strikes")) == 2
                for entry in pitch_events
            )
            if reached_3balls:
                counts["pa_reached_3balls"] += 1
                counts["pa_reached_3balls_vs_starter"] += 1 if is_starter_pitcher else 0
                counts["pa_reached_3balls_vs_reliever"] += 0 if is_starter_pitcher else 1
            if reached_full_count:
                counts["pa_reached_full_count"] += 1
                counts["pa_reached_full_count_vs_starter"] += 1 if is_starter_pitcher else 0
                counts["pa_reached_full_count_vs_reliever"] += 0 if is_starter_pitcher else 1

        pre_play_state = dict(base_state)
        outs_before = _safe_int((play.get("count") or {}).get("outs"))
        runners = play.get("runners") or []

        if event_type in WALK_EVENT_TYPES:
            counts["walk_events"] += 1
            if event_type in INTENT_WALK_EVENT_TYPES:
                counts["intent_walks"] += 1
            else:
                counts["unintent_walks"] += 1
                counts["starter_unintent_walks"] += 1 if is_starter_pitcher else 0
                counts["reliever_unintent_walks"] += 0 if is_starter_pitcher else 1
                counts["early_unintent_walks"] += 1 if inning <= 3 else 0
                counts["late_unintent_walks"] += 1 if inning >= 7 else 0
                counts["lead_off_walks"] += 1 if outs_before == 0 and not pre_play_state else 0
                counts["two_out_walks"] += 1 if outs_before >= 2 else 0
                counts["runner_on_walks"] += 1 if pre_play_state else 0
                counts["risp_walks"] += 1 if ("2B" in pre_play_state or "3B" in pre_play_state) else 0
                counts["bases_loaded_walks"] += 1 if all(base in pre_play_state for base in BASES) else 0
                counts["forced_run_walks"] += 1 if _safe_int(result.get("rbi")) > 0 else 0

                count_before = _walk_terminal_count_before_pitch(pitch_events)
                if count_before == (3, 0):
                    counts["three_zero_walks"] += 1
                elif count_before == (3, 1):
                    counts["three_one_walks"] += 1
                elif count_before == (3, 2):
                    counts["full_count_walks"] += 1
                if len(pitch_events) == 4:
                    counts["four_pitch_walks"] += 1

        for runner in runners:
            movement = runner.get("movement") or {}
            start = _runner_start(movement)
            if start in BASES:
                base_state.pop(start, None)

        for runner in runners:
            movement = runner.get("movement") or {}
            if bool(movement.get("isOut")):
                continue
            end = _runner_end(movement)
            if end in BASES:
                base_state[end] = True

    return dict(counts)


def _merge_game_and_feed(rows: List[Dict[str, Any]], feed_root: Path, min_bb_gap: float, min_run_gap: float) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    for row in rows:
        feed_path = _feed_path(feed_root, row["season"], row["date"], row["game_pk"])
        payload = dict(row)
        payload["feed_path"] = str(feed_path)
        if not feed_path.exists():
            payload["feed_missing"] = True
            merged.append(payload)
            continue

        payload.update(_extract_walk_buckets(_read_gzip_json(feed_path)))
        payload["actual_unintent_bb"] = _safe_int(payload.get("unintent_walks"))
        payload["actual_intent_bb"] = _safe_int(payload.get("intent_walks"))
        payload["bb_gap_raw"] = round(_safe_float(payload.get("actual_bb_box")) - _safe_float(payload.get("pred_bb")), 3)
        payload["bb_gap_unintent"] = round(_safe_float(payload.get("actual_unintent_bb")) - _safe_float(payload.get("pred_bb")), 3)
        payload["bb_surplus"] = bool(_safe_float(payload.get("bb_gap_unintent")) >= float(min_bb_gap))
        payload["bb_run_paradox"] = bool(payload["bb_surplus"] and _safe_float(payload.get("run_gap")) >= float(min_run_gap))
        payload["unintent_walk_rate"] = _rate(payload.get("actual_unintent_bb"), payload.get("pa_total"))
        payload["three_ball_reach_rate"] = _rate(payload.get("pa_reached_3balls"), payload.get("pa_total"))
        payload["three_ball_walk_conversion"] = _rate(payload.get("actual_unintent_bb"), payload.get("pa_reached_3balls"))
        payload["full_count_rate"] = _rate(payload.get("pa_reached_full_count"), payload.get("pa_total"))
        payload["full_count_walk_conversion"] = _rate(payload.get("full_count_walks"), payload.get("pa_reached_full_count"))
        payload["starter_walk_rate"] = _rate(payload.get("starter_unintent_walks"), payload.get("pa_vs_starter"))
        payload["reliever_walk_rate"] = _rate(payload.get("reliever_unintent_walks"), payload.get("pa_vs_reliever"))
        payload["starter_three_ball_reach_rate"] = _rate(payload.get("pa_reached_3balls_vs_starter"), payload.get("pa_vs_starter"))
        payload["reliever_three_ball_reach_rate"] = _rate(payload.get("pa_reached_3balls_vs_reliever"), payload.get("pa_vs_reliever"))
        payload["starter_three_ball_walk_conversion"] = _rate(payload.get("starter_unintent_walks"), payload.get("pa_reached_3balls_vs_starter"))
        payload["reliever_three_ball_walk_conversion"] = _rate(payload.get("reliever_unintent_walks"), payload.get("pa_reached_3balls_vs_reliever"))
        payload["runner_on_walk_share"] = _rate(payload.get("runner_on_walks"), payload.get("actual_unintent_bb"))
        payload["late_walk_share"] = _rate(payload.get("late_unintent_walks"), payload.get("actual_unintent_bb"))
        payload["full_count_walk_share"] = _rate(payload.get("full_count_walks"), payload.get("actual_unintent_bb"))
        payload["three_zero_walk_share"] = _rate(payload.get("three_zero_walks"), payload.get("actual_unintent_bb"))
        payload["three_one_walk_share"] = _rate(payload.get("three_one_walks"), payload.get("actual_unintent_bb"))
        payload["intent_walk_share"] = _rate(payload.get("actual_intent_bb"), payload.get("walk_events"))
        merged.append(payload)
    return merged


def _bucket_summary(positive_rows: List[Dict[str, Any]], negative_rows: List[Dict[str, Any]], bucket_keys: Iterable[str]) -> List[Dict[str, Any]]:
    summary: List[Dict[str, Any]] = []
    for key in bucket_keys:
        positive_present = [row for row in positive_rows if _safe_int(row.get(key)) > 0]
        negative_present = [row for row in negative_rows if _safe_int(row.get(key)) > 0]
        summary.append(
            {
                "bucket": key,
                "positive_share": round(len(positive_present) / len(positive_rows), 3) if positive_rows else None,
                "negative_share": round(len(negative_present) / len(negative_rows), 3) if negative_rows else None,
                "share_lift": round((len(positive_present) / len(positive_rows)) - (len(negative_present) / len(negative_rows)), 3)
                if positive_rows and negative_rows
                else None,
                "positive_avg_count": _mean_or_none(_safe_int(row.get(key)) for row in positive_rows),
                "negative_avg_count": _mean_or_none(_safe_int(row.get(key)) for row in negative_rows),
            }
        )
    return sorted(summary, key=lambda row: -999.0 if row.get("share_lift") is None else -float(row["share_lift"]))


def _metric_summary(positive_rows: List[Dict[str, Any]], negative_rows: List[Dict[str, Any]], metric_keys: Iterable[str]) -> List[Dict[str, Any]]:
    summary: List[Dict[str, Any]] = []
    for key in metric_keys:
        pos_mean = _mean_or_none(row.get(key) for row in positive_rows)
        neg_mean = _mean_or_none(row.get(key) for row in negative_rows)
        delta = None
        if pos_mean is not None and neg_mean is not None:
            delta = round(float(pos_mean) - float(neg_mean), 3)
        summary.append(
            {
                "metric": key,
                "positive_mean": pos_mean,
                "negative_mean": neg_mean,
                "delta": delta,
            }
        )
    return sorted(summary, key=lambda row: -999.0 if row.get("delta") is None else -float(row["delta"]))


def _top_profiles(rows: List[Dict[str, Any]], profile_buckets: Iterable[str]) -> List[Dict[str, Any]]:
    counts: Counter[Tuple[str, ...]] = Counter()
    for row in rows:
        profile = tuple(bucket for bucket in profile_buckets if _safe_int(row.get(bucket)) > 0)
        counts[profile] += 1
    top: List[Dict[str, Any]] = []
    for profile, count in counts.most_common(10):
        top.append(
            {
                "profile": list(profile),
                "games": count,
                "share": round(count / len(rows), 3) if rows else None,
            }
        )
    return top


def _top_games(rows: List[Dict[str, Any]], sort_keys: Tuple[str, str]) -> List[Dict[str, Any]]:
    rows = list(rows)
    rows.sort(key=lambda row: (-_safe_float(row.get(sort_keys[0])), -_safe_float(row.get(sort_keys[1])), row.get("date", "")))
    top: List[Dict[str, Any]] = []
    for row in rows[:20]:
        top.append(
            {
                "date": row["date"],
                "matchup": f"{row['away']} @ {row['home']}",
                "game_pk": row["game_pk"],
                "pred_bb": row.get("pred_bb"),
                "actual_bb_box": row.get("actual_bb_box"),
                "actual_unintent_bb": row.get("actual_unintent_bb"),
                "intent_bb": row.get("actual_intent_bb"),
                "bb_gap_unintent": row.get("bb_gap_unintent"),
                "run_gap": row.get("run_gap"),
                "three_ball_reach_rate": row.get("three_ball_reach_rate"),
                "three_ball_walk_conversion": row.get("three_ball_walk_conversion"),
                "full_count_walk_conversion": row.get("full_count_walk_conversion"),
                "starter_unintent_walks": _safe_int(row.get("starter_unintent_walks")),
                "reliever_unintent_walks": _safe_int(row.get("reliever_unintent_walks")),
                "runner_on_walks": _safe_int(row.get("runner_on_walks")),
                "bases_loaded_walks": _safe_int(row.get("bases_loaded_walks")),
                "forced_run_walks": _safe_int(row.get("forced_run_walks")),
            }
        )
    return top


def _to_markdown(report: Dict[str, Any]) -> str:
    lines: List[str] = []
    summary = report["summary"]
    lines.append("# Walk Generation Diagnostic")
    lines.append("")
    lines.append(f"- Batch dir: `{report['batch_dir']}`")
    lines.append(f"- Feed root: `{report['feed_root']}`")
    lines.append(f"- Games with feed: {summary['games_with_feed']} / {summary['games_total']}")
    lines.append(f"- BB-surplus threshold: actual unintentional BB - predicted BB >= {summary['min_bb_gap']}")
    lines.append(f"- BB-surplus games: {summary['bb_surplus_games']} ({summary['bb_surplus_share']})")
    lines.append(f"- BB-run paradox games: {summary['bb_run_paradox_games']} ({summary['bb_run_paradox_share']})")
    lines.append(f"- Mean BB gap in BB-surplus games: {summary['bb_surplus_mean_bb_gap']}")
    lines.append(f"- Mean run gap in BB-run paradox games: {summary['bb_run_paradox_mean_run_gap']}")
    lines.append("")
    lines.append("## BB-Surplus Bucket Ranking")
    lines.append("")
    lines.append("| Bucket | BB-Surplus Share | Other Share | Lift | BB-Surplus Avg Count | Other Avg Count |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for row in report["bb_surplus_bucket_summary"]:
        lines.append(
            "| {bucket} | {positive_share} | {negative_share} | {share_lift} | {positive_avg_count} | {negative_avg_count} |".format(
                **row
            )
        )
    lines.append("")
    lines.append("## BB-Surplus Metric Ranking")
    lines.append("")
    lines.append("| Metric | BB-Surplus Mean | Other Mean | Delta |")
    lines.append("|---|---:|---:|---:|")
    for row in report["bb_surplus_metric_summary"]:
        lines.append("| {metric} | {positive_mean} | {negative_mean} | {delta} |".format(**row))
    lines.append("")
    lines.append("## BB-Run Paradox Bucket Ranking")
    lines.append("")
    lines.append("Comparator set: BB-surplus games that did not also miss runs by the run-gap threshold.")
    lines.append("")
    lines.append("| Bucket | BB+Run Share | BB-Only Share | Lift | BB+Run Avg Count | BB-Only Avg Count |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for row in report["bb_run_paradox_bucket_summary"]:
        lines.append(
            "| {bucket} | {positive_share} | {negative_share} | {share_lift} | {positive_avg_count} | {negative_avg_count} |".format(
                **row
            )
        )
    lines.append("")
    lines.append("## BB-Run Paradox Metric Ranking")
    lines.append("")
    lines.append("| Metric | BB+Run Mean | BB-Only Mean | Delta |")
    lines.append("|---|---:|---:|---:|")
    for row in report["bb_run_paradox_metric_summary"]:
        lines.append("| {metric} | {positive_mean} | {negative_mean} | {delta} |".format(**row))
    lines.append("")
    lines.append("## Top BB-Surplus Profiles")
    lines.append("")
    lines.append("| Profile | Games | Share |")
    lines.append("|---|---:|---:|")
    for row in report["bb_surplus_top_profiles"]:
        label = ", ".join(row["profile"]) if row["profile"] else "none"
        lines.append(f"| {label} | {row['games']} | {row['share']} |")
    lines.append("")
    lines.append("## Top BB-Run Paradox Profiles")
    lines.append("")
    lines.append("| Profile | Games | Share |")
    lines.append("|---|---:|---:|")
    for row in report["bb_run_paradox_top_profiles"]:
        label = ", ".join(row["profile"]) if row["profile"] else "none"
        lines.append(f"| {label} | {row['games']} | {row['share']} |")
    lines.append("")
    lines.append("## Top BB-Surplus Games")
    lines.append("")
    lines.append("| Date | Matchup | Pred BB | Actual BB | Unintent BB | Intent BB | BB Gap | Run Gap | 3-Ball Reach | 3-Ball Conv | Full Count Conv | Starter BB | Reliever BB | Runner-On BB | Bases Loaded BB | Forced-Run BB |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in report["bb_surplus_top_games"]:
        lines.append(
            f"| {row['date']} | {row['matchup']} | {row['pred_bb']} | {row['actual_bb_box']} | {row['actual_unintent_bb']} | {row['intent_bb']} | {row['bb_gap_unintent']} | {row['run_gap']} | {row['three_ball_reach_rate']} | {row['three_ball_walk_conversion']} | {row['full_count_walk_conversion']} | {row['starter_unintent_walks']} | {row['reliever_unintent_walks']} | {row['runner_on_walks']} | {row['bases_loaded_walks']} | {row['forced_run_walks']} |"
        )
    lines.append("")
    lines.append("## Top BB-Run Paradox Games")
    lines.append("")
    lines.append("| Date | Matchup | Pred BB | Actual BB | Unintent BB | Intent BB | BB Gap | Run Gap | 3-Ball Reach | 3-Ball Conv | Full Count Conv | Starter BB | Reliever BB | Runner-On BB | Bases Loaded BB | Forced-Run BB |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in report["bb_run_paradox_top_games"]:
        lines.append(
            f"| {row['date']} | {row['matchup']} | {row['pred_bb']} | {row['actual_bb_box']} | {row['actual_unintent_bb']} | {row['intent_bb']} | {row['bb_gap_unintent']} | {row['run_gap']} | {row['three_ball_reach_rate']} | {row['three_ball_walk_conversion']} | {row['full_count_walk_conversion']} | {row['starter_unintent_walks']} | {row['reliever_unintent_walks']} | {row['runner_on_walks']} | {row['bases_loaded_walks']} | {row['forced_run_walks']} |"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze walk-generation residuals against raw feed_live event buckets.")
    parser.add_argument("--batch-dir", required=True, help="Path to a batch folder containing sim_vs_actual_*.json files.")
    parser.add_argument(
        "--feed-root",
        default="data/raw/statsapi/feed_live",
        help="Root path for raw feed_live files (season/date/game_pk.json.gz).",
    )
    parser.add_argument(
        "--min-bb-gap",
        type=float,
        default=2.0,
        help="Minimum actual unintentional BB minus predicted BB for a game to count as a BB-surplus miss.",
    )
    parser.add_argument(
        "--min-run-gap",
        type=float,
        default=1.0,
        help="Minimum actual minus predicted runs for a BB-surplus game to count as a BB-run paradox game.",
    )
    parser.add_argument("--out-json", default="", help="Optional output JSON path.")
    parser.add_argument("--out-md", default="", help="Optional output Markdown path.")
    args = parser.parse_args()

    batch_dir = Path(args.batch_dir)
    feed_root = Path(args.feed_root)
    if not batch_dir.exists():
        raise SystemExit(f"Missing batch dir: {batch_dir}")
    if not feed_root.exists():
        raise SystemExit(f"Missing feed root: {feed_root}")

    game_rows = _merge_game_and_feed(_load_games(batch_dir), feed_root, args.min_bb_gap, args.min_run_gap)
    rows_with_feed = [row for row in game_rows if not row.get("feed_missing")]
    bb_surplus_rows = [row for row in rows_with_feed if row.get("bb_surplus")]
    non_bb_surplus_rows = [row for row in rows_with_feed if not row.get("bb_surplus")]
    bb_run_paradox_rows = [row for row in bb_surplus_rows if row.get("bb_run_paradox")]
    bb_only_rows = [row for row in bb_surplus_rows if not row.get("bb_run_paradox")]

    report = {
        "batch_dir": str(batch_dir.resolve()),
        "feed_root": str(feed_root.resolve()),
        "summary": {
            "games_total": len(game_rows),
            "games_with_feed": len(rows_with_feed),
            "games_missing_feed": len([row for row in game_rows if row.get("feed_missing")]),
            "min_bb_gap": float(args.min_bb_gap),
            "min_run_gap": float(args.min_run_gap),
            "bb_surplus_games": len(bb_surplus_rows),
            "bb_surplus_share": round(len(bb_surplus_rows) / len(rows_with_feed), 3) if rows_with_feed else None,
            "bb_surplus_mean_bb_gap": _mean_or_none(row.get("bb_gap_unintent") for row in bb_surplus_rows),
            "bb_surplus_mean_run_gap": _mean_or_none(row.get("run_gap") for row in bb_surplus_rows),
            "bb_run_paradox_games": len(bb_run_paradox_rows),
            "bb_run_paradox_share": round(len(bb_run_paradox_rows) / len(bb_surplus_rows), 3) if bb_surplus_rows else None,
            "bb_run_paradox_mean_bb_gap": _mean_or_none(row.get("bb_gap_unintent") for row in bb_run_paradox_rows),
            "bb_run_paradox_mean_run_gap": _mean_or_none(row.get("run_gap") for row in bb_run_paradox_rows),
        },
        "bb_surplus_bucket_summary": _bucket_summary(bb_surplus_rows, non_bb_surplus_rows, COUNT_BUCKETS),
        "bb_surplus_metric_summary": _metric_summary(bb_surplus_rows, non_bb_surplus_rows, METRIC_KEYS),
        "bb_run_paradox_bucket_summary": _bucket_summary(bb_run_paradox_rows, bb_only_rows, COUNT_BUCKETS),
        "bb_run_paradox_metric_summary": _metric_summary(bb_run_paradox_rows, bb_only_rows, METRIC_KEYS),
        "bb_surplus_top_profiles": _top_profiles(bb_surplus_rows, PROFILE_BUCKETS),
        "bb_run_paradox_top_profiles": _top_profiles(bb_run_paradox_rows, PROFILE_BUCKETS),
        "bb_surplus_top_games": _top_games(bb_surplus_rows, ("bb_gap_unintent", "run_gap")),
        "bb_run_paradox_top_games": _top_games(bb_run_paradox_rows, ("run_gap", "bb_gap_unintent")),
        "games": rows_with_feed,
    }

    out_json = Path(args.out_json) if args.out_json else batch_dir / "walk_generation_bucket_summary.json"
    out_md = Path(args.out_md) if args.out_md else batch_dir / "walk_generation_bucket_summary.md"
    _write_json(out_json, report)
    _write_text(out_md, _to_markdown(report))
    print(
        json.dumps(
            {
                "summary": report["summary"],
                "top_bb_surplus_metrics": report["bb_surplus_metric_summary"][:10],
                "top_bb_run_metrics": report["bb_run_paradox_metric_summary"][:10],
                "out_json": str(out_json),
                "out_md": str(out_md),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())