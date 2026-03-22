from __future__ import annotations

import argparse
import gzip
import json
from collections import Counter
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Tuple


MISC_ADVANCE_EVENT_TYPES = {"wild_pitch", "passed_ball", "balk"}
STEAL_EVENT_TYPES = {"stolen_base"}
BB_HBP_EVENT_TYPES = {"walk", "intent_walk", "intentional_walk", "hit_by_pitch"}
HIT_REACH_EVENT_TYPES = {"single", "double", "triple", "home_run"}
NON_HIT_REACH_EXACT = {
    "field_error",
    "fielders_choice",
    "fielders_choice_out",
    "catcher_interference",
    "catcher_interf",
}
EXCLUDED_BASERUN_OUT_EVENTS = {
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
BASES = ("1B", "2B", "3B")
PROFILE_BUCKETS = [
    "hr_surplus_ge1",
    "bb_surplus_ge2",
    "productive_outs",
    "aggressive_advancement",
    "misc_advance",
    "non_hit_reach",
    "error_runs",
    "steals",
]
HR_CONTEXT_BUCKETS = [
    "multi_run_hr",
    "two_run_hr",
    "three_run_hr",
    "grand_slam",
    "starter_hr",
    "reliever_hr",
    "multi_run_hr_off_starter",
    "multi_run_hr_off_reliever",
    "early_hr",
    "late_hr",
    "hr_two_out",
    "multi_run_hr_two_out",
    "hr_setup_bb_hbp",
    "hr_setup_non_hit_reach",
    "hr_setup_hit_reach",
    "hr_setup_hits_only",
    "multi_run_hr_setup_bb_hbp",
    "multi_run_hr_setup_non_hit_reach",
    "multi_run_hr_setup_hit_reach",
    "multi_run_hr_setup_hits_only",
]
HR_CONTEXT_PROFILE_BUCKETS = [
    "multi_run_hr",
    "grand_slam",
    "starter_hr",
    "reliever_hr",
    "late_hr",
    "hr_two_out",
    "hr_setup_bb_hbp",
    "hr_setup_non_hit_reach",
    "multi_run_hr_setup_bb_hbp",
    "multi_run_hr_setup_non_hit_reach",
    "multi_run_hr_two_out",
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


def _team_totals(game: Dict[str, Any], side: str) -> Tuple[float, int, float, int, float, int]:
    team = ((game.get("team_batting") or {}).get(side) or {})
    pred = team.get("pred") or {}
    actual = team.get("actual") or {}
    return (
        _safe_float(pred.get("H_mean")),
        _safe_int(actual.get("H")),
        _safe_float(pred.get("HR_mean")),
        _safe_int(actual.get("HR")),
        _safe_float(pred.get("BB_mean")),
        _safe_int(actual.get("BB")),
    )


def _load_games(batch_dir: Path, min_run_gap: float) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for report_path in sorted(batch_dir.glob("sim_vs_actual_*.json")):
        report = _read_json(report_path)
        date = str(((report.get("meta") or {}).get("date")) or report_path.stem.replace("sim_vs_actual_", ""))
        season = _safe_int((report.get("meta") or {}).get("season"))
        for game in report.get("games") or []:
            full = ((game.get("segments") or {}).get("full") or {})
            actual_runs = _safe_int((full.get("actual") or {}).get("away")) + _safe_int((full.get("actual") or {}).get("home"))
            pred_runs = _safe_float(full.get("mean_total_runs"))
            away_pred_h, away_act_h, away_pred_hr, away_act_hr, away_pred_bb, away_act_bb = _team_totals(game, "away")
            home_pred_h, home_act_h, home_pred_hr, home_act_hr, home_pred_bb, home_act_bb = _team_totals(game, "home")
            pred_hits = away_pred_h + home_pred_h
            actual_hits = away_act_h + home_act_h
            pred_hr = away_pred_hr + home_pred_hr
            actual_hr = away_act_hr + home_act_hr
            pred_bb = away_pred_bb + home_pred_bb
            actual_bb = away_act_bb + home_act_bb
            run_gap = actual_runs - pred_runs
            hit_surplus = pred_hits - float(actual_hits)
            row = {
                "date": date,
                "season": season,
                "game_pk": _safe_int(game.get("game_pk")),
                "away": ((game.get("away") or {}).get("abbr")) or ((game.get("away") or {}).get("name")) or "AWAY",
                "home": ((game.get("home") or {}).get("abbr")) or ((game.get("home") or {}).get("name")) or "HOME",
                "pred_runs": round(pred_runs, 3),
                "actual_runs": actual_runs,
                "run_gap": round(run_gap, 3),
                "pred_hits": round(pred_hits, 3),
                "actual_hits": actual_hits,
                "hit_surplus": round(hit_surplus, 3),
                "pred_hr": round(pred_hr, 3),
                "actual_hr": actual_hr,
                "hr_surplus": round(float(actual_hr) - pred_hr, 3),
                "pred_bb": round(pred_bb, 3),
                "actual_bb": actual_bb,
                "bb_surplus": round(float(actual_bb) - pred_bb, 3),
            }
            row["paradox"] = bool(row["run_gap"] >= float(min_run_gap) and row["hit_surplus"] > 0.0)
            rows.append(row)
    return rows


def _feed_path(feed_root: Path, season: int, date: str, game_pk: int) -> Path:
    return feed_root / str(season) / date / f"{game_pk}.json.gz"


def _runner_start(movement: Dict[str, Any]) -> str:
    return str(movement.get("originBase") or movement.get("start") or "")


def _runner_end(movement: Dict[str, Any]) -> str:
    return str(movement.get("end") or "")


def _is_non_hit_reach(event_type: str) -> bool:
    if event_type in NON_HIT_REACH_EXACT:
        return True
    if "error" in event_type:
        return True
    if event_type.startswith("fielders_choice"):
        return True
    return False


def _classify_reach_source(event_type: str) -> str:
    if event_type in BB_HBP_EVENT_TYPES:
        return "bb_hbp"
    if _is_non_hit_reach(event_type):
        return "non_hit_reach"
    if event_type in HIT_REACH_EVENT_TYPES:
        return "hit_reach"
    return "other"


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


def _extract_feed_buckets(feed: Dict[str, Any]) -> Dict[str, int]:
    counts: Counter[str] = Counter()
    starters = _starting_pitcher_ids(feed)
    plays = (((feed.get("liveData") or {}).get("plays") or {}).get("allPlays") or [])
    current_half: Tuple[int, bool] | None = None
    base_state: Dict[str, str] = {}
    for play in plays:
        about = play.get("about") or {}
        half_key = (_safe_int(about.get("inning")), bool(about.get("isTopInning")))
        if half_key != current_half:
            current_half = half_key
            base_state = {}

        result = play.get("result") or {}
        event_type = _lower(result.get("eventType"))
        is_out = bool(result.get("isOut"))
        outs_before = _safe_int((play.get("count") or {}).get("outs"))
        play_event_types = {
            _lower(((entry.get("details") or {}).get("eventType")))
            for entry in (play.get("playEvents") or [])
            if isinstance(entry, dict)
        }
        runners = play.get("runners") or []

        if event_type == "home_run":
            rbi = _safe_int(result.get("rbi"))
            inning = _safe_int(about.get("inning"))
            matchup = play.get("matchup") or {}
            pitcher_id = _safe_int(((matchup.get("pitcher") or {}).get("id")))
            defense_side = "home" if bool(about.get("isTopInning")) else "away"
            starter_id = starters.get(defense_side, 0)
            on_base_sources = list(base_state.values())

            counts["hr_events"] += 1
            if rbi <= 1:
                counts["solo_hr"] += 1
            elif rbi == 2:
                counts["two_run_hr"] += 1
            elif rbi == 3:
                counts["three_run_hr"] += 1
            else:
                counts["grand_slam"] += 1

            if rbi >= 2:
                counts["multi_run_hr"] += 1

            if pitcher_id and pitcher_id == starter_id:
                counts["starter_hr"] += 1
                if rbi >= 2:
                    counts["multi_run_hr_off_starter"] += 1
            else:
                counts["reliever_hr"] += 1
                if rbi >= 2:
                    counts["multi_run_hr_off_reliever"] += 1

            if inning <= 3:
                counts["early_hr"] += 1
            elif inning >= 7:
                counts["late_hr"] += 1

            if outs_before >= 2:
                counts["hr_two_out"] += 1
                if rbi >= 2:
                    counts["multi_run_hr_two_out"] += 1

            if any(source == "bb_hbp" for source in on_base_sources):
                counts["hr_setup_bb_hbp"] += 1
                if rbi >= 2:
                    counts["multi_run_hr_setup_bb_hbp"] += 1
            if any(source == "non_hit_reach" for source in on_base_sources):
                counts["hr_setup_non_hit_reach"] += 1
                if rbi >= 2:
                    counts["multi_run_hr_setup_non_hit_reach"] += 1
            if any(source == "hit_reach" for source in on_base_sources):
                counts["hr_setup_hit_reach"] += 1
                if rbi >= 2:
                    counts["multi_run_hr_setup_hit_reach"] += 1
            if on_base_sources and all(source == "hit_reach" for source in on_base_sources):
                counts["hr_setup_hits_only"] += 1
                if rbi >= 2:
                    counts["multi_run_hr_setup_hits_only"] += 1

        if event_type == "single":
            for runner in runners:
                movement = runner.get("movement") or {}
                if bool(movement.get("isOut")):
                    continue
                start = _runner_start(movement)
                end = _runner_end(movement)
                if start == "2B" and end == "score":
                    counts["single_2b_to_home"] += 1
                if start == "1B" and end == "3B":
                    counts["single_1b_to_3b"] += 1
                if start == "1B" and end == "score":
                    counts["single_1b_to_home"] += 1

        if event_type == "double":
            for runner in runners:
                movement = runner.get("movement") or {}
                if bool(movement.get("isOut")):
                    continue
                start = _runner_start(movement)
                end = _runner_end(movement)
                if start == "1B" and end == "score":
                    counts["double_1b_to_home"] += 1
                if start == "1B" and end == "3B":
                    counts["double_1b_to_3b"] += 1

        if _is_non_hit_reach(event_type):
            counts["non_hit_reach"] += 1
            if "error" in event_type:
                counts["roe_events"] += 1
            if event_type.startswith("fielders_choice"):
                counts["fc_events"] += 1

        misc_types = {value for value in play_event_types if value in MISC_ADVANCE_EVENT_TYPES}
        if event_type in MISC_ADVANCE_EVENT_TYPES:
            misc_types.add(event_type)
        if misc_types:
            counts["misc_advance_events"] += 1
            counts["wild_pitch_events"] += 1 if "wild_pitch" in misc_types else 0
            counts["passed_ball_events"] += 1 if "passed_ball" in misc_types else 0
            counts["balk_events"] += 1 if "balk" in misc_types else 0
            moved = 0
            for runner in runners:
                movement = runner.get("movement") or {}
                if bool(movement.get("isOut")):
                    continue
                start = _runner_start(movement)
                end = _runner_end(movement)
                if start and end and start != end:
                    moved += 1
                    if end == "score":
                        counts["misc_advance_runs"] += 1
            counts["misc_advance_runner_moves"] += moved

        if event_type in STEAL_EVENT_TYPES or any(value in STEAL_EVENT_TYPES for value in play_event_types):
            counts["steals"] += 1

        if is_out and event_type not in EXCLUDED_BASERUN_OUT_EVENTS:
            for runner in runners:
                movement = runner.get("movement") or {}
                if bool(movement.get("isOut")):
                    continue
                start = _runner_start(movement)
                end = _runner_end(movement)
                if start == "3B" and end == "score":
                    counts["productive_out_scoring"] += 1
                elif start == "2B" and end == "3B":
                    counts["productive_out_2b_to_3b"] += 1
                elif start == "1B" and end == "2B":
                    counts["productive_out_1b_to_2b"] += 1

        pre_play_state = dict(base_state)
        for runner in runners:
            movement = runner.get("movement") or {}
            start = _runner_start(movement)
            if start in BASES:
                base_state.pop(start, None)

        batter_source = _classify_reach_source(event_type)
        for runner in runners:
            movement = runner.get("movement") or {}
            if bool(movement.get("isOut")):
                continue
            start = _runner_start(movement)
            end = _runner_end(movement)
            if end not in BASES:
                continue
            source = pre_play_state.get(start) if start in BASES else batter_source
            base_state[end] = source

        for runner in runners:
            details = runner.get("details") or {}
            if not bool(details.get("isScoringEvent")):
                continue
            if bool(details.get("teamUnearned")) or not bool(details.get("earned", True)):
                counts["error_runs"] += 1

    counts["productive_outs"] = (
        counts["productive_out_scoring"]
        + counts["productive_out_2b_to_3b"]
        + counts["productive_out_1b_to_2b"]
    )
    counts["aggressive_advancement"] = (
        counts["single_2b_to_home"]
        + counts["single_1b_to_3b"]
        + counts["single_1b_to_home"]
        + counts["double_1b_to_home"]
        + counts["double_1b_to_3b"]
    )
    return dict(counts)


def _merge_game_and_feed(rows: List[Dict[str, Any]], feed_root: Path) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    for row in rows:
        feed_path = _feed_path(feed_root, row["season"], row["date"], row["game_pk"])
        payload = dict(row)
        payload["feed_path"] = str(feed_path)
        if feed_path.exists():
            payload.update(_extract_feed_buckets(_read_gzip_json(feed_path)))
        else:
            payload["feed_missing"] = True
        merged.append(payload)
    return merged


def _bucket_summary(rows: List[Dict[str, Any]], bucket_keys: Iterable[str]) -> List[Dict[str, Any]]:
    paradox_rows = [row for row in rows if row.get("paradox")]
    non_paradox_rows = [row for row in rows if not row.get("paradox")]
    summary: List[Dict[str, Any]] = []
    for key in bucket_keys:
        all_present = [row for row in rows if _safe_int(row.get(key)) > 0]
        paradox_present = [row for row in paradox_rows if _safe_int(row.get(key)) > 0]
        non_paradox_present = [row for row in non_paradox_rows if _safe_int(row.get(key)) > 0]
        summary.append(
            {
                "bucket": key,
                "paradox_share": round(len(paradox_present) / len(paradox_rows), 3) if paradox_rows else None,
                "non_paradox_share": round(len(non_paradox_present) / len(non_paradox_rows), 3) if non_paradox_rows else None,
                "share_lift": round((len(paradox_present) / len(paradox_rows)) - (len(non_paradox_present) / len(non_paradox_rows)), 3)
                if paradox_rows and non_paradox_rows
                else None,
                "paradox_avg_count": round(mean(_safe_int(row.get(key)) for row in paradox_rows), 3) if paradox_rows else None,
                "non_paradox_avg_count": round(mean(_safe_int(row.get(key)) for row in non_paradox_rows), 3) if non_paradox_rows else None,
                "avg_run_gap_when_present": round(mean(_safe_float(row.get("run_gap")) for row in all_present), 3) if all_present else None,
                "games_present": len(all_present),
            }
        )
    return sorted(
        summary,
        key=lambda row: (
            -999.0 if row.get("share_lift") is None else -float(row["share_lift"]),
            -999.0 if row.get("avg_run_gap_when_present") is None else -float(row["avg_run_gap_when_present"]),
        ),
    )


def _top_profiles(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return _top_profiles_for_keys(rows, PROFILE_BUCKETS)


def _top_profiles_for_keys(rows: List[Dict[str, Any]], profile_buckets: Iterable[str]) -> List[Dict[str, Any]]:
    paradox_rows = [row for row in rows if row.get("paradox")]
    counts: Counter[Tuple[str, ...]] = Counter()
    for row in paradox_rows:
        profile = tuple(bucket for bucket in profile_buckets if _safe_int(row.get(bucket)) > 0)
        counts[profile] += 1
    top: List[Dict[str, Any]] = []
    for profile, count in counts.most_common(10):
        top.append(
            {
                "profile": list(profile),
                "games": count,
                "share": round(count / len(paradox_rows), 3) if paradox_rows else None,
            }
        )
    return top


def _top_hr_games(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    paradox_rows = [row for row in rows if row.get("paradox")]
    paradox_rows.sort(
        key=lambda row: (
            -_safe_float(row.get("run_gap")),
            -_safe_int(row.get("multi_run_hr")),
            -_safe_int(row.get("hr_setup_non_hit_reach")),
            -_safe_int(row.get("hr_setup_bb_hbp")),
            row.get("date", ""),
        )
    )
    top: List[Dict[str, Any]] = []
    for row in paradox_rows[:20]:
        top.append(
            {
                "date": row["date"],
                "matchup": f"{row['away']} @ {row['home']}",
                "game_pk": row["game_pk"],
                "run_gap": row["run_gap"],
                "hr_surplus": row["hr_surplus"],
                "multi_run_hr": _safe_int(row.get("multi_run_hr")),
                "grand_slam": _safe_int(row.get("grand_slam")),
                "starter_hr": _safe_int(row.get("starter_hr")),
                "reliever_hr": _safe_int(row.get("reliever_hr")),
                "late_hr": _safe_int(row.get("late_hr")),
                "hr_two_out": _safe_int(row.get("hr_two_out")),
                "hr_setup_bb_hbp": _safe_int(row.get("hr_setup_bb_hbp")),
                "hr_setup_non_hit_reach": _safe_int(row.get("hr_setup_non_hit_reach")),
                "multi_run_hr_setup_bb_hbp": _safe_int(row.get("multi_run_hr_setup_bb_hbp")),
                "multi_run_hr_setup_non_hit_reach": _safe_int(row.get("multi_run_hr_setup_non_hit_reach")),
            }
        )
    return top


def _top_games(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    paradox_rows = [row for row in rows if row.get("paradox")]
    paradox_rows.sort(key=lambda row: (-_safe_float(row.get("run_gap")), -_safe_float(row.get("hit_surplus")), row.get("date", "")))
    top: List[Dict[str, Any]] = []
    for row in paradox_rows[:20]:
        top.append(
            {
                "date": row["date"],
                "matchup": f"{row['away']} @ {row['home']}",
                "game_pk": row["game_pk"],
                "run_gap": row["run_gap"],
                "hit_surplus": row["hit_surplus"],
                "hr_surplus": row["hr_surplus"],
                "bb_surplus": row["bb_surplus"],
                "productive_outs": _safe_int(row.get("productive_outs")),
                "aggressive_advancement": _safe_int(row.get("aggressive_advancement")),
                "misc_advance_events": _safe_int(row.get("misc_advance_events")),
                "non_hit_reach": _safe_int(row.get("non_hit_reach")),
                "error_runs": _safe_int(row.get("error_runs")),
                "steals": _safe_int(row.get("steals")),
            }
        )
    return top


def _to_markdown(report: Dict[str, Any]) -> str:
    lines: List[str] = []
    summary = report["summary"]
    lines.append("# Residual Paradox Event Buckets")
    lines.append("")
    lines.append(f"- Batch dir: `{report['batch_dir']}`")
    lines.append(f"- Feed root: `{report['feed_root']}`")
    lines.append(f"- Games: {summary['games_total']}")
    lines.append(f"- Paradox games: {summary['paradox_games']} ({summary['paradox_share']})")
    lines.append(f"- Mean run gap in paradox games: {summary['paradox_mean_run_gap']}")
    lines.append(f"- Mean hit surplus in paradox games: {summary['paradox_mean_hit_surplus']}")
    lines.append(f"- HR-surplus games: {summary['hr_surplus_games']} ({summary['hr_surplus_paradox_games']} paradox, share {summary['hr_surplus_paradox_share']})")
    lines.append("")
    lines.append("## Bucket Ranking")
    lines.append("")
    lines.append("| Bucket | Paradox Share | Non-Paradox Share | Share Lift | Paradox Avg Count | Non-Paradox Avg Count | Avg Run Gap When Present | Games Present |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for row in report["bucket_summary"]:
        lines.append(
            "| {bucket} | {paradox_share} | {non_paradox_share} | {share_lift} | {paradox_avg_count} | {non_paradox_avg_count} | {avg_run_gap_when_present} | {games_present} |".format(
                **row
            )
        )
    lines.append("")
    lines.append("## HR-Conditioned Bucket Ranking")
    lines.append("")
    lines.append("Conditioning set: games with `actual HR - predicted HR >= 1`.")
    lines.append("")
    lines.append("| Bucket | Paradox Share | Non-Paradox Share | Share Lift | Paradox Avg Count | Non-Paradox Avg Count | Avg Run Gap When Present | Games Present |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for row in report["hr_context_summary"]:
        lines.append(
            "| {bucket} | {paradox_share} | {non_paradox_share} | {share_lift} | {paradox_avg_count} | {non_paradox_avg_count} | {avg_run_gap_when_present} | {games_present} |".format(
                **row
            )
        )
    lines.append("")
    lines.append("## Top Profiles")
    lines.append("")
    lines.append("| Profile | Games | Share |")
    lines.append("|---|---:|---:|")
    for row in report["top_profiles"]:
        label = ", ".join(row["profile"]) if row["profile"] else "none"
        lines.append(f"| {label} | {row['games']} | {row['share']} |")
    lines.append("")
    lines.append("## HR-Conditioned Top Profiles")
    lines.append("")
    lines.append("| Profile | Games | Share |")
    lines.append("|---|---:|---:|")
    for row in report["hr_context_top_profiles"]:
        label = ", ".join(row["profile"]) if row["profile"] else "none"
        lines.append(f"| {label} | {row['games']} | {row['share']} |")
    lines.append("")
    lines.append("## Top Paradox Games")
    lines.append("")
    lines.append("| Date | Matchup | Run Gap | Hit Surplus | HR Surplus | BB Surplus | Productive Outs | Aggressive Advancement | Misc Advances | Non-Hit Reach | Error Runs | Steals |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in report["top_games"]:
        lines.append(
            f"| {row['date']} | {row['matchup']} | {row['run_gap']} | {row['hit_surplus']} | {row['hr_surplus']} | {row['bb_surplus']} | {row['productive_outs']} | {row['aggressive_advancement']} | {row['misc_advance_events']} | {row['non_hit_reach']} | {row['error_runs']} | {row['steals']} |"
        )
    lines.append("")
    lines.append("## Top HR-Context Paradox Games")
    lines.append("")
    lines.append("| Date | Matchup | Run Gap | HR Surplus | Multi-Run HR | Grand Slam | Starter HR | Reliever HR | Late HR | Two-Out HR | HR Setup BB/HBP | HR Setup Non-Hit Reach | Multi-Run HR Setup BB/HBP | Multi-Run HR Setup Non-Hit Reach |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for row in report["hr_context_top_games"]:
        lines.append(
            f"| {row['date']} | {row['matchup']} | {row['run_gap']} | {row['hr_surplus']} | {row['multi_run_hr']} | {row['grand_slam']} | {row['starter_hr']} | {row['reliever_hr']} | {row['late_hr']} | {row['hr_two_out']} | {row['hr_setup_bb_hbp']} | {row['hr_setup_non_hit_reach']} | {row['multi_run_hr_setup_bb_hbp']} | {row['multi_run_hr_setup_non_hit_reach']} |"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze residual paradox games against raw feed_live event buckets.")
    parser.add_argument("--batch-dir", required=True, help="Path to a batch folder containing sim_vs_actual_*.json files.")
    parser.add_argument(
        "--feed-root",
        default="data/raw/statsapi/feed_live",
        help="Root path for raw feed_live files (season/date/game_pk.json.gz).",
    )
    parser.add_argument(
        "--min-run-gap",
        type=float,
        default=1.0,
        help="Minimum actual minus predicted runs for a game to count as paradox when hit surplus is positive.",
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

    game_rows = _merge_game_and_feed(_load_games(batch_dir, args.min_run_gap), feed_root)
    for row in game_rows:
        row["hr_surplus_ge1"] = 1 if _safe_float(row.get("hr_surplus")) >= 1.0 else 0
        row["bb_surplus_ge2"] = 1 if _safe_float(row.get("bb_surplus")) >= 2.0 else 0

    paradox_rows = [row for row in game_rows if row.get("paradox")]
    hr_surplus_rows = [row for row in game_rows if _safe_int(row.get("hr_surplus_ge1")) > 0]
    hr_surplus_paradox_rows = [row for row in hr_surplus_rows if row.get("paradox")]
    report = {
        "batch_dir": str(batch_dir.resolve()),
        "feed_root": str(feed_root.resolve()),
        "summary": {
            "games_total": len(game_rows),
            "paradox_games": len(paradox_rows),
            "paradox_share": round(len(paradox_rows) / len(game_rows), 3) if game_rows else None,
            "paradox_mean_run_gap": round(mean(_safe_float(row.get("run_gap")) for row in paradox_rows), 3) if paradox_rows else None,
            "paradox_mean_hit_surplus": round(mean(_safe_float(row.get("hit_surplus")) for row in paradox_rows), 3) if paradox_rows else None,
            "paradox_mean_hr_surplus": round(mean(_safe_float(row.get("hr_surplus")) for row in paradox_rows), 3) if paradox_rows else None,
            "paradox_mean_bb_surplus": round(mean(_safe_float(row.get("bb_surplus")) for row in paradox_rows), 3) if paradox_rows else None,
            "hr_surplus_games": len(hr_surplus_rows),
            "hr_surplus_paradox_games": len(hr_surplus_paradox_rows),
            "hr_surplus_paradox_share": round(len(hr_surplus_paradox_rows) / len(hr_surplus_rows), 3) if hr_surplus_rows else None,
        },
        "bucket_summary": _bucket_summary(
            game_rows,
            [
                "hr_surplus_ge1",
                "bb_surplus_ge2",
                "productive_outs",
                "productive_out_scoring",
                "productive_out_2b_to_3b",
                "productive_out_1b_to_2b",
                "aggressive_advancement",
                "single_2b_to_home",
                "single_1b_to_3b",
                "double_1b_to_home",
                "misc_advance_events",
                "misc_advance_runner_moves",
                "misc_advance_runs",
                "non_hit_reach",
                "roe_events",
                "fc_events",
                "error_runs",
                "steals",
            ],
        ),
        "top_profiles": _top_profiles(game_rows),
        "top_games": _top_games(game_rows),
        "hr_context_summary": _bucket_summary(hr_surplus_rows, HR_CONTEXT_BUCKETS),
        "hr_context_top_profiles": _top_profiles_for_keys(hr_surplus_rows, HR_CONTEXT_PROFILE_BUCKETS),
        "hr_context_top_games": _top_hr_games(hr_surplus_rows),
        "games": game_rows,
    }

    out_json = Path(args.out_json) if args.out_json else batch_dir / "paradox_event_bucket_summary.json"
    out_md = Path(args.out_md) if args.out_md else batch_dir / "paradox_event_bucket_summary.md"
    _write_json(out_json, report)
    _write_text(out_md, _to_markdown(report))
    print(json.dumps({
        "summary": report["summary"],
        "top_profiles": report["top_profiles"][:5],
        "top_buckets": report["bucket_summary"][:10],
        "top_hr_context_buckets": report["hr_context_summary"][:10],
        "out_json": str(out_json),
        "out_md": str(out_md),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())