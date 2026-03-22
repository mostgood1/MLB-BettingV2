from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(path)


def _run(cmd: List[str], cwd: Path, child_stdout: str) -> int:
    if str(child_stdout) == "quiet":
        p = subprocess.run(cmd, cwd=str(cwd), stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    else:
        p = subprocess.run(cmd, cwd=str(cwd))
    return int(p.returncode)


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            import ctypes

            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
            if handle:
                ctypes.windll.kernel32.CloseHandle(handle)
                return True
            return False
        except Exception:
            return False

    try:
        os.kill(int(pid), 0)
        return True
    except OSError:
        return False


def _acquire_out_dir_lock(out_dir: Path) -> Path:
    lock_path = out_dir / ".run_batch_eval_days.lock.json"
    payload = {
        "pid": os.getpid(),
        "python": sys.executable,
        "cwd": str(Path.cwd()),
        "started_at": datetime.now().isoformat(),
        "argv": sys.argv,
    }

    for _ in range(2):
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
                f.write("\n")
            return lock_path
        except FileExistsError:
            try:
                existing = json.loads(lock_path.read_text(encoding="utf-8"))
                existing_pid = int(existing.get("pid") or 0)
                if _pid_is_alive(existing_pid):
                    raise RuntimeError(
                        f"Lock exists for {out_dir} (pid={existing_pid}). "
                        f"Refusing to run concurrently. If stale, delete {lock_path}."
                    )
            except json.JSONDecodeError:
                pass

            try:
                lock_path.unlink(missing_ok=True)
            except OSError:
                time.sleep(0.2)

    raise RuntimeError(
        f"Could not acquire lock for {out_dir}. If safe, delete {lock_path} and retry."
    )


def _release_out_dir_lock(lock_path: Path) -> None:
    try:
        if not lock_path.exists():
            return
        existing = json.loads(lock_path.read_text(encoding="utf-8"))
        if int(existing.get("pid") or 0) != os.getpid():
            return
        lock_path.unlink(missing_ok=True)
    except Exception:
        return


def main() -> int:
    ap = argparse.ArgumentParser(description="Run eval_sim_day_vs_actual.py across multiple dates")
    ap.add_argument("--dates", default="", help="Comma-separated YYYY-MM-DD list")
    ap.add_argument("--date-file", default="", help="Text file with one YYYY-MM-DD per line")
    ap.add_argument(
        "--spring-mode",
        choices=["on", "off"],
        default="off",
        help="If on, pass --spring-mode on to eval_sim_day_vs_actual.py.",
    )
    ap.add_argument(
        "--stats-season",
        type=int,
        default=0,
        help="If >0, pass --stats-season to eval_sim_day_vs_actual.py.",
    )
    ap.add_argument(
        "--use-daily-snapshots",
        choices=["on", "off"],
        default="on",
        help="If on, pass --use-daily-snapshots to eval_sim_day_vs_actual.py.",
    )
    ap.add_argument(
        "--daily-snapshots-root",
        default="data/daily/snapshots",
        help="Pass through to eval_sim_day_vs_actual.py.",
    )
    ap.add_argument(
        "--use-roster-artifacts",
        choices=["on", "off"],
        default="on",
        help="Pass through to eval_sim_day_vs_actual.py --use-roster-artifacts.",
    )
    ap.add_argument(
        "--write-roster-artifacts",
        choices=["on", "off"],
        default="off",
        help="Pass through to eval_sim_day_vs_actual.py --write-roster-artifacts.",
    )
    ap.add_argument(
        "--lineups-last-known",
        default="",
        help="Optional path to lineups_last_known_by_team.json (pass through).",
    )
    ap.add_argument("--sims-per-game", type=int, default=1000)
    ap.add_argument(
        "--bvp-hr",
        choices=["on", "off"],
        default="off",
        help="If on, apply a shrunk batter-vs-starter HR multiplier from local Statcast raw pitch files (passed through).",
    )
    ap.add_argument("--bvp-days-back", type=int, default=365, help="How many days of history to consider for BvP lookup (passed through).")
    ap.add_argument("--bvp-min-pa", type=int, default=10, help="Minimum BvP PA required to apply a multiplier (passed through).")
    ap.add_argument("--bvp-shrink-pa", type=float, default=50.0, help="Shrinkage PA constant (passed through).")
    ap.add_argument("--bvp-clamp-lo", type=float, default=0.80, help="Lower clamp for BvP HR multiplier (passed through).")
    ap.add_argument("--bvp-clamp-hi", type=float, default=1.25, help="Upper clamp for BvP HR multiplier (passed through).")
    ap.add_argument(
        "--hitter-hr-topn",
        type=int,
        default=0,
        help="If >0, include top-N lineup batters by HR likelihood in each per-day report (passed through).",
    )
    ap.add_argument(
        "--hitter-props-topn",
        type=int,
        default=24,
        help=(
            "Top-N size for broader hitter props. Default 24. "
            "-1=use --hitter-hr-topn (back-compat), 0=disable (passed through)."
        ),
    )
    ap.add_argument("--jobs", type=int, default=4)
    ap.add_argument("--use-raw", choices=["on", "off"], default="on")
    ap.add_argument("--prop-lines-source", choices=["auto", "oddsapi", "last_known", "bovada", "off"], default="last_known")
    ap.add_argument(
        "--market-push-policy",
        choices=["loss", "half", "skip"],
        default="skip",
        help=(
            "How to score exact-at-line outcomes (pushes) for pitcher O/U at market lines. "
            "loss=treat push as not-over (y=0), half=soft label y=0.5, skip=exclude push rows from scoring (passed through)."
        ),
    )
    ap.add_argument(
        "--child-stdout",
        choices=["inherit", "quiet"],
        default="inherit",
        help="Whether to inherit or silence stdout/stderr from eval_sim_day_vs_actual.py subprocesses",
    )
    ap.add_argument(
        "--retries",
        type=int,
        default=1,
        help="Retry a failed date up to N additional times (useful for transient API/network flake).",
    )
    ap.add_argument(
        "--pitcher-rate-sampling",
        choices=["on", "off"],
        default="on",
        help="Toggle per-game pitcher day-rate sampling (uncertainty)",
    )
    ap.add_argument(
        "--stamina-mode",
        choices=["season", "season_bullpen65", "prior", "legacy92"],
        default="season",
        help="Ablation hook for pitcher stamina_pitches (passed through to eval_sim_day_vs_actual.py)",
    )
    ap.add_argument(
        "--umpire-mode",
        choices=["factors", "neutral"],
        default="factors",
        help="Ablation hook for umpire called-strike multiplier (passed through to eval_sim_day_vs_actual.py)",
    )
    ap.add_argument(
        "--umpire-shrink",
        type=float,
        default=0.75,
        help="Shrink umpire called_strike_mult toward 1.0 (passed through). 1.0=no shrink, 0.5=half effect.",
    )
    ap.add_argument(
        "--manager-pitching",
        choices=["off", "legacy", "v2"],
        default="v2",
        help="Pitching change / bullpen management model (passed through to eval_sim_day_vs_actual.py)",
    )
    ap.add_argument(
        "--manager-pitching-overrides",
        default="data/tuning/manager_pitching_overrides/default.json",
        help=(
            "JSON dict or path to JSON file (passed to eval_sim_day_vs_actual.py --manager-pitching-overrides). "
            "Use --manager-pitching-overrides '' to disable."
        ),
    )
    ap.add_argument(
        "--bip-baserunning",
        choices=["on", "off"],
        default="on",
        help="Toggle batted-ball-informed baserunning (DP/SF/advancement)",
    )
    ap.add_argument(
        "--batter-vs-pitch-type",
        choices=["on", "off"],
        default="on",
        help="Toggle Statcast-derived batter vs pitch-type multipliers (passed through)",
    )
    ap.add_argument(
        "--batter-platoon",
        choices=["on", "off"],
        default="on",
        help="Toggle batter platoon split multipliers (passed through)",
    )
    ap.add_argument(
        "--pitcher-platoon",
        choices=["on", "off"],
        default="on",
        help="Toggle pitcher platoon split multipliers (passed through)",
    )
    ap.add_argument(
        "--batter-platoon-alpha",
        type=float,
        default=0.55,
        help="Shrink batter platoon multipliers toward 1.0 (passed through).",
    )
    ap.add_argument(
        "--pitcher-platoon-alpha",
        type=float,
        default=0.55,
        help="Shrink pitcher platoon multipliers toward 1.0 (passed through).",
    )
    ap.add_argument(
        "--batter-recency-games",
        type=int,
        default=14,
        help="Recent-games window for batter recency blend when building rosters (passed through).",
    )
    ap.add_argument(
        "--batter-recency-weight",
        type=float,
        default=0.15,
        help="Weight for batter recency blend (passed through).",
    )
    ap.add_argument(
        "--pitcher-recency-games",
        type=int,
        default=6,
        help="Recent-games window for pitcher recency blend when building rosters (passed through).",
    )
    ap.add_argument(
        "--pitcher-recency-weight",
        type=float,
        default=0.15,
        help="Weight for pitcher recency blend (passed through).",
    )
    ap.add_argument(
        "--weather-hr-weight",
        type=float,
        default=1.0,
        help="Exponent weight for weather HR multiplier (mult^weight) (passed through).",
    )
    ap.add_argument(
        "--weather-inplay-hit-weight",
        type=float,
        default=1.0,
        help="Exponent weight for weather in-play hit multiplier (mult^weight) (passed through).",
    )
    ap.add_argument(
        "--weather-xb-share-weight",
        type=float,
        default=1.0,
        help="Exponent weight for weather XB share multiplier (mult^weight) (passed through).",
    )
    ap.add_argument(
        "--park-hr-weight",
        type=float,
        default=1.0,
        help="Exponent weight for park HR multiplier (mult^weight) (passed through).",
    )
    ap.add_argument(
        "--park-inplay-hit-weight",
        type=float,
        default=1.0,
        help="Exponent weight for park in-play hit multiplier (mult^weight) (passed through).",
    )
    ap.add_argument(
        "--park-xb-share-weight",
        type=float,
        default=1.0,
        help="Exponent weight for park XB share multiplier (mult^weight) (passed through).",
    )
    ap.add_argument(
        "--bip-dp-rate",
        type=float,
        default=None,
        help="Override DP rate on in-play ground-ball outs (passed through to eval_sim_day_vs_actual.py). If omitted, uses GameConfig default (currently 0.0).",
    )
    ap.add_argument(
        "--bip-sf-rate-flypop",
        type=float,
        default=None,
        help="Override sac-fly rate for fly/pop outs (passed through to eval_sim_day_vs_actual.py)",
    )
    ap.add_argument(
        "--bip-sf-rate-line",
        type=float,
        default=None,
        help="Override sac-fly rate for line-drive outs (passed through to eval_sim_day_vs_actual.py)",
    )
    ap.add_argument(
        "--bip-1b-p2-scores-mult",
        type=float,
        default=None,
        help="Scale probability runner on 2B scores on 1B (passed through to eval_sim_day_vs_actual.py)",
    )
    ap.add_argument(
        "--bip-2b-p1-scores-mult",
        type=float,
        default=None,
        help="Scale probability runner on 1B scores on 2B (passed through to eval_sim_day_vs_actual.py)",
    )
    ap.add_argument(
        "--bip-1b-p1-to-3b-rate",
        type=float,
        default=None,
        help="Override probability runner on 1B advances to 3B on a 1B when not forced (passed through).",
    )
    ap.add_argument(
        "--bip-ground-rbi-out-rate",
        type=float,
        default=None,
        help="Override probability of a ground-ball RBI out with runner on 3B and less than 2 outs (passed through).",
    )
    ap.add_argument(
        "--bip-out-2b-to-3b-rate",
        type=float,
        default=None,
        help="Override probability runner on 2B advances to 3B on a productive out (passed through).",
    )
    ap.add_argument(
        "--bip-out-1b-to-2b-rate",
        type=float,
        default=None,
        help="Override probability runner on 1B advances to 2B on a productive out (passed through).",
    )
    ap.add_argument(
        "--bip-misc-advance-pitch-rate",
        type=float,
        default=None,
        help="Override probability of a WP/PB/balk-style runner advance on a non-in-play pitch (passed through).",
    )
    ap.add_argument(
        "--bip-roe-rate",
        type=float,
        default=None,
        help="Override probability an in-play out becomes reach-on-error (passed through).",
    )
    ap.add_argument(
        "--bip-fc-rate",
        type=float,
        default=None,
        help="Override probability of a fielder's-choice style out on a ground ball with runner on 1B (passed through).",
    )
    ap.add_argument(
        "--bip-fc-runner-on-3b-score-rate",
        type=float,
        default=None,
        help="Override probability a runner on 3B scores on a fielder's-choice ground ball (passed through).",
    )
    ap.add_argument(
        "--pitcher-distribution-overrides",
        default="",
        help="JSON dict or path to JSON file (passed to eval_sim_day_vs_actual.py --pitcher-distribution-overrides)",
    )
    ap.add_argument(
        "--pitch-model-overrides",
        default="",
        help="JSON dict or path to JSON file (passed to eval_sim_day_vs_actual.py --pitch-model-overrides)",
    )
    ap.add_argument(
        "--market-game-config-overrides",
        default="",
        help="JSON dict or path to JSON file with market-context per-game config rules (passed through).",
    )
    ap.add_argument(
        "--so-prob-calibration",
        default="data/tuning/so_calibration/default.json",
        help="JSON dict or path to JSON file (passed to eval_sim_day_vs_actual.py --so-prob-calibration)",
    )
    ap.add_argument(
        "--outs-prob-calibration",
        default="data/tuning/outs_calibration/default.json",
        help="JSON dict or path to JSON file (passed to eval_sim_day_vs_actual.py --outs-prob-calibration)",
    )
    ap.add_argument(
        "--hitter-hr-prob-calibration",
        default="data/tuning/hitter_hr_calibration/default.json",
        help="JSON dict or path to JSON file (passed to eval_sim_day_vs_actual.py --hitter-hr-prob-calibration)",
    )
    ap.add_argument(
        "--hitter-props-prob-calibration",
        default="data/tuning/hitter_props_calibration/default.json",
        help="JSON dict or path to JSON file (passed to eval_sim_day_vs_actual.py --hitter-props-prob-calibration)",
    )
    ap.add_argument(
        "--progress",
        choices=["on", "off"],
        default="on",
        help="Print per-date progress/timing (helps when child stdout is quiet)",
    )
    ap.add_argument("--batch-out", default="", help="Output folder (default: data/eval/batches/<timestamp>)")
    args = ap.parse_args()

    v2_root = Path(__file__).resolve().parents[2]
    tool_path = v2_root / "tools" / "eval" / "eval_sim_day_vs_actual.py"

    dates: List[str] = []
    if str(args.dates).strip():
        dates.extend([x.strip() for x in str(args.dates).split(",") if x.strip()])
    if str(args.date_file).strip():
        p = Path(args.date_file)
        if not p.is_absolute():
            p = (Path.cwd() / p).resolve()
        if p.exists():
            for line in p.read_text(encoding="utf-8").splitlines():
                s = line.strip().lstrip("\ufeff")
                if s and not s.startswith("#"):
                    dates.append(s)

    dates = sorted(set(dates))
    if not dates:
        print("No dates provided. Use --dates or --date-file.")
        return 2

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if str(args.batch_out).strip():
        out_dir = Path(args.batch_out)
        if not out_dir.is_absolute():
            out_dir = (v2_root / out_dir).resolve()
    else:
        out_dir = (v2_root / "data" / "eval" / "batches" / stamp)
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        lock_path = _acquire_out_dir_lock(out_dir)
    except RuntimeError as e:
        print(str(e))
        return 3

    py = sys.executable
    meta: Dict[str, Any] = {
        "generated_at": datetime.now().isoformat(),
        "python": py,
        "spring_mode": str(args.spring_mode),
        "stats_season": int(args.stats_season),
        "use_daily_snapshots": str(args.use_daily_snapshots),
        "daily_snapshots_root": str(args.daily_snapshots_root),
        "lineups_last_known": str(args.lineups_last_known or ""),
        "sims_per_game": int(args.sims_per_game),
        "bvp_hr": str(args.bvp_hr),
        "bvp_days_back": int(args.bvp_days_back),
        "bvp_min_pa": int(args.bvp_min_pa),
        "bvp_shrink_pa": float(args.bvp_shrink_pa),
        "bvp_clamp_lo": float(args.bvp_clamp_lo),
        "bvp_clamp_hi": float(args.bvp_clamp_hi),
        "hitter_hr_top_n": int(args.hitter_hr_topn),
        "hitter_props_top_n": int(args.hitter_props_topn),
        "jobs": int(args.jobs),
        "use_raw": str(args.use_raw),
        "prop_lines_source": str(args.prop_lines_source),
        "market_push_policy": str(args.market_push_policy),
        "child_stdout": str(args.child_stdout),
        "retries": int(args.retries),
        "pitcher_rate_sampling": str(args.pitcher_rate_sampling),
        "stamina_mode": str(args.stamina_mode),
        "umpire_mode": str(args.umpire_mode),
        "umpire_shrink": float(args.umpire_shrink),
        "manager_pitching": str(args.manager_pitching),
        "manager_pitching_overrides": str(args.manager_pitching_overrides or ""),
        "bip_baserunning": str(args.bip_baserunning),
        "batter_vs_pitch_type": str(args.batter_vs_pitch_type),
        "batter_platoon": str(args.batter_platoon),
        "pitcher_platoon": str(args.pitcher_platoon),
        "batter_platoon_alpha": float(args.batter_platoon_alpha),
        "pitcher_platoon_alpha": float(args.pitcher_platoon_alpha),
        "batter_recency_games": int(args.batter_recency_games),
        "batter_recency_weight": float(args.batter_recency_weight),
        "pitcher_recency_games": int(args.pitcher_recency_games),
        "pitcher_recency_weight": float(args.pitcher_recency_weight),
        "weather_hr_weight": float(args.weather_hr_weight),
        "weather_inplay_hit_weight": float(args.weather_inplay_hit_weight),
        "weather_xb_share_weight": float(args.weather_xb_share_weight),
        "park_hr_weight": float(args.park_hr_weight),
        "park_inplay_hit_weight": float(args.park_inplay_hit_weight),
        "park_xb_share_weight": float(args.park_xb_share_weight),
        "bip_dp_rate": (None if args.bip_dp_rate is None else float(args.bip_dp_rate)),
        "bip_sf_rate_flypop": (None if args.bip_sf_rate_flypop is None else float(args.bip_sf_rate_flypop)),
        "bip_sf_rate_line": (None if args.bip_sf_rate_line is None else float(args.bip_sf_rate_line)),
        "bip_1b_p2_scores_mult": (None if args.bip_1b_p2_scores_mult is None else float(args.bip_1b_p2_scores_mult)),
        "bip_2b_p1_scores_mult": (None if args.bip_2b_p1_scores_mult is None else float(args.bip_2b_p1_scores_mult)),
        "bip_1b_p1_to_3b_rate": (None if args.bip_1b_p1_to_3b_rate is None else float(args.bip_1b_p1_to_3b_rate)),
        "bip_ground_rbi_out_rate": (None if args.bip_ground_rbi_out_rate is None else float(args.bip_ground_rbi_out_rate)),
        "bip_out_2b_to_3b_rate": (None if args.bip_out_2b_to_3b_rate is None else float(args.bip_out_2b_to_3b_rate)),
        "bip_out_1b_to_2b_rate": (None if args.bip_out_1b_to_2b_rate is None else float(args.bip_out_1b_to_2b_rate)),
        "bip_misc_advance_pitch_rate": (None if args.bip_misc_advance_pitch_rate is None else float(args.bip_misc_advance_pitch_rate)),
        "bip_roe_rate": (None if args.bip_roe_rate is None else float(args.bip_roe_rate)),
        "bip_fc_rate": (None if args.bip_fc_rate is None else float(args.bip_fc_rate)),
        "bip_fc_runner_on_3b_score_rate": (None if args.bip_fc_runner_on_3b_score_rate is None else float(args.bip_fc_runner_on_3b_score_rate)),
        "pitcher_distribution_overrides": str(args.pitcher_distribution_overrides or ""),
        "pitch_model_overrides": str(args.pitch_model_overrides or ""),
        "market_game_config_overrides": str(args.market_game_config_overrides or ""),
        "so_prob_calibration": str(args.so_prob_calibration or ""),
        "outs_prob_calibration": str(args.outs_prob_calibration or ""),
        "hitter_hr_prob_calibration": str(args.hitter_hr_prob_calibration or ""),
        "hitter_props_prob_calibration": str(args.hitter_props_prob_calibration or ""),
        "dates": dates,
        "reports": [],
        "failures": [],
    }

    try:
        n_dates = len(dates)
        for i, d in enumerate(dates, start=1):
            out_path = out_dir / f"sim_vs_actual_{d}.json"
            if str(args.progress) == "on":
                print(f"[{i}/{n_dates}] {d} -> {out_path.name}", flush=True)
            t0 = time.perf_counter()
            cmd = [
                py,
                str(tool_path),
                "--date",
                str(d),
                "--spring-mode",
                str(args.spring_mode),
                "--stats-season",
                str(int(args.stats_season)),
                "--use-daily-snapshots",
                str(args.use_daily_snapshots),
                "--daily-snapshots-root",
                str(args.daily_snapshots_root),
                "--use-roster-artifacts",
                str(args.use_roster_artifacts),
                "--write-roster-artifacts",
                str(args.write_roster_artifacts),
                "--sims-per-game",
                str(int(args.sims_per_game)),
                "--bvp-hr",
                str(args.bvp_hr),
                "--bvp-days-back",
                str(int(args.bvp_days_back)),
                "--bvp-min-pa",
                str(int(args.bvp_min_pa)),
                "--bvp-shrink-pa",
                str(float(args.bvp_shrink_pa)),
                "--bvp-clamp-lo",
                str(float(args.bvp_clamp_lo)),
                "--bvp-clamp-hi",
                str(float(args.bvp_clamp_hi)),
                "--hitter-hr-topn",
                str(int(args.hitter_hr_topn)),
                "--hitter-props-topn",
                str(int(args.hitter_props_topn)),
                "--jobs",
                str(int(args.jobs)),
                "--use-raw",
                str(args.use_raw),
                "--prop-lines-source",
                str(args.prop_lines_source),
                "--market-push-policy",
                str(args.market_push_policy),
                "--pitch-model-overrides",
                str(args.pitch_model_overrides or ""),
                "--market-game-config-overrides",
                str(args.market_game_config_overrides or ""),
                "--batter-vs-pitch-type",
                str(args.batter_vs_pitch_type),
                "--batter-platoon",
                str(args.batter_platoon),
                "--pitcher-platoon",
                str(args.pitcher_platoon),
                "--batter-platoon-alpha",
                str(float(args.batter_platoon_alpha)),
                "--pitcher-platoon-alpha",
                str(float(args.pitcher_platoon_alpha)),
                "--batter-recency-games",
                str(int(args.batter_recency_games)),
                "--batter-recency-weight",
                str(float(args.batter_recency_weight)),
                "--pitcher-recency-games",
                str(int(args.pitcher_recency_games)),
                "--pitcher-recency-weight",
                str(float(args.pitcher_recency_weight)),
                "--weather-hr-weight",
                str(float(args.weather_hr_weight)),
                "--weather-inplay-hit-weight",
                str(float(args.weather_inplay_hit_weight)),
                "--weather-xb-share-weight",
                str(float(args.weather_xb_share_weight)),
                "--park-hr-weight",
                str(float(args.park_hr_weight)),
                "--park-inplay-hit-weight",
                str(float(args.park_inplay_hit_weight)),
                "--park-xb-share-weight",
                str(float(args.park_xb_share_weight)),
                "--bip-baserunning",
                str(args.bip_baserunning),
                "--manager-pitching",
                str(args.manager_pitching),
                "--manager-pitching-overrides",
                str(args.manager_pitching_overrides or ""),
                "--so-prob-calibration",
                str(args.so_prob_calibration or ""),
            ]
            if str(args.lineups_last_known or "").strip():
                cmd += ["--lineups-last-known", str(args.lineups_last_known)]
            if str(args.outs_prob_calibration or "").strip():
                cmd += ["--outs-prob-calibration", str(args.outs_prob_calibration)]
            if str(args.hitter_hr_prob_calibration or "").strip():
                cmd += ["--hitter-hr-prob-calibration", str(args.hitter_hr_prob_calibration)]
            if str(args.hitter_props_prob_calibration or "").strip():
                cmd += ["--hitter-props-prob-calibration", str(args.hitter_props_prob_calibration)]
            if args.bip_dp_rate is not None:
                cmd += ["--bip-dp-rate", str(float(args.bip_dp_rate))]
            if args.bip_sf_rate_flypop is not None:
                cmd += ["--bip-sf-rate-flypop", str(float(args.bip_sf_rate_flypop))]
            if args.bip_sf_rate_line is not None:
                cmd += ["--bip-sf-rate-line", str(float(args.bip_sf_rate_line))]
            if args.bip_1b_p2_scores_mult is not None:
                cmd += ["--bip-1b-p2-scores-mult", str(float(args.bip_1b_p2_scores_mult))]
            if args.bip_2b_p1_scores_mult is not None:
                cmd += ["--bip-2b-p1-scores-mult", str(float(args.bip_2b_p1_scores_mult))]
            if args.bip_1b_p1_to_3b_rate is not None:
                cmd += ["--bip-1b-p1-to-3b-rate", str(float(args.bip_1b_p1_to_3b_rate))]
            if args.bip_ground_rbi_out_rate is not None:
                cmd += ["--bip-ground-rbi-out-rate", str(float(args.bip_ground_rbi_out_rate))]
            if args.bip_out_2b_to_3b_rate is not None:
                cmd += ["--bip-out-2b-to-3b-rate", str(float(args.bip_out_2b_to_3b_rate))]
            if args.bip_out_1b_to_2b_rate is not None:
                cmd += ["--bip-out-1b-to-2b-rate", str(float(args.bip_out_1b_to_2b_rate))]
            if args.bip_misc_advance_pitch_rate is not None:
                cmd += ["--bip-misc-advance-pitch-rate", str(float(args.bip_misc_advance_pitch_rate))]
            if args.bip_roe_rate is not None:
                cmd += ["--bip-roe-rate", str(float(args.bip_roe_rate))]
            if args.bip_fc_rate is not None:
                cmd += ["--bip-fc-rate", str(float(args.bip_fc_rate))]
            if args.bip_fc_runner_on_3b_score_rate is not None:
                cmd += ["--bip-fc-runner-on-3b-score-rate", str(float(args.bip_fc_runner_on_3b_score_rate))]

            cmd += [
                "--pitcher-rate-sampling",
                str(args.pitcher_rate_sampling),
                "--stamina-mode",
                str(args.stamina_mode),
                "--umpire-mode",
                str(args.umpire_mode),
                "--umpire-shrink",
                str(float(args.umpire_shrink)),
                "--pitcher-distribution-overrides",
                str(args.pitcher_distribution_overrides or ""),
                "--out",
                str(out_path),
            ]
            max_attempts = 1 + max(0, int(args.retries))
            rc = 1
            attempts = 0
            while attempts < max_attempts:
                attempts += 1
                rc = _run(cmd, cwd=v2_root, child_stdout=str(args.child_stdout))
                if rc == 0:
                    break
                if attempts < max_attempts:
                    time.sleep(2.0)
            if str(args.progress) == "on":
                dt = time.perf_counter() - t0
                extra = "" if attempts <= 1 else f" attempts={attempts}"
                print(f"[{i}/{n_dates}] {d} rc={rc} elapsed={dt:.1f}s{extra}", flush=True)
            if rc != 0:
                meta["failures"].append({"date": d, "returncode": rc, "attempts": attempts})
            else:
                meta["reports"].append(str(out_path))

        _write_json(out_dir / "batch_meta.json", meta)
        print(f"Wrote batch meta: {out_dir / 'batch_meta.json'}")
        print("Reports:", len(meta["reports"]), "Failures:", len(meta["failures"]))
        return 0 if not meta["failures"] else 1
    finally:
        _release_out_dir_lock(lock_path)


if __name__ == "__main__":
    raise SystemExit(main())
