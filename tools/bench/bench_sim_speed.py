from __future__ import annotations

import argparse
import sys
import time
from dataclasses import replace
from pathlib import Path

# Ensure the project root (MLB-BettingV2/) is importable when running this file directly.
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sim_engine.models import (  # noqa: E402
    BatterProfile,
    GameConfig,
    Handedness,
    Lineup,
    ManagerProfile,
    PitcherProfile,
    Player,
    PitchType,
    Team,
    TeamRoster,
)
from sim_engine.simulate import simulate_game  # noqa: E402


def _make_batter(pid: int) -> BatterProfile:
    # Mildly realistic-ish rates (not intended for modeling, only perf)
    return BatterProfile(
        player=Player(
            mlbam_id=pid,
            full_name=f"B{pid}",
            primary_position="1B",
            bat_side=Handedness.R,
            throw_side=Handedness.R,
        ),
        k_rate=0.22,
        bb_rate=0.08,
        hbp_rate=0.008,
        hr_rate=0.035,
        inplay_hit_rate=0.275,
        xb_hit_share=0.28,
        sb_attempt_rate=0.02,
        sb_success_rate=0.72,
    )


def _make_pitcher(pid: int, stamina: int = 90) -> PitcherProfile:
    return PitcherProfile(
        player=Player(
            mlbam_id=pid,
            full_name=f"P{pid}",
            primary_position="P",
            bat_side=Handedness.R,
            throw_side=Handedness.R,
        ),
        k_rate=0.24,
        bb_rate=0.08,
        hbp_rate=0.008,
        hr_rate=0.035,
        inplay_hit_rate=0.27,
        arsenal={PitchType.FF: 0.55, PitchType.SL: 0.25, PitchType.CH: 0.20},
        stamina_pitches=stamina,
    )


def _toy_roster(team_id: int, abbr: str, base_id: int) -> TeamRoster:
    team = Team(team_id=team_id, name=abbr, abbreviation=abbr)
    mgr = ManagerProfile()

    batters = [_make_batter(base_id + i) for i in range(1, 10)]
    starter = _make_pitcher(base_id + 100, stamina=95)
    bullpen = [_make_pitcher(base_id + 200 + i, stamina=25) for i in range(8)]

    lineup = Lineup(batters=batters, pitcher=starter, bench=[], bullpen=bullpen)
    return TeamRoster(team=team, manager=mgr, lineup=lineup)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=50)
    ap.add_argument("--mode", choices=["off", "legacy", "v2"], default="v2")
    ap.add_argument("--pbp", choices=["off", "pa", "pitch"], default="off")
    ap.add_argument("--seed", type=int, default=123)
    args = ap.parse_args()

    away = _toy_roster(1, "AWY", 100000)
    home = _toy_roster(2, "HOM", 200000)

    cfg = GameConfig(rng_seed=int(args.seed), manager_pitching=str(args.mode), pbp=str(args.pbp))

    t0 = time.perf_counter()
    last = None
    for i in range(int(args.n)):
        # Vary rng seed per run to avoid any accidental caching artifacts.
        r = simulate_game(away, home, replace(cfg, rng_seed=int(args.seed) + i))
        last = r
    dt = time.perf_counter() - t0

    per_game = (dt / max(1, int(args.n)))
    print(f"n={int(args.n)} mode={args.mode} pbp={args.pbp} total_sec={dt:.3f} sec_per_game={per_game:.4f}")
    if last is not None:
        print(f"last_score away={last.away_score} home={last.home_score} innings={last.innings_played}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
