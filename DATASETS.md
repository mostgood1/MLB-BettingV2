# V2 datasets

This repo keeps **MLB-BettingV2/** self-contained and reproducible.

Principles:
- Prefer **StatsAPI** and **Statcast** (via `pybaseball`) as primary sources.
- Keep ARM64 runtime light: Statcast pulls run in **.venv_x64**; ARM64 sim reads artifacts.
- Store **raw inputs** under `data/raw/` and derived maps under `data/<domain>/`.

## Storage layout

- `data/raw/statsapi/feed_live/<season>/<date>/<gamePk>.json.gz`
  - Full `v1.1/game/<gamePk>/feed/live` payload per game.

- `data/raw/statcast/pitches/<season>/<YYYY-MM>/statcast_<YYYY-MM-DD>_<YYYY-MM-DD>.csv.gz`
  - Pitch-level Statcast pulls, partitioned by month (or arbitrary windows).

Derived outputs (consumed by sim):
- `data/umpire/umpire_factors.json`
- `data/park/park_factors.json` (future)
- `data/manager/manager_tendencies.json` (future)
- `data/weather/weather_history.json` (optional, future)

## Backfill tools

### 1) StatsAPI feed/live backfill (ARM64 safe)

- `tools/datasets/backfill_statsapi_feed_live.py --start-date 2025-03-01 --end-date 2025-11-30 --season 2025`

Writes compressed JSON into `data/raw/statsapi/feed_live/...`.

### 2) Statcast pitch events backfill (x64 only)

Requires `.venv_x64` with `pybaseball` installed (see `STATCAST_X64_SETUP.md`).

- `.venv_x64\Scripts\python.exe tools\datasets\backfill_statcast_pitches_x64.py --start-date 2025-03-01 --end-date 2025-11-30 --season 2025`

Writes compressed CSV windows into `data/raw/statcast/pitches/...`.

## Derived builders (next)

These produce lightweight JSON maps used by the sim and daily snapshots.

- Park factors (from Statcast pitch events partitioned by venue)
  - Tool: `tools/datasets/build_park_factors_from_raw.py`
  - Output: `data/park/park_factors.json`
  - Example:
    - `C:/Users/mostg/OneDrive/Coding/MLB-BettingV2/.venv/Scripts/python.exe tools/datasets/build_park_factors_from_raw.py --season 2025 --start-date 2025-03-01 --end-date 2025-11-30 --min-pa 4000`
  - Intended use: replace/augment the current geometry heuristic.

- Manager tendencies (from StatsAPI `feed/live`)
  - Tool: `tools/datasets/build_manager_tendencies_from_feed_live.py`
  - Output: `data/manager/manager_tendencies.json`
  - Example:
    - `C:/Users/mostg/OneDrive/Coding/MLB-BettingV2/.venv/Scripts/python.exe tools/datasets/build_manager_tendencies_from_feed_live.py --season 2025 --start-date 2025-03-01 --end-date 2025-11-30`
  - Intended use: starter leash, bullpen usage, pinch-hit aggressiveness.

## Suggested backfill windows

- For modeling: backfill the full regular season.
- For quick iteration: backfill the last 30–60 days.

## Why these datasets

With raw feeds + Statcast pitch events you can derive:
- play/inning/game/player aggregates
- park factors (venue run/HR environment)
- umpire zone/called-strike tendencies
- manager tendencies (starter leash, bullpen usage, pinch hit rate)

