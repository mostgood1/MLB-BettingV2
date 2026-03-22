# Statcast splits on Windows ARM64 (x64 helper)

On Windows ARM64, `pybaseball` may fail to install because it pulls `cryptography` and wheels are often missing for `win_arm64`.

The V2 simulator is set up to **read cached Statcast splits only**. You populate the cache using a **Windows x64** Python environment (Option A).

## 1) Install Python x64

- Install **Python for Windows (x86-64)** from python.org.
- Verify you have an x64 interpreter (example):
  - `py -3.11-64 -c "import platform; print(platform.machine())"`
  - You want: `AMD64`

## 2) Create an x64 venv and install pybaseball

From the `MLB-BettingV2/` folder:

- Create venv:
  - `py -3.11-64 -m venv .venv_x64`
- Install deps:
  - `.venv_x64\Scripts\python.exe -m pip install -U pip`
  - `.venv_x64\Scripts\python.exe -m pip install pybaseball`

## 3) Populate the shared cache

This writes JSON into `MLB-BettingV2/data/cache/statcast/` which your ARM64 sim reads.

- For a slate date (probable starters):
  - `.venv_x64\Scripts\python.exe tools\statcast\fetch_pitcher_pitch_splits_x64.py --date 2025-09-20 --season 2025 --out-report data\daily\snapshots\2025-09-20\statcast_fetch_report.json`

- Or for specific pitchers:
  - `.venv_x64\Scripts\python.exe tools\statcast\fetch_pitcher_pitch_splits_x64.py --season 2025 --pitcher-ids 666214,672456`

## 3b) (Optional) Build Statcast-based umpire factors

The sim can apply a **home-plate umpire called-strike multiplier** via the local map:

- `MLB-BettingV2/data/umpire/umpire_factors.json`

This tool computes per-umpire called-strike rates from Statcast (called_strike vs ball)
over a date window, compares to league average, clamps the multiplier, and writes both
ID and name keys for robustness.

- Example (last 21 days ending 2025-09-20):
  - `.venv_x64\Scripts\python.exe tools\statcast\fetch_umpire_factors_x64.py --date 2025-09-20 --days-back 21 --min-pitches 1500 --out-report data\daily\snapshots\2025-09-20\umpire_statcast_report.json`

- Example (explicit window):
  - `.venv_x64\Scripts\python.exe tools\statcast\fetch_umpire_factors_x64.py --start-date 2025-08-20 --end-date 2025-09-20 --min-pitches 1500`

## 4) Run the normal ARM64 daily update

Back in your normal `.venv` (ARM64):

- `C:/Users/mostg/OneDrive/Coding/MLB-BettingV2/.venv/Scripts/python.exe tools/daily_update.py --date 2025-09-20 --season 2025 --sims 100 --statcast-starter-splits starter`

### One-command (optional)

If you have `.venv_x64/` set up, you can have the daily updater invoke the x64
prefetch automatically:

- `C:/Users/mostg/OneDrive/Coding/MLB-BettingV2/.venv/Scripts/python.exe tools/daily_update.py --date 2025-09-20 --season 2025 --sims 100 --statcast-starter-splits starter --statcast-x64-prefetch auto`

## Notes

- If the cache has no entry for a pitcher, the sim falls back gracefully (no Statcast multipliers).
- The feature snapshots include per-pitcher `pitch_type_whiff_mult` / `pitch_type_inplay_mult` when present.

- If the umpire factors map has no entry for the home-plate umpire, the sim uses `called_strike_mult = 1.0`.
