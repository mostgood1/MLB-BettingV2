# 2025 Retune + Recal (V2-only playbook)

This playbook assumes **all work is done in MLB-BettingV2** and uses:
- `tools/daily_update.py` for snapshot + sim generation
- `tools/eval/eval_sim_day_vs_actual.py` and `tools/eval/run_batch_eval_days.py` for evaluation
- `data/tuning/*` JSONs as the promoted tuning artifacts
- Roster artifacts under `data/daily/snapshots/<date>/roster_objs/` to make reruns fast and deterministic

## 0) One-time prerequisites

- Use the V2 interpreter: `.venv_x64/Scripts/python.exe`
- Confirm the lineup-cycling fix is already in place (done earlier in this session).

## 1) Lock down a 2025 training and validation set

Use existing date sets (already versioned):
- Training candidate: `data/eval/date_sets/random_feed_live_2025_regseason_50days_min10_seed2026.txt`
- Alternative coverage-biased: `data/eval/date_sets/prop_coverage_2025_50days.txt`

Rule of thumb:
- Use ONE set for tuning iterations.
- Hold out a second set (or a different seed) for final validation.

## 2) Warm caches + write roster artifacts (critical for speed)

Do a first pass over the tuning set with roster artifact writing ON.

Example (runs daily_update for each date in the set):

```powershell
C:/Users/mostg/OneDrive/Coding/MLB-BettingV2/.venv_x64/Scripts/python.exe tools/daily_update_batch.py \
  --date-set data/eval/date_sets/random_feed_live_2025_regseason_50days_min10_seed2026.txt \
  --sims 200 --workers 4 --pbp off \
  --use-roster-artifacts on --write-roster-artifacts on
```

Notes:
- This is the slowest pass (StatsAPI calls + cache warm + artifact creation).
- After this, reruns should mostly say `Loaded roster artifact: ...`.

## 3) Establish a baseline objective (frozen knobs)

Note: `tools/daily_update.py`, `tools/eval/eval_sim_day_vs_actual.py`, and `tools/eval/run_batch_eval_days.py` now default
`--manager-pitching-overrides` to `data/tuning/manager_pitching_overrides/default.json` (currently the bullpen-tax preset).
Pass `--manager-pitching-overrides ''` if you want a no-overrides baseline/ablation.

Pick and record the exact objective file (example used earlier):
- `data/tuning/objectives/all_metrics_v3_tuned_best20260210b_random50.json`

Create a baseline eval summary across the same dates with fixed seeds.

Example:

```powershell
C:/Users/mostg/OneDrive/Coding/MLB-BettingV2/.venv_x64/Scripts/python.exe tools/eval/run_batch_eval_days.py \
  --date-set data/eval/date_sets/random_feed_live_2025_regseason_50days_min10_seed2026.txt \
  --season 2025 --sims-per-game 50 --jobs 4 \
  --use-raw on --prop-lines-source off \
  --use-roster-artifacts on --write-roster-artifacts off
```

## 4) Tune in the right order (biggest signal first)

Suggested order (minimize confounding):

1) **Core pitch model overrides**
   - Run `tools/tune/search_pitch_model_overrides.py` against the tuning set.
   - Keep manager pitching, baserunning, and pitcher sampling fixed.

2) **Pitcher distribution overrides / sampling**
   - Adjust `pitcher_rate_sampling` and any `pitcher_distribution_overrides`.

3) **Manager pitching + overrides**
   - Tune `manager_pitching` version and `manager_pitching_overrides`.

4) **BIP baserunning toggles**
   - Only after the main run environment is stable.

## 5) Refit calibrations (after structural tuning)

Once the structural model is stable, refit the probability calibrations:
- `data/tuning/so_calibration/default.json`
- `data/tuning/outs_calibration/default.json`
- `data/tuning/hitter_hr_calibration/default.json`
- `data/tuning/hitter_props_calibration/default.json`

Then rerun batch eval and confirm improvements on both:
- training set
- holdout set

## 6) Promotion checklist (what “done” means)

- Re-run batch eval on holdout date set.
- Save the winning artifacts under `data/tuning/**`:
  - `pitch_model/current_best.json` (or equivalent)
  - calibrations under their `default.json`
  - any manager/pitching override files used
- Record the exact command lines used (copy/paste into a small run log).

## 7) Operationalizing (daily production)

Daily sim runs should use the same knobs as eval/tuning.
`tools/daily_update.py` now passes the parsed `cfg_kwargs` into:
- distribution sims (`_sim_many(..., cfg_kwargs=...)`)
- representative PBP sim (`GameConfig(..., **cfg_kwargs, pbp=...)`)

This ensures what we tune is what we ship.
