from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional


def _read_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))


def _short_item(d: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
    return {k: d.get(k) for k in keys if k in d}


def main() -> int:
    ap = argparse.ArgumentParser(description="Inspect hitter top-N payload schema in sim_vs_actual report")
    ap.add_argument("--report", required=True)
    args = ap.parse_args()

    report = Path(str(args.report)).resolve()
    obj = _read_json(report)

    assessment = obj.get("assessment") if isinstance(obj, dict) else None
    full = assessment.get("full_game") if isinstance(assessment, dict) else None

    print(f"report: {report}")
    print("top_keys:", sorted(obj.keys()) if isinstance(obj, dict) else type(obj).__name__)
    print("assessment keys:", sorted(assessment.keys()) if isinstance(assessment, dict) else None)
    print("full_game keys:", sorted(full.keys()) if isinstance(full, dict) else None)

    hr = full.get("hitter_hr_likelihood_topn") if isinstance(full, dict) else None
    hp = full.get("hitter_props_likelihood_topn") if isinstance(full, dict) else None

    games = obj.get("games") if isinstance(obj, dict) else None
    g0 = games[0] if isinstance(games, list) and games else None

    print("\nHR topn:")
    print(" type:", type(hr).__name__)
    if isinstance(hr, dict):
        print(" keys:", sorted(hr.keys()))
        if "top_n" in hr:
            tn = hr.get("top_n")
            print(" top_n type:", type(tn).__name__)
            if isinstance(tn, list):
                print(" top_n len:", len(tn))
                if tn and isinstance(tn[0], dict):
                    print(" top_n[0] keys:", sorted(tn[0].keys()))
                    print(
                        " top_n[0] sample:",
                        _short_item(tn[0], ["player_id", "player_name", "team", "opp", "game_id", "p", "p_cal", "y", "w"]),
                    )
            elif isinstance(tn, dict):
                print(" top_n keys:", sorted(tn.keys())[:40])
        for k in sorted(hr.keys()):
            v = hr.get(k)
            if isinstance(v, list) and v:
                item = v[0]
                print(f" list '{k}' len:", len(v))
                print("  item0 keys:", sorted(item.keys()) if isinstance(item, dict) else None)
                if isinstance(item, dict):
                    print(
                        "  item0 sample:",
                        _short_item(item, ["player_id", "player_name", "team", "opp", "game_id", "p", "p_cal", "y", "w"]),
                    )
                break
    elif isinstance(hr, list):
        print(" len:", len(hr))
        if hr:
            item = hr[0]
            print(" item0 keys:", sorted(item.keys()) if isinstance(item, dict) else None)
            if isinstance(item, dict):
                print(" item0 sample:", _short_item(item, ["player_id", "player_name", "team", "opp", "game_id", "p", "p_cal", "y", "w"]))

    print("\nProps topn:")
    print(" type:", type(hp).__name__)
    if isinstance(hp, dict):
        props = sorted(hp.keys())
        print(" props:", props)
        if "top_n" in hp:
            print(" top_n type:", type(hp.get('top_n')).__name__)
        for prop in props:
            if prop == "top_n":
                continue
            v = hp.get(prop)
            if not isinstance(v, dict) or not v:
                continue
            print(f" prop {prop}:")
            print("  keys:", sorted(v.keys()))
            tn = v.get("top_n")
            print("  top_n type:", type(tn).__name__)
            if isinstance(tn, list):
                print("  top_n len:", len(tn))
                if tn and isinstance(tn[0], dict):
                    print("  top_n[0] keys:", sorted(tn[0].keys()))
                    print(
                        "  top_n[0] sample:",
                        _short_item(tn[0], ["player_id", "player_name", "p", "p_cal", "y", "w"]),
                    )
            elif isinstance(tn, dict):
                print("  top_n keys:", sorted(tn.keys())[:40])
            break

    print("\nGame[0] backtests:")
    if isinstance(g0, dict):
        print(" game0 keys:", sorted(g0.keys()))
        hr_bt = g0.get("hitter_hr_backtest")
        hp_bt = g0.get("hitter_props_backtest")
        print(" hitter_hr_backtest type:", type(hr_bt).__name__)
        if isinstance(hr_bt, dict):
            print("  keys:", sorted(hr_bt.keys()))
            so = hr_bt.get("scored_overall")
            print("  scored_overall type:", type(so).__name__)
            if isinstance(so, list):
                print("  scored_overall len:", len(so))
                if so and isinstance(so[0], dict):
                    print("  scored_overall[0] keys:", sorted(so[0].keys()))
                    print(
                        "  scored_overall[0] sample:",
                        _short_item(so[0], ["player_id", "player_name", "p_hr_1plus", "p_hr_1plus_cal", "y_hr_1plus", "w"]),
                    )
        print(" hitter_props_backtest type:", type(hp_bt).__name__)
        if isinstance(hp_bt, dict):
            print("  keys:", sorted(hp_bt.keys()))
            # Try to find the first prop payload and print its schema.
            for prop in sorted(hp_bt.keys()):
                pv = hp_bt.get(prop)
                print(f"  prop '{prop}' type:", type(pv).__name__)
                if isinstance(pv, dict):
                    print("   keys:", sorted(pv.keys()))
                    scored = pv.get("scored")
                    print("   scored type:", type(scored).__name__)
                    if isinstance(scored, list):
                        print("   scored len:", len(scored))
                        if scored and isinstance(scored[0], dict):
                            item0 = scored[0]
                            print("   scored[0] keys:", sorted(item0.keys()))
                            # Print fields that look like p / p_cal / y for quick discovery.
                            sample_fields = {}
                            for k, v in item0.items():
                                if any(tok in k for tok in ("p_", "_cal", "y_", "actual_")) and isinstance(v, (int, float)):
                                    sample_fields[k] = v
                            if sample_fields:
                                print("   scored[0] sample fields:", sample_fields)
                            else:
                                print(
                                    "   scored[0] sample:",
                                    _short_item(item0, ["batter_id", "name", "p", "p_cal", "y", "w"]),
                                )
                            break
                elif isinstance(pv, list) and pv and isinstance(pv[0], dict):
                    print("   list len:", len(pv))
                    print("   item0 keys:", sorted(pv[0].keys()))
                    break
    else:
        print(" no games found")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
