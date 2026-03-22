from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional


def _read_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))


def _load_jsonish(val: str) -> Optional[Dict[str, Any]]:
    s = str(val or "").strip()
    if not s:
        return None
    try:
        if s.startswith("{"):
            obj = json.loads(s)
        else:
            obj = json.loads(Path(s).read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _prob_over_line_from_dist(dist: Dict[str, Any], line: float) -> Optional[float]:
    if not isinstance(dist, dict) or not dist:
        return None
    try:
        ln = float(line)
    except Exception:
        return None

    n = 0.0
    over = 0.0
    for k, v in dist.items():
        try:
            kk = int(k)
            vv = float(v)
        except Exception:
            continue
        if vv <= 0:
            continue
        n += vv
        if float(kk) > ln:
            over += vv

    if n <= 0:
        return None
    return float(max(0.0, min(1.0, over / n)))


def _logloss(p: float, y: int, eps: float = 1e-12) -> float:
    pp = float(min(1.0 - eps, max(eps, float(p))))
    yy = 1.0 if int(y) == 1 else 0.0
    return float(-(yy * math.log(pp) + (1.0 - yy) * math.log(1.0 - pp)))


def _brier(p: float, y: int) -> float:
    return float((float(p) - float(y)) ** 2)


def _apply_outs_prob_calibration(p: float, cfg: Optional[Dict[str, Any]]) -> float:
    if not isinstance(cfg, dict) or not cfg:
        return float(p)
    if str(cfg.get("enabled", "true")).lower() in ("0", "false", "off", "no"):
        return float(p)

    mode = str(cfg.get("mode") or "affine_logit").strip().lower()
    if mode in ("shrink_0p5", "shrink_to_half", "shrink"):
        try:
            alpha = float(cfg.get("alpha", 0.0))
        except Exception:
            alpha = 0.0
        alpha = float(max(0.0, min(1.0, alpha)))
        return float((1.0 - alpha) * float(p) + alpha * 0.5)

    # NOTE: We intentionally only implement the modes needed for outs rescore.
    # If the config is unsupported, return p unchanged.
    return float(p)


def _no_vig_over_prob_from_american(over_odds: Any, under_odds: Any) -> Optional[float]:
    def imp(od: Any) -> Optional[float]:
        try:
            o = float(od)
        except Exception:
            return None
        if o > 0:
            return 100.0 / (o + 100.0)
        if o < 0:
            return (-o) / ((-o) + 100.0)
        return None

    po = imp(over_odds)
    pu = imp(under_odds)
    if po is None or pu is None:
        return None
    if (po + pu) <= 0:
        return None
    return float(po / (po + pu))


def main() -> int:
    ap = argparse.ArgumentParser(description="Rescore OUTS market-line metrics for an existing batch, without re-simulating")
    ap.add_argument("--batch-dir", required=True)
    ap.add_argument("--outs-prob-calibration", default="", help="JSON dict or path to JSON file")
    ap.add_argument("--out", default="", help="Output JSON (default: <batch_dir>/outs_market_rescore.json)")
    args = ap.parse_args()

    batch_dir = Path(str(args.batch_dir)).resolve()
    if not batch_dir.exists() or not batch_dir.is_dir():
        print(f"Invalid batch dir: {batch_dir}")
        return 2

    cfg = _load_jsonish(str(args.outs_prob_calibration))

    briers: List[float] = []
    lls: List[float] = []
    accs: List[float] = []
    edges: List[float] = []

    reports = sorted(batch_dir.glob("sim_vs_actual_*.json"))
    for rp in reports:
        try:
            report = _read_json(rp)
        except Exception:
            continue
        games = report.get("games") or []
        if not isinstance(games, list):
            continue

        for g in games:
            props = (g.get("pitcher_props") or {}) if isinstance(g, dict) else {}
            for side in ("away", "home"):
                pp = (props.get(side) or {}) if isinstance(props, dict) else {}
                actp = pp.get("actual") or {}
                pred = pp.get("pred") or {}
                market = pp.get("market") or {}
                if not isinstance(actp, dict) or not isinstance(pred, dict) or not isinstance(market, dict):
                    continue

                mk_outs = market.get("outs") or {}
                if not isinstance(mk_outs, dict):
                    continue
                line = mk_outs.get("line")
                if line is None:
                    continue

                try:
                    a_outs = int(actp.get("outs"))
                    ln = float(line)
                except Exception:
                    continue

                outs_dist = pred.get("outs_dist") or {}
                p_over = _prob_over_line_from_dist(outs_dist, ln)
                if p_over is None:
                    continue
                p_over = _apply_outs_prob_calibration(float(p_over), cfg)

                y_over = 1 if float(a_outs) > float(ln) else 0

                briers.append(_brier(p_over, y_over))
                lls.append(_logloss(p_over, y_over))
                accs.append(1.0 if ((p_over >= 0.5) == (y_over == 1)) else 0.0)

                try:
                    over_odds = mk_outs.get("over_odds")
                    under_odds = mk_outs.get("under_odds")
                    p_imp = _no_vig_over_prob_from_american(over_odds, under_odds)
                    if p_imp is not None:
                        edges.append(float(p_over) - float(p_imp))
                except Exception:
                    pass

    out_obj = {
        "batch_dir": str(batch_dir),
        "outs_prob_calibration": (cfg or {}),
        "n": int(len(briers)),
        "outs_brier": (sum(briers) / len(briers)) if briers else None,
        "outs_logloss": (sum(lls) / len(lls)) if lls else None,
        "outs_accuracy": (sum(accs) / len(accs)) if accs else None,
        "outs_avg_edge_vs_no_vig": (sum(edges) / len(edges)) if edges else None,
    }

    out_path = Path(str(args.out)).resolve() if str(args.out).strip() else (batch_dir / "outs_market_rescore.json")
    out_path.write_text(json.dumps(out_obj, indent=2), encoding="utf-8")

    print(json.dumps(out_obj, indent=2))
    print(f"Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
