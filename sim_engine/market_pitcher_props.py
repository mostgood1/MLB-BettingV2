from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from .env import load_dotenv_if_present


def normalize_pitcher_name(name: str) -> str:
    """Normalize pitcher name strings for cross-repo matching.

    Goals:
    - Handle accents/diacritics
    - Drop parenthetical team tags like "(nyy)"
    - Lowercase and collapse whitespace/punctuation
    """
    s = str(name or "").strip()
    if not s:
        return ""

    # Remove common parenthetical team suffixes e.g. "Name (ATL)"
    if "(" in s:
        s = s.split("(", 1)[0].strip()

    # Normalize unicode (including NBSP)
    s = s.replace("\u00a0", " ")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))

    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = " ".join(s.split())
    return s


def _date_to_file_token(date_str: str) -> str:
    return str(date_str).strip().replace("-", "_")


def _load_json_if_exists(path: Path) -> Optional[Dict[str, Any]]:
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _parse_american_odds(x: Any) -> Optional[int]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        try:
            v = int(float(x))
        except Exception:
            return None
        return v if v != 0 else None
    s = str(x).strip().upper()
    if not s:
        return None
    if s in {"EVEN", "EV"}:
        return 100
    s = s.replace("+", "")
    try:
        v = int(float(s))
    except Exception:
        return None
    return v if v != 0 else None


def american_implied_prob(odds: Any) -> Optional[float]:
    o = _parse_american_odds(odds)
    if o is None:
        return None
    if o > 0:
        return float(100.0 / (float(o) + 100.0))
    return float(float(-o) / (float(-o) + 100.0))


def no_vig_over_prob(over_odds: Any, under_odds: Any) -> Optional[float]:
    p_over = american_implied_prob(over_odds)
    p_under = american_implied_prob(under_odds)
    if p_over is None or p_under is None:
        return None
    denom = float(p_over + p_under)
    if denom <= 0:
        return None
    return float(p_over / denom)


def market_side_probabilities(over_odds: Any, under_odds: Any) -> Dict[str, Any]:
    p_over = american_implied_prob(over_odds)
    p_under = american_implied_prob(under_odds)
    if p_over is not None and p_under is not None:
        denom = float(p_over + p_under)
        if denom <= 0:
            return {}
        return {
            "over": float(p_over / denom),
            "under": float(p_under / denom),
            "mode": "no_vig_two_way",
        }

    out: Dict[str, Any] = {"mode": "single_side_implied"}
    if p_over is not None:
        out["over"] = float(p_over)
    if p_under is not None:
        out["under"] = float(p_under)
    return out if len(out) > 1 else {}


def load_pitcher_prop_lines(
    date_str: str,
    *,
    original_repo_root: Optional[Path] = None,
    prefer: str = "auto",
) -> Tuple[Dict[str, Dict[str, Dict[str, Any]]], Dict[str, Any]]:
    """Load pitcher strikeout/outs props lines from the original app repo.

    Returns:
      (lines_by_pitcher, meta)

    lines_by_pitcher maps normalized pitcher name -> market -> {line, over_odds, under_odds, src, stale?}
    """
    v2_root = Path(__file__).resolve().parents[1]
    # Ensure ODDS_API_KEY can be picked up from MLB-BettingV2/.env in tools that call this.
    load_dotenv_if_present(v2_root / ".env")
    orig = Path(original_repo_root) if original_repo_root else (v2_root.parent / "MLB-Betting")

    token = _date_to_file_token(date_str)

    candidates = []
    # 0) Prefer V2-local persisted market data (if present)
    v2_market = v2_root / "data" / "market" / "oddsapi"
    candidates.append(("v2_oddsapi", v2_market / f"oddsapi_pitcher_props_{token}.json"))
    if prefer in {"auto", "oddsapi"}:
        candidates.append(
            ("oddsapi", orig / "data" / "daily_oddsapi" / f"oddsapi_pitcher_props_{token}.json")
        )
    if prefer in {"auto", "last_known", "bovada"}:
        candidates.append(
            ("last_known", orig / "data" / "daily_bovada" / f"pitcher_last_known_lines_{token}.json")
        )
    if prefer in {"auto", "bovada"}:
        candidates.append(
            ("bovada", orig / "data" / "daily_bovada" / f"bovada_pitcher_props_{token}.json")
        )

    def build_entry(src: str, line: Any, over_odds: Any, under_odds: Any, stale: Optional[bool] = None) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "line": None,
            "over_odds": None,
            "under_odds": None,
            "src": str(src),
        }
        try:
            if line is not None:
                out["line"] = float(line)
        except Exception:
            out["line"] = None
        oo = _parse_american_odds(over_odds)
        uo = _parse_american_odds(under_odds)
        out["over_odds"] = oo
        out["under_odds"] = uo
        if stale is not None:
            out["stale"] = bool(stale)
        return out

    for src, path in candidates:
        data = _load_json_if_exists(path)
        if not data:
            continue

        lines: Dict[str, Dict[str, Dict[str, Any]]] = {}

        if src in {"oddsapi", "v2_oddsapi"}:
            pitcher_props = data.get("pitcher_props") or {}
            if not isinstance(pitcher_props, dict) or not pitcher_props:
                continue
            for raw_name, markets in pitcher_props.items():
                nk = normalize_pitcher_name(str(raw_name))
                if not nk or not isinstance(markets, dict):
                    continue
                for mkey in ("strikeouts", "outs"):
                    mk = markets.get(mkey)
                    if not isinstance(mk, dict):
                        continue
                    entry = build_entry(
                        "oddsapi" if src == "oddsapi" else "v2_oddsapi",
                        mk.get("line"),
                        mk.get("over_odds"),
                        mk.get("under_odds"),
                        stale=mk.get("_stale") if "_stale" in mk else None,
                    )
                    if entry.get("line") is None:
                        continue
                    lines.setdefault(nk, {})[mkey] = entry

        elif src == "last_known":
            pitchers = data.get("pitchers") or {}
            if not isinstance(pitchers, dict) or not pitchers:
                continue
            for raw_name, markets in pitchers.items():
                nk = normalize_pitcher_name(str(raw_name))
                if not nk or not isinstance(markets, dict):
                    continue
                for mkey in ("strikeouts", "outs"):
                    mk = markets.get(mkey)
                    if not isinstance(mk, dict):
                        continue
                    entry = build_entry("last_known", mk.get("line"), mk.get("over_odds"), mk.get("under_odds"))
                    if entry.get("line") is None:
                        continue
                    lines.setdefault(nk, {})[mkey] = entry

        elif src == "bovada":
            pitcher_props = data.get("pitcher_props") or {}
            if not isinstance(pitcher_props, dict) or not pitcher_props:
                continue
            for raw_name, markets in pitcher_props.items():
                # Keys look like "Name (TEAM)"
                nk = normalize_pitcher_name(str(raw_name))
                if not nk or not isinstance(markets, dict) or not markets:
                    continue
                for mkey in ("strikeouts", "outs"):
                    mk = markets.get(mkey)
                    if not isinstance(mk, dict):
                        continue
                    entry = build_entry(
                        "bovada",
                        mk.get("line"),
                        mk.get("over_odds"),
                        mk.get("under_odds"),
                        stale=mk.get("_stale") if "_stale" in mk else None,
                    )
                    if entry.get("line") is None:
                        continue
                    lines.setdefault(nk, {})[mkey] = entry

        if lines:
            return lines, {"source": src, "path": str(path), "pitchers": int(len(lines))}

    return {}, {"source": None, "path": None, "pitchers": 0}
