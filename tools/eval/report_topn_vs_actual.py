from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _fmt(x: Any, nd: int = 3) -> str:
    if x is None:
        return ""
    if isinstance(x, float):
        return f"{x:.{nd}f}"
    return str(x)


def _game_label(g: Dict[str, Any]) -> str:
    away = (g.get("away") or {}) if isinstance(g.get("away"), dict) else {}
    home = (g.get("home") or {}) if isinstance(g.get("home"), dict) else {}
    a = str(away.get("abbr") or away.get("name") or "AWAY")
    h = str(home.get("abbr") or home.get("name") or "HOME")
    return f"{a} @ {h}"


def _md_table(headers: List[str], rows: List[List[str]]) -> str:
    lines: List[str] = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "|".join(["---"] * len(headers)) + "|")
    for r in rows:
        lines.append("| " + " | ".join(r) + " |")
    return "\n".join(lines)


def _collect_day_rows(report: Dict[str, Any], prop: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    games = report.get("games") or []
    if not isinstance(games, list):
        return out

    for g in games:
        if not isinstance(g, dict):
            continue
        glab = _game_label(g)
        game_pk = g.get("game_pk")

        if prop == "hr_1plus":
            hb = g.get("hitter_hr_backtest") or {}
            scored = hb.get("scored_overall") if isinstance(hb, dict) else None
            if not isinstance(scored, list):
                continue
            for r in scored:
                if not isinstance(r, dict):
                    continue
                out.append(
                    {
                        "game": glab,
                        "game_pk": game_pk,
                        "batter_id": r.get("batter_id"),
                        "name": r.get("name"),
                        "p": r.get("p_hr_1plus"),
                        "p_cal": r.get("p_hr_1plus_cal"),
                        "actual": r.get("actual_hr"),
                        "y": r.get("y_hr_1plus"),
                    }
                )
            continue

        hp = g.get("hitter_props_backtest") or {}
        if not isinstance(hp, dict):
            continue
        block = hp.get(prop) or {}
        if not isinstance(block, dict):
            continue
        scored = block.get("scored")
        if not isinstance(scored, list):
            continue
        for r in scored:
            if not isinstance(r, dict):
                continue
            out.append(
                {
                    "game": glab,
                    "game_pk": game_pk,
                    "batter_id": r.get("batter_id"),
                    "name": r.get("name"),
                    "p": r.get("p"),
                    "p_cal": r.get("p_cal"),
                    "actual": r.get("actual"),
                    "y": r.get("y"),
                }
            )

    return out


def _sort_key(r: Dict[str, Any]) -> Tuple[float, float, str]:
    try:
        p_cal = float(r.get("p_cal") or 0.0)
    except Exception:
        p_cal = 0.0
    try:
        p = float(r.get("p") or 0.0)
    except Exception:
        p = 0.0
    name = str(r.get("name") or "")
    return (p_cal, p, name)


def _prop_title(prop: str) -> str:
    mapping = {
        "hr_1plus": "HR 1+",
        "hits_1plus": "Hits 1+",
        "hits_2plus": "Hits 2+",
        "doubles_1plus": "Doubles 1+",
        "triples_1plus": "Triples 1+",
        "runs_1plus": "Runs 1+",
        "rbi_1plus": "RBI 1+",
        "sb_1plus": "SB 1+",
    }
    return mapping.get(prop, prop)


def _metric_block(report: Dict[str, Any]) -> str:
    full = (((report.get("assessment") or {}).get("full_game") or {}))
    lines: List[str] = []

    hr = full.get("hitter_hr_likelihood_topn") or {}
    if isinstance(hr, dict) and hr:
        lines.append(f"- HR top-N: n={hr.get('n')}, brier={_fmt(hr.get('brier'))}, logloss={_fmt(hr.get('logloss'))}, avg_p={_fmt(hr.get('avg_p'))}, emp_rate={_fmt(hr.get('emp_rate'))}")

    hp = full.get("hitter_props_likelihood_topn") or {}
    if isinstance(hp, dict) and hp:
        for k in (
            "hits_1plus",
            "hits_2plus",
            "doubles_1plus",
            "triples_1plus",
            "runs_1plus",
            "rbi_1plus",
            "sb_1plus",
        ):
            blk = hp.get(k) or {}
            if not isinstance(blk, dict) or not blk:
                continue
            lines.append(
                f"- {k}: n={blk.get('n')}, brier={_fmt(blk.get('brier'))}, logloss={_fmt(blk.get('logloss'))}, avg_p={_fmt(blk.get('avg_p'))}, emp_rate={_fmt(blk.get('emp_rate'))}"
            )

    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate a top-N vs actual Markdown report from a sim_vs_actual day JSON.")
    ap.add_argument("--in", dest="inp", required=True, help="Path to sim_vs_actual_*.json")
    ap.add_argument("--out", default="", help="Output .md path (default: <in>.md)")
    ap.add_argument("--topn", type=int, default=0, help="Top-N rows per prop across the day (default: use report meta hitter_hr_top_n)")
    args = ap.parse_args()

    in_path = Path(args.inp)
    report = _read_json(in_path)
    if not isinstance(report, dict):
        raise ValueError("Input JSON must be an object")

    meta = report.get("meta") or {}
    date = str(meta.get("date") or "")
    sims = meta.get("sims_per_game")
    default_topn = int(meta.get("hitter_hr_top_n") or 0)
    topn = int(args.topn) if int(args.topn or 0) > 0 else int(default_topn)

    props = [
        "hr_1plus",
        "hits_1plus",
        "hits_2plus",
        "doubles_1plus",
        "triples_1plus",
        "runs_1plus",
        "rbi_1plus",
        "sb_1plus",
    ]

    lines: List[str] = []
    lines.append(f"# Top-{topn} Hitter Props vs Actual — {date}")
    lines.append("")
    lines.append(f"- sims_per_game: {sims}")
    lines.append(f"- source: {in_path.as_posix()}")
    lines.append("")
    lines.append("## Day-Level Scoring (top-N backtests)")
    mb = _metric_block(report)
    if mb:
        lines.append(mb)
    else:
        lines.append("(No hitter top-N metrics found in this report.)")

    for prop in props:
        rows = _collect_day_rows(report, prop)
        rows.sort(key=_sort_key, reverse=True)
        rows = rows[: max(0, int(topn))] if topn > 0 else rows

        lines.append("")
        lines.append(f"## {_prop_title(prop)} (Top-{topn} across day by p_cal)")
        lines.append("")

        table_rows: List[List[str]] = []
        for i, r in enumerate(rows, start=1):
            table_rows.append(
                [
                    str(i),
                    str(r.get("name") or ""),
                    str(r.get("game") or ""),
                    _fmt(r.get("p_cal")),
                    _fmt(r.get("p")),
                    str(r.get("actual") if r.get("actual") is not None else ""),
                    str(r.get("y") if r.get("y") is not None else ""),
                ]
            )

        if table_rows:
            lines.append(_md_table(["#", "player", "game", "p_cal", "p_raw", "actual", "y"], table_rows))
        else:
            lines.append("(No rows found.)")

    out_path = Path(args.out) if str(args.out).strip() else in_path.with_suffix(in_path.suffix + ".md")
    _write_text(out_path, "\n".join(lines) + "\n")
    print(f"Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
