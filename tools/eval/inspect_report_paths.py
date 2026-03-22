from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Iterable, List


def _read_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))


def _iter_paths(obj: Any, *, max_depth: int, max_list: int) -> Iterable[str]:
    def walk(x: Any, path: str, depth: int) -> Iterable[str]:
        if depth > max_depth:
            return
        if isinstance(x, dict):
            for k, v in x.items():
                np = f"{path}.{k}" if path else str(k)
                yield np
                yield from walk(v, np, depth + 1)
        elif isinstance(x, list):
            for i, v in enumerate(x[:max_list]):
                np = f"{path}[{i}]"
                yield np
                yield from walk(v, np, depth + 1)

    yield from walk(obj, "", 0)


def main() -> int:
    ap = argparse.ArgumentParser(description="Inspect JSON report and print matching key paths")
    ap.add_argument("--report", required=True)
    ap.add_argument("--pattern", default="hitter|topn|hr_|props|likelihood")
    ap.add_argument("--max-depth", type=int, default=8)
    ap.add_argument("--max-list", type=int, default=25)
    args = ap.parse_args()

    report = Path(str(args.report)).resolve()
    if not report.exists():
        raise SystemExit(f"Missing report: {report}")

    obj = _read_json(report)
    pat = re.compile(str(args.pattern), re.I)

    matches: List[str] = []
    for p in _iter_paths(obj, max_depth=int(args.max_depth), max_list=int(args.max_list)):
        key = p.split(".")[-1]
        if pat.search(key):
            matches.append(p)

    matches = sorted(set(matches))
    print(f"report: {report}")
    print(f"top_keys: {sorted(obj.keys()) if isinstance(obj, dict) else type(obj).__name__}")
    print(f"matches: {len(matches)}")
    for m in matches[:250]:
        print(m)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
