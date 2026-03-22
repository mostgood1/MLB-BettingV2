from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


def _stable_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _key(parts: Dict[str, Any]) -> str:
    raw = _stable_json(parts).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


@dataclass
class DiskCache:
    root_dir: Path
    default_ttl_seconds: int = 6 * 3600

    def __post_init__(self) -> None:
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def get(self, namespace: str, parts: Dict[str, Any], ttl_seconds: Optional[int] = None) -> Optional[Dict[str, Any]]:
        ttl = self.default_ttl_seconds if ttl_seconds is None else int(ttl_seconds)
        k = _key({"ns": namespace, **parts})
        path = self.root_dir / namespace / f"{k}.json"
        try:
            if not path.exists():
                return None
            st = path.stat()
            if ttl > 0 and (time.time() - st.st_mtime) > ttl:
                return None
            with open(path, "r", encoding="utf-8") as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    # Corrupted/partial cache file: treat as miss and remove so we don't
                    # repeatedly fail on the same path.
                    try:
                        path.unlink(missing_ok=True)
                    except Exception:
                        pass
                    return None
        except Exception:
            return None

    def set(self, namespace: str, parts: Dict[str, Any], value: Dict[str, Any]) -> None:
        k = _key({"ns": namespace, **parts})
        path = self.root_dir / namespace / f"{k}.json"
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".json.tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(value, f)
            os.replace(tmp, path)
        except Exception:
            # best-effort
            return
