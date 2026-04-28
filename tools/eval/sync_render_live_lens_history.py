from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import sys
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List


_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2) + "\n", encoding="utf-8")


def _env_first(*names: str) -> str:
    for name in names:
        value = str(os.environ.get(str(name)) or "").strip()
        if value:
            return value
    return ""


def _infer_render_base_url() -> str:
    explicit = _env_first("MLB_BETTING_BASE_URL", "BASE_URL", "RENDER_URL", "RENDER_EXTERNAL_URL")
    if explicit:
        return explicit
    render_yaml = _ROOT / "render.yaml"
    try:
        text = render_yaml.read_text(encoding="utf-8")
    except Exception:
        return ""
    for raw_line in text.splitlines():
        line = str(raw_line or "").strip()
        if not line.startswith("name:"):
            continue
        service_name = str(line.split(":", 1)[1] or "").strip()
        if service_name:
            return f"https://{service_name}.onrender.com"
    return ""


def _daterange(start: dt.date, end: dt.date) -> List[dt.date]:
    days: List[dt.date] = []
    current = start
    while current <= end:
        days.append(current)
        current += dt.timedelta(days=1)
    return days


def _fetch_payload(*, base_url: str, token: str, date_str: str, timeout_seconds: int) -> Dict[str, Any]:
    query = urllib.parse.urlencode({"date": str(date_str), "includeArchive": "on"})
    url = f"{str(base_url).rstrip('/')}/api/cron/live-lens-reports?{query}"
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(request, timeout=max(1, int(timeout_seconds))) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("live-lens reports response was not a JSON object")
    return payload


def sync_render_live_lens_history(
    *,
    base_url: str,
    cron_token: str,
    start_date: dt.date,
    end_date: dt.date,
    timeout_seconds: int = 45,
    out_dir: Path,
    overwrite: bool = False,
    require_archive: bool = False,
) -> Dict[str, Any]:
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")

    results: List[Dict[str, Any]] = []
    archive_ok_count = 0
    out_dir.mkdir(parents=True, exist_ok=True)
    for day in _daterange(start_date, end_date):
        date_str = day.isoformat()
        slug = date_str.replace("-", "_")
        out_path = out_dir / f"live_lens_reports_{slug}.json"
        if out_path.exists() and not bool(overwrite):
            existing_payload = _read_json(out_path) if out_path.exists() else {}
            archive_rows = len(list((existing_payload or {}).get("firstObservationArchive") or [])) if isinstance(existing_payload, dict) else 0
            if archive_rows > 0:
                archive_ok_count += 1
            results.append({
                "date": date_str,
                "status": "skipped",
                "path": str(out_path.relative_to(_ROOT)).replace('\\', '/'),
                "archiveRows": archive_rows,
            })
            continue
        try:
            payload = _fetch_payload(
                base_url=base_url,
                token=cron_token,
                date_str=date_str,
                timeout_seconds=int(timeout_seconds),
            )
            _write_json(out_path, payload)
            archive_rows = len(list(payload.get("firstObservationArchive") or [])) if isinstance(payload.get("firstObservationArchive"), list) else 0
            if archive_rows > 0:
                archive_ok_count += 1
            results.append({
                "date": date_str,
                "status": "ok",
                "path": str(out_path.relative_to(_ROOT)).replace('\\', '/'),
                "source": str(payload.get("source") or ""),
                "reportPath": payload.get("reportPath"),
                "entries": payload.get("entries"),
                "archiveRows": archive_rows,
            })
        except urllib.error.HTTPError as exc:
            results.append({"date": date_str, "status": "http_error", "code": int(exc.code), "reason": str(exc.reason)})
        except Exception as exc:
            results.append({"date": date_str, "status": "error", "error": f"{type(exc).__name__}: {exc}"})

    summary = {
        "ok": True,
        "baseUrl": base_url,
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "results": results,
        "okCount": sum(1 for row in results if str(row.get("status")) == "ok"),
        "skippedCount": sum(1 for row in results if str(row.get("status")) == "skipped"),
        "errorCount": sum(1 for row in results if str(row.get("status")) not in {"ok", "skipped"}),
        "archiveOkCount": int(archive_ok_count),
    }
    if bool(require_archive) and int(archive_ok_count) <= 0:
        raise RuntimeError(
            f"render live-lens sync completed but no archived observation rows were found between {start_date.isoformat()} and {end_date.isoformat()}"
        )
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync historical Render live-lens day payloads into data/live_lens/render_sync")
    parser.add_argument("--base-url", default="", help="Render base URL; defaults to env or render.yaml service host")
    parser.add_argument("--cron-token", default="", help="Cron bearer token; defaults to MLB_BETTING_CRON_TOKEN/MLB_CRON_TOKEN/CRON_TOKEN env")
    parser.add_argument("--start-date", required=True, help="Start date inclusive (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date inclusive (YYYY-MM-DD)")
    parser.add_argument("--timeout-seconds", type=int, default=45)
    parser.add_argument("--out-dir", default="data/live_lens/render_sync", help="Output directory for synced day payloads")
    parser.add_argument("--overwrite", choices=("on", "off"), default="off")
    parser.add_argument("--require-archive", choices=("on", "off"), default="off", help="Fail if the synced range contains no archived first-observation rows.")
    args = parser.parse_args()

    start_date = dt.date.fromisoformat(str(args.start_date))
    end_date = dt.date.fromisoformat(str(args.end_date))
    if end_date < start_date:
        raise SystemExit("end-date must be on or after start-date")

    base_url = str(args.base_url or "").strip() or _infer_render_base_url()
    cron_token = str(args.cron_token or "").strip() or _env_first("MLB_BETTING_CRON_TOKEN", "MLB_CRON_TOKEN", "CRON_TOKEN")
    if not base_url:
        raise SystemExit("Missing base URL. Pass --base-url or set MLB_BETTING_BASE_URL/RENDER_URL.")
    if not cron_token:
        raise SystemExit("Missing cron token. Pass --cron-token or set MLB_BETTING_CRON_TOKEN/MLB_CRON_TOKEN/CRON_TOKEN.")

    out_dir = Path(str(args.out_dir))
    if not out_dir.is_absolute():
        out_dir = (_ROOT / out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    summary = sync_render_live_lens_history(
        base_url=base_url,
        cron_token=cron_token,
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=int(args.timeout_seconds),
        out_dir=out_dir,
        overwrite=str(args.overwrite) == "on",
        require_archive=str(args.require_archive or "off") == "on",
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
