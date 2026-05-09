import json
import os
import sys
from datetime import datetime, timezone

import requests


def _env_text(name: str, default: str = "") -> str:
    return str(os.environ.get(name) or default).strip()


def _env_int(name: str, default: int) -> int:
    raw = _env_text(name)
    try:
        return int(raw or default)
    except Exception:
        return int(default)


def _base_url() -> str:
    explicit = _env_text("MLB_WEB_INTERNAL_BASE_URL")
    if explicit:
        return explicit.rstrip("/")
    host = _env_text("MLB_WEB_INTERNAL_HOST")
    port = _env_text("MLB_WEB_INTERNAL_PORT")
    if host and port:
        return f"http://{host}:{port}"
    if host:
        return f"http://{host}"
    raise RuntimeError("Missing MLB_WEB_INTERNAL_BASE_URL or MLB_WEB_INTERNAL_HOST")


def _request(session: requests.Session, method: str, url: str, *, token: str, timeout: int, params: dict | None = None) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    response = session.request(method=method, url=url, headers=headers, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, dict):
        return payload
    return {"ok": True, "raw": payload}


def main() -> int:
    token = _env_text("MLB_CRON_TOKEN")
    if not token:
        raise RuntimeError("Missing MLB_CRON_TOKEN")

    base_url = _base_url()
    timeout = max(30, _env_int("MLB_CRON_REQUEST_TIMEOUT_SECONDS", 180))
    market_refresh_interval = max(0, _env_int("MLB_LIVE_LENS_MARKET_REFRESH_INTERVAL_MINUTES", 15))
    now_utc = datetime.now(timezone.utc)
    should_refresh_markets = bool(market_refresh_interval and (now_utc.minute % market_refresh_interval == 0))

    session = requests.Session()
    results: dict[str, dict] = {}

    if should_refresh_markets:
        results["markets"] = _request(
            session,
            "GET",
            f"{base_url}/api/cron/refresh-oddsapi-markets",
            token=token,
            timeout=timeout,
            params={"republish": "off", "overwrite": "on"},
        )

    results["liveLensTick"] = _request(
        session,
        "GET",
        f"{base_url}/api/cron/live-lens-tick",
        token=token,
        timeout=timeout,
        params={"refreshMarkets": "off"},
    )

    results["warmCardsCache"] = _request(
        session,
        "GET",
        f"{base_url}/api/cron/warm-cards-cache",
        token=token,
        timeout=timeout,
    )

    print(json.dumps({
        "ok": True,
        "baseUrl": base_url,
        "marketRefreshIntervalMinutes": market_refresh_interval,
        "marketsTriggered": should_refresh_markets,
        "results": results,
    }, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, separators=(",", ":")))
        raise SystemExit(1)