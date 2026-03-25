# Render Deploy

This repo is configured for a Render Python web service with a persistent disk for live market snapshots and live-lens logs.

## Files

- `render.yaml`: Render service definition, persistent disk mount, and runtime env vars
- `requirements.txt`: runtime Python dependencies for the web app
- `wsgi.py`: Gunicorn entrypoint that exposes the Flask app
- `.github/workflows/*`: scheduled GitHub Actions that call Render cron endpoints
- `scripts/render_cron_call.sh`: helper for token-gated `/api/cron/*` calls

## Deploy

1. Push this repo to GitHub.
2. In Render, create a new Blueprint instance from the GitHub repo.
3. Render will read `render.yaml` and create the `mlb-betting-v2` web service with a disk mounted at `/opt/render/project/data`.
4. In Render, set these secrets before using the cron endpoints or scheduled workflows:
	- `MLB_CRON_TOKEN`
	- `ODDS_API_KEY`
5. After the first deploy, open the generated Render URL.

## Runtime

- Build command: `pip install -r requirements.txt`
- Start command: `gunicorn wsgi:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120`
- Python version: `3.11.9`
- Data root: `/opt/render/project/data`
- Live lens dir: `/opt/render/project/data/live_lens`

## Cron Endpoints

- `GET /api/cron/ping`
- `GET /api/cron/config`
- `GET /api/cron/refresh-oddsapi-markets?date=YYYY-MM-DD`
- `GET /api/cron/live-lens-tick?date=YYYY-MM-DD`
- `GET /api/cron/live-lens-reports?date=YYYY-MM-DD`

All cron endpoints accept either:

- `Authorization: Bearer $MLB_CRON_TOKEN`
- `X-Cron-Token: $MLB_CRON_TOKEN`
- `?token=$MLB_CRON_TOKEN`

## GitHub Actions

The included workflows are intended to run from GitHub Actions and hit the Render cron endpoints. Configure these repository secrets:

- `MLB_BETTING_BASE_URL` or `RENDER_URL`
- `MLB_BETTING_CRON_TOKEN` or `CRON_TOKEN`

## Notes

- The Flask app now resolves template and static directories with `pathlib`, so it works on Linux hosts such as Render.
- The web app now prefers `MLB_BETTING_DATA_ROOT` for mutable live data and falls back to tracked repo data for historical files.
- The local runner in `tools/web/flask_frontend.py` also honors `HOST`, `PORT`, and `FLASK_DEBUG`.