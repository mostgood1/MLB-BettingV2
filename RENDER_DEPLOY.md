# Render Deploy

This repo is configured for a Render Python web service.

## Files

- `render.yaml`: Render service definition
- `requirements.txt`: runtime Python dependencies for the web app
- `wsgi.py`: Gunicorn entrypoint that exposes the Flask app

## Deploy

1. Push this repo to GitHub.
2. In Render, create a new Blueprint instance from the GitHub repo.
3. Render will read `render.yaml` and create the `mlb-betting-v2` web service.
4. After the first deploy, open the generated Render URL.

## Runtime

- Build command: `pip install -r requirements.txt`
- Start command: `gunicorn wsgi:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120`
- Python version: `3.11.9`

## Notes

- The Flask app now resolves template and static directories with `pathlib`, so it works on Linux hosts such as Render.
- The local runner in `tools/web/flask_frontend.py` also honors `HOST`, `PORT`, and `FLASK_DEBUG`.