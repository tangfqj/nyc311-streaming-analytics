"""Download observed-history reanalysis, never future forecast features."""
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import time

import pandas as pd
import requests

from nyc311.config import log_event, write_json

API_URL = "https://archive-api.open-meteo.com/v1/archive"
DOCUMENTATION_URL = "https://open-meteo.com/en/docs/historical-weather-api"
DAILY_FIELDS = ("temperature_2m_min", "temperature_2m_max", "precipitation_sum")


def download_weather(output_dir, start_date, end_date, config=None):
    """Download an inclusive date range; failures persist a manifest and return Failed.

    ERA5 is a gridded reanalysis incorporating observations, not a local station
    measurement. One Manhattan representative point approximates citywide weather.
    """
    started = time.monotonic()
    cfg = (config or {}).get("weather", config or {})
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    params = {
        "latitude": 40.7128, "longitude": -74.0060,
        "start_date": str(start_date), "end_date": str(end_date),
        "daily": ",".join(DAILY_FIELDS), "timezone": "America/New_York",
        "temperature_unit": "celsius", "precipitation_unit": "mm", "models": "era5",
    }
    manifest = {
        "source": API_URL, "documentation": DOCUMENTATION_URL,
        "source_kind": "official_open_meteo_historical_reanalysis",
        "weather_semantics": "Historical ERA5 gridded reanalysis, not a direct station observation or forecast",
        "spatial_assumption": "One NYC representative point; not borough-specific weather",
        "parameters": params, "downloaded_at": datetime.now(timezone.utc).isoformat(),
        "requested_end_inclusive": True, "status": "Failed", "attempts": [],
    }
    try:
        expected = pd.date_range(start_date, end_date, freq="D")
        if not len(expected):
            raise ValueError("end_date must be on or after start_date")
        response = None
        retries = int(cfg.get("retries", 3))
        for attempt in range(retries + 1):
            try:
                response = requests.get(API_URL, params=params, timeout=float(cfg.get("timeout_seconds", 30)))
                manifest["attempts"].append({"attempt": attempt + 1, "http_status": response.status_code})
                if response.status_code == 429 or response.status_code >= 500:
                    response.raise_for_status()
                response.raise_for_status()
                break
            except requests.RequestException as exc:
                if response is None:
                    manifest["attempts"].append({"attempt": attempt + 1, "error": str(exc)})
                if attempt >= retries or (response is not None and response.status_code < 500 and response.status_code != 429):
                    raise
                time.sleep(min(float(cfg.get("retry_base_seconds", 1)) * (2 ** attempt), 16))
                response = None
        payload = response.json()
        daily = payload.get("daily", {})
        if not all(field in daily for field in ("time", *DAILY_FIELDS)):
            raise ValueError("Weather API response missing required daily fields")
        frame = pd.DataFrame({"date": daily["time"], **{k: daily[k] for k in DAILY_FIELDS}})
        frame["date"] = pd.to_datetime(frame["date"], errors="raise").dt.strftime("%Y-%m-%d")
        if frame["date"].duplicated().any():
            raise ValueError("Weather response contains duplicate dates")
        requested_dates = set(expected.strftime("%Y-%m-%d"))
        if set(frame["date"]) != requested_dates:
            raise ValueError("Weather response does not cover the requested calendar exactly")
        for name in DAILY_FIELDS:
            frame[name] = pd.to_numeric(frame[name], errors="coerce")
        frame["weather_complete"] = frame[list(DAILY_FIELDS)].notna().all(axis=1)
        frame = frame.sort_values("date")
        raw_path = output / "weather_response.json"
        write_json(raw_path, payload)
        csv_path = output / "weather.csv"
        frame.to_csv(csv_path, index=False)
        manifest.update({
            "status": "Success", "rows": len(frame),
            "complete_weather_days": int(frame["weather_complete"].sum()),
            "missing_value_days": int((~frame["weather_complete"]).sum()),
            "file": str(csv_path), "raw_file": str(raw_path),
            "sha256": hashlib.sha256(csv_path.read_bytes()).hexdigest(),
            "daily_units": payload.get("daily_units", {}),
            "returned_latitude": payload.get("latitude"), "returned_longitude": payload.get("longitude"),
        })
    except (requests.RequestException, ValueError, TypeError, KeyError) as exc:
        manifest.update({"status": "Failed", "error_type": type(exc).__name__, "reason": str(exc),
                         "manual_step": "Retry make fetch-weather with network access; no synthetic weather is substituted."})
    manifest["elapsed_seconds"] = time.monotonic() - started
    write_json(output / "manifest.json", manifest)
    log_event("weather_download", output_dir=str(output_dir), **{k: manifest[k] for k in ("status", "elapsed_seconds")}, rows=manifest.get("rows", 0), reason=manifest.get("reason"))
    return manifest
