"""
Weather Feed — fetch forecasts from Open-Meteo (multiple models).
Each model returns temperature, precipitation, wind, snowfall forecasts.
"""

import time
import random
import requests
import numpy as np
from datetime import datetime, timedelta
from config import OPEN_METEO_API, OPEN_METEO_HIST, WEATHER_MODELS, CITIES


def fetch_forecast(city_name: str, city_coords: dict, model_key: str) -> dict:
    """
    Fetch weather forecast for a city from a specific model via Open-Meteo.
    Returns dict with hourly forecast data for next 48h.
    """
    model_info = WEATHER_MODELS[model_key]
    lat, lon = city_coords["lat"], city_coords["lon"]

    try:
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m,precipitation,snowfall,wind_speed_10m,cape,visibility",
            "forecast_days": 3,
            "models": model_info["source"],
        }
        resp = requests.get(f"{OPEN_METEO_API}/forecast", params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            hourly = data.get("hourly", {})
            return {
                "model": model_key,
                "city": city_name,
                "latency_ms": model_info["latency_ms"] + random.randint(-20, 40),
                "accuracy": model_info["accuracy"],
                "timestamp": datetime.utcnow().isoformat(),
                "temperature_2m": hourly.get("temperature_2m", []),
                "temperature_2m_min": min(hourly.get("temperature_2m", [20])) if hourly.get("temperature_2m") else 20,
                "temperature_2m_max": max(hourly.get("temperature_2m", [20])) if hourly.get("temperature_2m") else 20,
                "precipitation": sum(hourly.get("precipitation", [0])),
                "snowfall": sum(hourly.get("snowfall", [0])),
                "wind_speed_10m_max": max(hourly.get("wind_speed_10m", [0])) if hourly.get("wind_speed_10m") else 0,
                "cape": max(hourly.get("cape", [0])) if hourly.get("cape") else 0,
                "visibility": min(hourly.get("visibility", [10000])) if hourly.get("visibility") else 10000,
                "status": "ok",
            }
    except Exception as e:
        pass

    # Fallback: simulated data
    return _simulate_forecast(city_name, model_key)


def _simulate_forecast(city_name: str, model_key: str) -> dict:
    """Generate simulated forecast when API is unavailable."""
    model_info = WEATHER_MODELS[model_key]
    base_temp = {"Chicago": 10, "New York": 12, "Miami": 28, "Los Angeles": 22,
                 "Houston": 25, "Denver": 8, "Seattle": 11, "Phoenix": 30}.get(city_name, 15)

    noise = random.gauss(0, 3)
    return {
        "model": model_key,
        "city": city_name,
        "latency_ms": model_info["latency_ms"] + random.randint(-20, 40),
        "accuracy": model_info["accuracy"],
        "timestamp": datetime.utcnow().isoformat(),
        "temperature_2m": [base_temp + noise + random.gauss(0, 1) for _ in range(72)],
        "temperature_2m_min": base_temp + noise - abs(random.gauss(0, 5)),
        "temperature_2m_max": base_temp + noise + abs(random.gauss(0, 8)),
        "precipitation": max(0, random.gauss(5, 10)),
        "snowfall": max(0, random.gauss(0, 3)) if base_temp < 5 else 0,
        "wind_speed_10m_max": max(0, random.gauss(20, 15)),
        "cape": max(0, random.gauss(500, 600)),
        "visibility": max(100, random.gauss(8000, 4000)),
        "status": "simulated",
    }


def fetch_all_forecasts() -> dict:
    """
    Fetch forecasts from all models for all cities.
    Returns {city: {model: forecast_data}}
    """
    results = {}
    for city_name, coords in CITIES.items():
        results[city_name] = {}
        for model_key in WEATHER_MODELS:
            forecast = fetch_forecast(city_name, coords, model_key)
            results[city_name][model_key] = forecast
    return results


def calc_model_consensus(forecasts: dict, city: str, metric: str) -> dict:
    """
    Calculate consensus across models for a metric in a city.
    Returns mean, std, min, max, and per-model values.
    """
    values = []
    model_vals = {}
    for model_key, data in forecasts.get(city, {}).items():
        val = data.get(metric)
        if val is not None and not (isinstance(val, float) and np.isnan(val)):
            values.append(float(val))
            model_vals[model_key] = float(val)

    if not values:
        return {"mean": 0, "std": 0, "min": 0, "max": 0, "models": {}, "n": 0}

    return {
        "mean": np.mean(values),
        "std": np.std(values),
        "min": np.min(values),
        "max": np.max(values),
        "models": model_vals,
        "n": len(values),
    }


def calc_weather_triggers(forecasts: dict) -> dict:
    """
    Calculate weather trigger values from forecast consensus.
    Returns dict of trigger_name: value.
    """
    triggers = {}

    # Temperature delta (spread between model forecasts)
    chi_temps = []
    for model, data in forecasts.get("Chicago", {}).items():
        chi_temps.append(data.get("temperature_2m_min", 10))
    if chi_temps:
        triggers["temp_delta"] = round(max(chi_temps) - min(chi_temps), 1)

    # Precipitation divergence
    precip_vals = []
    for model, data in forecasts.get("Houston", {}).items():
        precip_vals.append(data.get("precipitation", 0))
    if precip_vals:
        triggers["precip_divergence"] = f"{round(np.std(precip_vals), 0):.0f}mm"

    # Wind anomaly
    wind_vals = []
    for model, data in forecasts.get("Miami", {}).items():
        wind_vals.append(data.get("wind_speed_10m_max", 0))
    if wind_vals:
        triggers["wind_anomaly"] = round(np.mean(wind_vals), 1)

    # Pressure gradient (simulated from CAPE)
    cape_vals = []
    for model, data in forecasts.get("Denver", {}).items():
        cape_vals.append(data.get("cape", 0))
    if cape_vals:
        triggers["pressure_gradient"] = round(np.std(cape_vals), 1)

    # Forecast spread
    all_spreads = []
    for city in forecasts:
        temps = [d.get("temperature_2m_max", 20) for d in forecasts[city].values()]
        if temps:
            all_spreads.append(max(temps) - min(temps))
    triggers["forecast_spread"] = round(np.mean(all_spreads), 2) if all_spreads else 0

    return triggers


def fetch_historical_weather(city_coords: dict, start_date: str, end_date: str) -> dict:
    """Fetch historical weather data from Open-Meteo archive API."""
    try:
        params = {
            "latitude": city_coords["lat"],
            "longitude": city_coords["lon"],
            "start_date": start_date,
            "end_date": end_date,
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,snowfall_sum,wind_speed_10m_max",
        }
        resp = requests.get(OPEN_METEO_HIST, params=params, timeout=15)
        if resp.status_code == 200:
            return resp.json().get("daily", {})
    except:
        pass
    return {}
