"""
Data Logger — records real Polymarket odds + weather forecasts every cycle.
Saves to CSV for future backtesting with real historical odds.
"""

import os
import csv
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
ODDS_FILE = os.path.join(DATA_DIR, "polymarket_odds.csv")
FORECASTS_FILE = os.path.join(DATA_DIR, "weather_forecasts.csv")
TRADES_FILE = os.path.join(DATA_DIR, "dry_run_trades.csv")

ODDS_FIELDS = [
    "timestamp", "cycle", "market_id", "city", "target_date",
    "temp_value", "unit", "threshold_type", "yes_price", "no_price",
    "volume", "slug",
]

FORECAST_FIELDS = [
    "timestamp", "cycle", "city", "model",
    "temp_max", "temp_min", "precipitation", "wind_max",
]

TRADE_FIELDS = [
    "timestamp", "cycle", "contract", "side", "entry_price",
    "size", "edge", "confidence", "status", "exit_price", "pnl",
]


def _ensure_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def _append_csv(filepath: str, fields: list, rows: list):
    """Append rows to CSV, create with header if new."""
    _ensure_dir()
    file_exists = os.path.exists(filepath) and os.path.getsize(filepath) > 10
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


def log_odds(cycle: int, parsed_markets: list):
    """Log all current Polymarket odds."""
    ts = datetime.utcnow().isoformat()
    rows = []
    for m in parsed_markets:
        rows.append({
            "timestamp": ts,
            "cycle": cycle,
            "market_id": m.get("id", ""),
            "city": m.get("city", ""),
            "target_date": m.get("target_date", ""),
            "temp_value": m.get("temp_mid_original", ""),
            "unit": m.get("unit", ""),
            "threshold_type": m.get("threshold_type", ""),
            "yes_price": m.get("yes_price", ""),
            "no_price": m.get("no_price", ""),
            "volume": m.get("volume", ""),
            "slug": m.get("slug", ""),
        })
    if rows:
        _append_csv(ODDS_FILE, ODDS_FIELDS, rows)


def log_forecasts(cycle: int, forecasts: dict):
    """Log weather forecasts from all models for all cities."""
    ts = datetime.utcnow().isoformat()
    rows = []
    for city, models in forecasts.items():
        for model_key, data in models.items():
            rows.append({
                "timestamp": ts,
                "cycle": cycle,
                "city": city,
                "model": model_key,
                "temp_max": data.get("temperature_2m_max", ""),
                "temp_min": data.get("temperature_2m_min", ""),
                "precipitation": data.get("precipitation", ""),
                "wind_max": data.get("wind_speed_10m_max", ""),
            })
    if rows:
        _append_csv(FORECASTS_FILE, FORECAST_FIELDS, rows)


def log_trade(cycle: int, trade_data: dict):
    """Log a single trade event (open or close)."""
    ts = datetime.utcnow().isoformat()
    row = {
        "timestamp": ts,
        "cycle": cycle,
        "contract": trade_data.get("contract", ""),
        "side": trade_data.get("side", ""),
        "entry_price": trade_data.get("entry_price", ""),
        "size": trade_data.get("size", ""),
        "edge": trade_data.get("edge", ""),
        "confidence": trade_data.get("confidence", ""),
        "status": trade_data.get("status", "open"),
        "exit_price": trade_data.get("exit_price", ""),
        "pnl": trade_data.get("pnl", ""),
    }
    _append_csv(TRADES_FILE, TRADE_FIELDS, [row])


def get_logged_stats() -> dict:
    """Get stats about logged data."""
    stats = {}
    for name, path in [("odds", ODDS_FILE), ("forecasts", FORECASTS_FILE), ("trades", TRADES_FILE)]:
        if os.path.exists(path):
            with open(path, "r") as f:
                lines = sum(1 for _ in f) - 1  # minus header
            size_kb = os.path.getsize(path) / 1024
            stats[name] = {"records": max(0, lines), "size_kb": round(size_kb, 1)}
        else:
            stats[name] = {"records": 0, "size_kb": 0}
    return stats
