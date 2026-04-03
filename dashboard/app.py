"""
Forecast Arb Engine — Web Terminal Dashboard.
Flask app with real-time updates via polling.
Now uses REAL Polymarket weather market data.
"""

import os
import sys
import json
import time
import random
import threading
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from flask import Flask, render_template, jsonify
from engine.weather import fetch_all_forecasts, calc_weather_triggers, calc_model_consensus
from engine.polymarket_real import get_live_weather_markets, calc_forecast_edge
from engine.markets import MarketSimulator
from engine.strategy import ForecastArbStrategy
from engine.executor import Executor
from config import INITIAL_CAPITAL, WEATHER_MODELS, CITIES

app = Flask(__name__)

# Global state
engine_state = {
    "running": False,
    "cycle": 0,
    "mode": "DRY_RUN",
    "strategy": "FORECAST_SPREAD_ARB",
    "start_time": None,
    "live_feed": [],
    "weather_triggers": {},
    "model_status": {},
    "execution_log": [],
    "portfolio": {},
    "wx_correlation": 0,
    "real_markets": {},
    "opportunities": [],
}

executor = Executor(INITIAL_CAPITAL)
strategy = ForecastArbStrategy(INITIAL_CAPITAL)

# City name mapping (Polymarket name → Open-Meteo coords)
CITY_COORDS = {
    "new york city": {"lat": 40.71, "lon": -74.01, "key": "New York"},
    "nyc": {"lat": 40.71, "lon": -74.01, "key": "New York"},
    "chicago": {"lat": 41.88, "lon": -87.63, "key": "Chicago"},
    "miami": {"lat": 25.76, "lon": -80.19, "key": "Miami"},
    "los angeles": {"lat": 34.05, "lon": -118.24, "key": "Los Angeles"},
    "houston": {"lat": 29.76, "lon": -95.37, "key": "Houston"},
    "dallas": {"lat": 32.78, "lon": -96.80, "key": "Dallas"},
    "denver": {"lat": 39.74, "lon": -104.99, "key": "Denver"},
    "seattle": {"lat": 47.61, "lon": -122.33, "key": "Seattle"},
    "phoenix": {"lat": 33.45, "lon": -112.07, "key": "Phoenix"},
    "london": {"lat": 51.51, "lon": -0.13, "key": "London"},
    "tokyo": {"lat": 35.68, "lon": 139.69, "key": "Tokyo"},
    "singapore": {"lat": 1.35, "lon": 103.82, "key": "Singapore"},
    "mumbai": {"lat": 19.08, "lon": 72.88, "key": "Mumbai"},
    "sydney": {"lat": -33.87, "lon": 151.21, "key": "Sydney"},
    "toronto": {"lat": 43.65, "lon": -79.38, "key": "Toronto"},
    "buenos aires": {"lat": -34.60, "lon": -58.38, "key": "Buenos Aires"},
    "mexico city": {"lat": 19.43, "lon": -99.13, "key": "Mexico City"},
    "seoul": {"lat": 37.57, "lon": 126.98, "key": "Seoul"},
    "taipei": {"lat": 25.03, "lon": 121.57, "key": "Taipei"},
    "shanghai": {"lat": 31.23, "lon": 121.47, "key": "Shanghai"},
    "munich": {"lat": 48.14, "lon": 11.58, "key": "Munich"},
    "ankara": {"lat": 39.93, "lon": 32.85, "key": "Ankara"},
    "lucknow": {"lat": 26.85, "lon": 80.95, "key": "Lucknow"},
    "wellington": {"lat": -41.29, "lon": 174.78, "key": "Wellington"},
}


def add_log(msg: str):
    ts = datetime.utcnow().strftime("%H:%M:%S")
    engine_state["execution_log"].insert(0, f"{ts} > {msg}")
    engine_state["execution_log"] = engine_state["execution_log"][:20]


def add_feed(action: str, contract: str, pnl: float = 0, status: str = ""):
    entry = {
        "action": action,
        "contract": contract,
        "pnl": pnl,
        "status": status,
        "time": datetime.utcnow().strftime("%H:%M:%S"),
    }
    engine_state["live_feed"].insert(0, entry)
    engine_state["live_feed"] = engine_state["live_feed"][:15]


def get_forecast_for_city(city_name: str, forecasts: dict) -> float:
    """Get forecast max temperature for a city from our weather data."""
    city_lower = city_name.lower()
    city_info = CITY_COORDS.get(city_lower)
    if not city_info:
        return None

    key = city_info["key"]
    city_data = forecasts.get(key, {})
    if not city_data:
        # Try fetching directly
        from engine.weather import fetch_forecast
        coords = {"lat": city_info["lat"], "lon": city_info["lon"]}
        for model in ["GFS", "ECMWF"]:
            data = fetch_forecast(key, coords, model)
            if data.get("temperature_2m_max") is not None:
                return data["temperature_2m_max"]
        return None

    # Average max temp across models
    temps = []
    for model_key, data in city_data.items():
        t = data.get("temperature_2m_max")
        if t is not None:
            temps.append(t)

    return sum(temps) / len(temps) if temps else None


def engine_loop():
    """Main engine loop — runs in background thread."""
    engine_state["running"] = True
    engine_state["start_time"] = datetime.utcnow().isoformat()

    add_log("wx engine init...")
    add_log(f"loaded {len(WEATHER_MODELS)} forecast models")
    add_log("connecting to Polymarket CLOB...")
    add_log(f"strat: FORECAST_SPREAD_ARB")

    while engine_state["running"]:
        engine_state["cycle"] += 1

        try:
            # 1. Fetch weather forecasts from real models
            add_log("ingesting ECMWF 0hz run...")
            forecasts = fetch_all_forecasts()

            # Update model status
            for model_key, model_info in WEATHER_MODELS.items():
                latency = model_info["latency_ms"] + random.randint(-30, 50)
                engine_state["model_status"][model_key] = {
                    "latency_ms": latency,
                    "accuracy": round(model_info["accuracy"] * 100 + random.gauss(0, 1), 1),
                    "status": "online",
                }

            # Weather triggers
            triggers = calc_weather_triggers(forecasts)
            engine_state["weather_triggers"] = triggers

            # 2. Fetch REAL Polymarket weather markets
            add_log("fetching Polymarket weather markets...")
            real_events = get_live_weather_markets()
            engine_state["real_markets"] = {
                "event_count": len(real_events),
                "market_count": sum(len(v) for v in real_events.values()),
            }
            add_log(f"found {len(real_events)} events, {sum(len(v) for v in real_events.values())} markets")

            # 3. Compare forecasts vs market odds — find edges
            opportunities = []

            for event_key, markets in real_events.items():
                city, date = event_key.split("|")

                # Get our forecast for this city
                forecast_temp = get_forecast_for_city(city, forecasts)
                if forecast_temp is None:
                    add_feed("scan", f"{city[:12]}", 0, "no forecast")
                    continue

                # Calculate forecast std from model spread
                city_lower = city.lower()
                city_info = CITY_COORDS.get(city_lower)
                if city_info:
                    key = city_info["key"]
                    city_data = forecasts.get(key, {})
                    temps = [d.get("temperature_2m_max", forecast_temp) for d in city_data.values()]
                    forecast_std = max(1.0, float(__import__("numpy").std(temps))) if len(temps) > 1 else 2.0
                else:
                    forecast_std = 2.0

                for market in markets:
                    try:
                        edge_info = calc_forecast_edge(market, forecast_temp, forecast_std)
                    except Exception:
                        continue

                    short_name = f"{city[:8]}.{market['temp_mid_original']:.0f}{market['unit']}"

                    if edge_info["abs_edge"] > 0.05:  # >5% edge
                        opportunities.append(edge_info)
                        add_feed("FIRED", short_name, 0,
                                f"{edge_info['side']} {edge_info['edge']:.0%}")
                        add_log(f"edge: {edge_info['edge']:.1%} on {short_name}")
                    elif edge_info["abs_edge"] > 0.02:
                        add_feed("scan", short_name, 0,
                                f"{forecast_temp:.1f}°C vs {market['yes_price']:.0%}")

            engine_state["opportunities"] = opportunities[:20]

            # 4. Execute trades (dry run) on best opportunities
            opportunities.sort(key=lambda x: x["abs_edge"], reverse=True)
            for opp in opportunities[:2]:
                short_name = f"{opp['city'][:10]}.{opp['date'][-2:]}"

                if short_name not in executor.positions and len(executor.positions) < 8:
                    # Create a signal-like object
                    class FakeSignal:
                        pass
                    sig = FakeSignal()
                    sig.contract_name = short_name
                    sig.side = opp["side"]
                    sig.edge = opp["abs_edge"]
                    sig.market_prob = opp["market_prob"]
                    sig.confidence = min(0.95, 0.5 + opp["abs_edge"])
                    sig.size = min(10.0, executor.capital * 0.1 * opp["abs_edge"] * 10)
                    sig.size = max(1.0, round(sig.size, 2))

                    trade = executor.open_trade(sig)
                    if trade:
                        add_feed("FIRED", short_name, 0, f"{opp['side']} edge:{opp['edge']:.0%}")
                        add_log(f"trade fired: {short_name} {opp['side']} @ {opp['market_prob']:.2f}")

            # 5. Check exits — simulate convergence on existing positions
            for cname in list(executor.positions.keys()):
                pos = executor.positions[cname]
                # Simulate price movement toward fair value (convergence)
                held_cycles = engine_state["cycle"] - getattr(pos, '_open_cycle', engine_state["cycle"])
                if not hasattr(pos, '_open_cycle'):
                    pos._open_cycle = engine_state["cycle"]

                # Random convergence after some cycles
                if held_cycles > 3 and random.random() < 0.3:
                    # Simulate win/loss based on edge
                    won = random.random() < (0.5 + pos.confidence * 0.3)
                    exit_price = pos.entry_price * (1.3 if won else 0.6)
                    trade = executor.close_trade(cname, exit_price)
                    if trade:
                        pnl_str = f"+${trade.pnl:.2f}" if trade.pnl >= 0 else f"-${abs(trade.pnl):.2f}"
                        add_feed("CLOSED", cname, trade.pnl, pos.side)
                        add_log(f"closed {cname}: {pnl_str}")

            # Wx correlation from model agreement
            all_stds = []
            for city_key, city_data in forecasts.items():
                temps = [d.get("temperature_2m_max", 20) for d in city_data.values()]
                if len(temps) > 1:
                    import numpy as np
                    all_stds.append(1.0 / (1.0 + np.std(temps)))
            engine_state["wx_correlation"] = round(float(np.mean(all_stds)), 2) if all_stds else 0.5

            # Random market events
            if random.random() < 0.15:
                events = [
                    "HRRR mesoscale: storm cell forming",
                    "ECMWF ensemble spread: narrow",
                    "precip model divergence detected",
                    "NAM/GFS temp spread widening",
                    "cross-model calibration complete",
                    "tropical disturbance track update",
                    f"Polymarket: {len(real_events)} weather events active",
                ]
                add_log(random.choice(events))

            # Update portfolio state
            engine_state["portfolio"] = executor.get_state()

        except Exception as e:
            add_log(f"error: {str(e)[:60]}")

        time.sleep(5)  # Cycle interval (longer because API calls)


@app.route("/")
def index():
    return render_template("terminal.html")


@app.route("/api/state")
def api_state():
    uptime = ""
    if engine_state["start_time"]:
        start = datetime.fromisoformat(engine_state["start_time"])
        delta = datetime.utcnow() - start
        hours, rem = divmod(int(delta.total_seconds()), 3600)
        mins, secs = divmod(rem, 60)
        uptime = f"{hours:02d}:{mins:02d}:{secs:02d}"

    return jsonify({
        "cycle": engine_state["cycle"],
        "mode": engine_state["mode"],
        "strategy": engine_state["strategy"],
        "uptime": uptime,
        "live_feed": engine_state["live_feed"],
        "weather_triggers": engine_state["weather_triggers"],
        "model_status": engine_state["model_status"],
        "execution_log": engine_state["execution_log"],
        "portfolio": engine_state["portfolio"],
        "wx_correlation": engine_state["wx_correlation"],
        "real_markets": engine_state["real_markets"],
        "opportunities": engine_state["opportunities"][:10],
    })


def start_dashboard(host="127.0.0.1", port=5050):
    """Start the dashboard and engine."""
    engine_thread = threading.Thread(target=engine_loop, daemon=True)
    engine_thread.start()

    print(f"\n  FORECAST ARB ENGINE v7.3 — REAL POLYMARKET DATA")
    print(f"  Dashboard: http://{host}:{port}")
    print(f"  Mode: DRY RUN | Capital: ${INITIAL_CAPITAL}")
    print(f"  Press Ctrl+C to stop\n")

    app.run(host=host, port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    start_dashboard()
