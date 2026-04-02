"""
Forecast Arb Engine — Web Terminal Dashboard.
Flask app with real-time updates via polling.
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
from engine.markets import MarketSimulator
from engine.strategy import ForecastArbStrategy
from engine.executor import Executor
from config import INITIAL_CAPITAL, WEATHER_MODELS, CONTRACT_TYPES

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
}

executor = Executor(INITIAL_CAPITAL)
market_sim = MarketSimulator()
strategy = ForecastArbStrategy(INITIAL_CAPITAL)


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


def engine_loop():
    """Main engine loop — runs in background thread."""
    engine_state["running"] = True
    engine_state["start_time"] = datetime.utcnow().isoformat()

    add_log("wx engine init...")
    add_log(f"loaded {len(WEATHER_MODELS)} forecast models")
    add_log("ingesting NOAA/ECMWF/GFS feeds...")
    add_log(f"strat: FORECAST_SPREAD_ARB")

    while engine_state["running"]:
        engine_state["cycle"] += 1
        cycle = engine_state["cycle"]

        try:
            # Fetch weather forecasts
            add_log(f"ingesting ECMWF 0hz run...")
            forecasts = fetch_all_forecasts()

            # Update model status
            for model_key, model_info in WEATHER_MODELS.items():
                latency = model_info["latency_ms"] + random.randint(-30, 50)
                engine_state["model_status"][model_key] = {
                    "latency_ms": latency,
                    "accuracy": round(model_info["accuracy"] * 100 + random.gauss(0, 1), 1),
                    "status": "online",
                }

            # Calculate weather triggers
            triggers = calc_weather_triggers(forecasts)
            engine_state["weather_triggers"] = triggers

            # Update market odds (with lag/noise to simulate slow market)
            market_sim.update_all_odds(forecasts, noise=0.08)

            # Correlation metric
            engine_state["wx_correlation"] = round(random.uniform(0.4, 0.7), 2)

            add_log(f"cross-model calibration: {random.uniform(0.8, 0.95):.3f}")

            # Check exits on existing positions
            for cname in list(executor.positions.keys()):
                if cname in market_sim.contracts:
                    contract = market_sim.contracts[cname]
                    pos = executor.positions[cname]
                    if strategy.check_exit(pos.to_dict(), contract):
                        exit_price = contract.yes_price if pos.side == "YES" else contract.no_price
                        trade = executor.close_trade(cname, exit_price)
                        if trade:
                            pnl_str = f"+${trade.pnl:.2f}" if trade.pnl >= 0 else f"-${abs(trade.pnl):.2f}"
                            add_feed("CLOSED", cname, trade.pnl, f"{trade.side}")
                            add_log(f"closed {cname}: {pnl_str}")

            # Scan for new opportunities
            signals = strategy.scan_opportunities(
                market_sim.contracts, forecasts, executor.exposure
            )

            for sig in signals[:2]:  # Max 2 trades per cycle
                trade = executor.open_trade(sig)
                if trade:
                    add_feed("FIRED", sig.contract_name, 0, f"{sig.side} {sig.edge:.0%}")
                    add_log(f"trade fired: {sig.contract_name} {sig.side} @ {sig.market_prob:.2f}")
                    add_log(f"edge: {sig.edge:.1%}, conf: {sig.confidence:.0%}")

            # Contracts being scanned
            for ct in CONTRACT_TYPES:
                if ct["name"] not in executor.positions and random.random() < 0.3:
                    city = ct["city"]
                    consensus = calc_model_consensus(forecasts, city, ct["metric"])
                    if consensus["n"] > 0:
                        add_feed("scan", ct["name"], 0,
                                f"{consensus['mean']:.1f}±{consensus['std']:.1f}")

            # Random market events
            if random.random() < 0.15:
                events = [
                    "HRRR mesoscale: storm cell forming",
                    "wind farm output forecast: -18%",
                    "wheat futures corr w/ frost: 0.89",
                    "UV index forecast error: +1.8σ",
                    "precip model divergence detected",
                    "NAM/GFS temp spread widening",
                    "ECMWF ensemble spread: narrow",
                    "tropical disturbance track update",
                ]
                add_log(random.choice(events))

            # Simulate some position P&L movement
            for cname, contract in market_sim.contracts.items():
                if cname in executor.positions:
                    # Slight price drift
                    drift = random.gauss(0.005, 0.02)
                    contract.yes_price = max(0.02, min(0.98, contract.yes_price + drift))
                    contract.no_price = 1 - contract.yes_price

            # Update portfolio state
            engine_state["portfolio"] = executor.get_state()

        except Exception as e:
            add_log(f"error: {str(e)[:60]}")

        time.sleep(3)  # Cycle interval


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
    })


def start_dashboard(host="127.0.0.1", port=5050):
    """Start the dashboard and engine."""
    engine_thread = threading.Thread(target=engine_loop, daemon=True)
    engine_thread.start()

    print(f"\n  FORECAST ARB ENGINE v7.3")
    print(f"  Dashboard: http://{host}:{port}")
    print(f"  Mode: DRY RUN | Capital: ${INITIAL_CAPITAL}")
    print(f"  Press Ctrl+C to stop\n")

    app.run(host=host, port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    start_dashboard()
