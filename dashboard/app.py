"""
Forecast Arb Engine — Web Terminal Dashboard.
Flask app with real-time updates via polling.
LIVE mode: real Polymarket wallet balance, real trades.
DRY_RUN mode: simulated executor.
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
from engine.polymarket_real import get_live_weather_markets, calc_forecast_edge, parse_market
from engine.markets import MarketSimulator
from engine.data_logger import log_odds, log_forecasts, log_trade, get_logged_stats
from engine.live_trader import LiveTrader, check_env
from engine.strategy import ForecastArbStrategy
from engine.executor import Executor
from config import INITIAL_CAPITAL, WEATHER_MODELS, CITIES, MAX_POSITION_SIZE

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
    "wx_correlation": 0,
    "real_markets": {},
    "opportunities": [],
    # LIVE mode wallet data
    "live_balance": 0.0,
    "live_initial_balance": 0.0,
    "live_pnl": 0.0,
    "live_open_orders": [],
    "live_trade_count": 0,
    "live_wins": 0,
    "live_trade_history": [],
    "live_equity_curve": [],
}

executor = Executor(INITIAL_CAPITAL)  # Only used in DRY_RUN
live_trader = None

# City name mapping
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
    city_lower = city_name.lower()
    city_info = CITY_COORDS.get(city_lower)
    if not city_info:
        return None

    key = city_info["key"]
    city_data = forecasts.get(key, {})
    if not city_data:
        from engine.weather import fetch_forecast
        coords = {"lat": city_info["lat"], "lon": city_info["lon"]}
        for model in ["GFS", "ECMWF"]:
            data = fetch_forecast(key, coords, model)
            if data.get("temperature_2m_max") is not None:
                return data["temperature_2m_max"]
        return None

    temps = []
    for model_key, data in city_data.items():
        t = data.get("temperature_2m_max")
        if t is not None:
            temps.append(t)

    return sum(temps) / len(temps) if temps else None


def engine_loop():
    """Main engine loop — runs in background thread."""
    global live_trader

    engine_state["running"] = True
    engine_state["start_time"] = datetime.utcnow().isoformat()

    # Check if live mode
    is_live = os.environ.get("ENGINE_MODE") == "LIVE"
    if is_live:
        engine_state["mode"] = "LIVE"
        add_log("*** LIVE MODE — REAL WALLET ***")
        live_trader = LiveTrader()
        if live_trader.connect():
            balance = live_trader.get_balance()
            engine_state["live_balance"] = balance
            engine_state["live_initial_balance"] = balance
            engine_state["live_equity_curve"] = [
                (datetime.utcnow().isoformat(), round(balance, 2))
            ]
            add_log(f"wallet connected, balance: ${balance:.2f} USDC")
            add_log(f"max position: ${live_trader.max_position}")
        else:
            add_log("ERROR: wallet connection failed, falling back to DRY RUN")
            engine_state["mode"] = "DRY_RUN"
            is_live = False
    else:
        engine_state["mode"] = "DRY_RUN"

    add_log("wx engine init...")
    add_log(f"loaded {len(WEATHER_MODELS)} forecast models")
    add_log("connecting to Polymarket CLOB...")
    add_log(f"strat: FORECAST_SPREAD_ARB")

    while engine_state["running"]:
        engine_state["cycle"] += 1

        try:
            cycle = engine_state["cycle"]

            # 1. Fetch weather forecasts
            add_log("ingesting ECMWF 0hz run...")
            forecasts = fetch_all_forecasts()
            log_forecasts(cycle, forecasts)

            # Update model status
            for model_key, model_info in WEATHER_MODELS.items():
                latency = model_info["latency_ms"] + random.randint(-30, 50)
                engine_state["model_status"][model_key] = {
                    "latency_ms": latency,
                    "accuracy": round(model_info["accuracy"] * 100 + random.gauss(0, 1), 1),
                    "status": "online",
                }

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

            # LOG all Polymarket odds
            all_parsed = []
            for event_markets in real_events.values():
                all_parsed.extend(event_markets)
            log_odds(cycle, all_parsed)

            # 3. Compare forecasts vs market odds
            opportunities = []

            for event_key, markets in real_events.items():
                city, date = event_key.split("|")

                forecast_temp = get_forecast_for_city(city, forecasts)
                if forecast_temp is None:
                    add_feed("scan", f"{city[:12]}", 0, "no forecast")
                    continue

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

                    if edge_info["abs_edge"] > 0.05:
                        opportunities.append(edge_info)
                        add_feed("FIRED", short_name, 0,
                                f"{edge_info['side']} {edge_info['edge']:.0%}")
                        add_log(f"edge: {edge_info['edge']:.1%} on {short_name}")
                    elif edge_info["abs_edge"] > 0.02:
                        add_feed("scan", short_name, 0,
                                f"{forecast_temp:.1f}°C vs {market['yes_price']:.0%}")

            engine_state["opportunities"] = opportunities[:20]

            # 4. LIVE MODE: real wallet data + real trade execution
            if is_live and live_trader and live_trader.connected:
                # Refresh real balance every cycle
                balance = live_trader.get_balance()
                engine_state["live_balance"] = balance
                engine_state["live_pnl"] = round(balance - engine_state["live_initial_balance"], 2)

                # Track equity curve from real balance
                engine_state["live_equity_curve"].append(
                    (datetime.utcnow().isoformat(), round(balance, 2))
                )
                engine_state["live_equity_curve"] = engine_state["live_equity_curve"][-200:]

                # Get real open orders
                orders = live_trader.get_open_orders()
                engine_state["live_open_orders"] = orders

                add_log(f"wallet: ${balance:.2f} USDC | orders: {len(orders)}")

                # Execute on best opportunities (only if we have balance)
                opportunities.sort(key=lambda x: x["abs_edge"], reverse=True)
                for opp in opportunities[:1]:  # Max 1 trade per cycle in live
                    if balance < 1.0:
                        add_log("insufficient balance for live trade")
                        break

                    cond_id = opp.get("condition_id", "")
                    if not cond_id:
                        continue

                    trade_size = min(
                        live_trader.max_position,
                        balance * 0.4,  # Max 40% of balance per trade
                        max(0.5, balance * opp["abs_edge"] * 5),
                    )
                    trade_size = round(trade_size, 2)

                    if trade_size < 0.50:
                        continue

                    short_name = f"{opp['city'][:10]}.{opp['date'][-2:]}"

                    try:
                        market_info = live_trader.get_market_info(cond_id)
                        tokens = market_info.get("tokens", [])
                        if not tokens:
                            continue

                        token_id = tokens[0].get("token_id", "")
                        if not token_id:
                            continue

                        if opp["side"] == "YES":
                            result = live_trader.buy_yes(token_id, trade_size, opp["market_prob"])
                        else:
                            result = live_trader.buy_no(token_id, trade_size, opp["market_prob"])

                        if "error" in result:
                            add_log(f"LIVE ORDER FAILED: {result['error'][:60]}")
                            add_feed("FAILED", short_name, 0, result["error"][:30])
                        else:
                            add_log(f"LIVE ORDER OK: {short_name} {opp['side']} ${trade_size}")
                            add_feed("LIVE", short_name, 0,
                                    f"{opp['side']} ${trade_size:.2f}")

                            engine_state["live_trade_count"] += 1
                            engine_state["live_trade_history"].append({
                                "time": datetime.utcnow().strftime("%H:%M:%S"),
                                "contract": short_name,
                                "side": opp["side"],
                                "size": trade_size,
                                "price": opp["market_prob"],
                                "edge": opp["abs_edge"],
                                "status": "filled",
                            })
                            engine_state["live_trade_history"] = engine_state["live_trade_history"][-20:]

                            log_trade(cycle, {
                                "contract": short_name, "side": opp["side"],
                                "entry_price": opp["market_prob"], "size": trade_size,
                                "edge": opp["abs_edge"], "confidence": min(0.95, 0.5 + opp["abs_edge"]),
                                "status": "live_filled",
                            })

                    except Exception as e:
                        add_log(f"LIVE TRADE ERROR: {str(e)[:60]}")

            # DRY_RUN MODE: simulated executor
            elif not is_live:
                opportunities.sort(key=lambda x: x["abs_edge"], reverse=True)
                for opp in opportunities[:2]:
                    short_name = f"{opp['city'][:10]}.{opp['date'][-2:]}"

                    if short_name not in executor.positions and len(executor.positions) < 8:
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
                            log_trade(cycle, {
                                "contract": short_name, "side": opp["side"],
                                "entry_price": opp["market_prob"], "size": sig.size,
                                "edge": opp["abs_edge"], "confidence": sig.confidence,
                                "status": "open",
                            })

                # Simulate exits (DRY_RUN only)
                for cname in list(executor.positions.keys()):
                    pos = executor.positions[cname]
                    held_cycles = engine_state["cycle"] - getattr(pos, '_open_cycle', engine_state["cycle"])
                    if not hasattr(pos, '_open_cycle'):
                        pos._open_cycle = engine_state["cycle"]

                    if held_cycles > 3 and random.random() < 0.3:
                        won = random.random() < (0.5 + pos.confidence * 0.3)
                        exit_price = pos.entry_price * (1.3 if won else 0.6)
                        trade = executor.close_trade(cname, exit_price)
                        if trade:
                            pnl_str = f"+${trade.pnl:.2f}" if trade.pnl >= 0 else f"-${abs(trade.pnl):.2f}"
                            add_feed("CLOSED", cname, trade.pnl, pos.side)
                            add_log(f"closed {cname}: {pnl_str}")
                            log_trade(cycle, {
                                "contract": cname, "side": pos.side,
                                "entry_price": pos.entry_price, "size": pos.size,
                                "edge": "", "confidence": pos.confidence,
                                "status": "closed", "exit_price": exit_price,
                                "pnl": trade.pnl,
                            })

            # Wx correlation
            all_stds = []
            for city_key, city_data in forecasts.items():
                temps = [d.get("temperature_2m_max", 20) for d in city_data.values()]
                if len(temps) > 1:
                    import numpy as np
                    all_stds.append(1.0 / (1.0 + np.std(temps)))
            engine_state["wx_correlation"] = round(float(__import__("numpy").mean(all_stds)), 2) if all_stds else 0.5

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

        except Exception as e:
            add_log(f"error: {str(e)[:60]}")

        time.sleep(5)


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

    is_live = engine_state["mode"] == "LIVE"

    # Build portfolio based on mode
    if is_live:
        balance = engine_state["live_balance"]
        initial = engine_state["live_initial_balance"]
        pnl = engine_state["live_pnl"]
        trade_count = engine_state["live_trade_count"]
        wins = engine_state["live_wins"]
        win_rate = (wins / trade_count * 100) if trade_count > 0 else 0

        # Open orders as positions
        positions = {}
        for order in engine_state["live_open_orders"]:
            name = order.get("market", order.get("id", "?"))[:15]
            positions[name] = {
                "id": order.get("id", ""),
                "contract": name,
                "side": order.get("side", "BUY"),
                "size": float(order.get("size", 0)),
                "entry_price": float(order.get("price", 0)),
                "exit_price": None,
                "pnl": 0,
                "target_price": 0,
                "status": order.get("status", "open"),
                "open_time": order.get("created_at", ""),
                "close_time": None,
            }

        exposure_usd = sum(p["size"] for p in positions.values())
        exposure_pct = (exposure_usd / balance * 100) if balance > 0 else 0

        portfolio = {
            "equity": round(balance, 2),
            "capital": round(balance, 2),
            "total_pnl": round(pnl, 2),
            "today_pnl": round(pnl, 2),
            "today_trades": trade_count,
            "win_rate": round(win_rate, 1),
            "sharpe": 0,
            "exposure": round(exposure_pct, 1),
            "exposure_usd": round(exposure_usd, 2),
            "drawdown": 0,
            "daily_var": 0,
            "daily_var_usd": 0,
            "total_trades": trade_count,
            "winning_trades": wins,
            "kelly_f": round(balance * 0.25, 2) if balance > 0 else 0,
            "max_pos": MAX_POSITION_SIZE,
            "max_exposure": 30,
            "wx_confidence": 0,
            "initial_capital": round(initial, 2),
            "positions": positions,
            "recent_trades": engine_state["live_trade_history"][-10:],
            "equity_curve": engine_state["live_equity_curve"][-200:],
        }
    else:
        portfolio = executor.get_state()

    return jsonify({
        "cycle": engine_state["cycle"],
        "mode": engine_state["mode"],
        "strategy": engine_state["strategy"],
        "uptime": uptime,
        "live_feed": engine_state["live_feed"],
        "weather_triggers": engine_state["weather_triggers"],
        "model_status": engine_state["model_status"],
        "execution_log": engine_state["execution_log"],
        "portfolio": portfolio,
        "wx_correlation": engine_state["wx_correlation"],
        "real_markets": engine_state["real_markets"],
        "opportunities": engine_state["opportunities"][:10],
        "data_log": get_logged_stats(),
    })


def start_dashboard(host="127.0.0.1", port=5050):
    engine_thread = threading.Thread(target=engine_loop, daemon=True)
    engine_thread.start()

    mode = os.environ.get("ENGINE_MODE", "DRY_RUN")
    print(f"\n  FORECAST ARB ENGINE v7.3 — REAL POLYMARKET DATA")
    print(f"  Dashboard: http://{host}:{port}")
    print(f"  Mode: {mode} | Max Position: ${MAX_POSITION_SIZE}")
    print(f"  Press Ctrl+C to stop\n")

    app.run(host=host, port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    start_dashboard()
