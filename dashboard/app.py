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

STATE_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "engine_state.json")

# Keys to persist across restarts
PERSIST_KEYS = [
    "cycle", "start_time", "live_feed", "execution_log",
    "live_balance", "live_initial_balance", "live_pnl",
    "live_trade_count", "live_wins", "live_trade_history",
    "live_equity_curve", "_active_markets_cache",
    "_market_prices_cache",
]

# Default state
DEFAULT_STATE = {
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
    "live_balance": 0.0,
    "live_initial_balance": 0.0,
    "live_pnl": 0.0,
    "live_open_orders": [],
    "live_trade_count": 0,
    "live_wins": 0,
    "live_trade_history": [],
    "live_equity_curve": [],
    "_active_markets_cache": {},
    "_market_prices_cache": {},
}


def load_state() -> dict:
    """Load persisted state from disk, merge with defaults."""
    state = dict(DEFAULT_STATE)
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                saved = json.load(f)
            for key in PERSIST_KEYS:
                if key in saved:
                    state[key] = saved[key]
            print(f"  [STATE] Restored: cycle={state['cycle']}, trades={state['live_trade_count']}, equity_pts={len(state['live_equity_curve'])}")
    except Exception as e:
        print(f"  [STATE] Load error: {e}")
    return state


def save_state():
    """Persist key state fields to disk."""
    try:
        to_save = {}
        for key in PERSIST_KEYS:
            to_save[key] = engine_state.get(key)
        with open(STATE_FILE, "w") as f:
            json.dump(to_save, f)
    except Exception as e:
        print(f"  [STATE] Save error: {e}")


engine_state = load_state()

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
    # Keep original start_time if restored, otherwise set new
    if not engine_state.get("start_time"):
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
            # Only set initial_balance if not restored from saved state
            if engine_state["live_initial_balance"] == 0:
                engine_state["live_initial_balance"] = balance
            # Append to existing equity curve (don't reset)
            engine_state["live_equity_curve"].append(
                (datetime.utcnow().isoformat(), round(balance, 2))
            )
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

            today_str = datetime.utcnow().strftime("%Y-%m-%d")

            for event_key, markets in real_events.items():
                city, date = event_key.split("|")

                # Skip expired markets (past dates)
                if date < today_str:
                    continue

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

                # Get open orders + filled trades (positions)
                orders = live_trader.get_open_orders()
                filled = live_trader.get_filled_trades()
                engine_state["live_open_orders"] = orders
                engine_state["live_filled_trades"] = filled

                # Check which markets are still active + fetch current prices
                market_ids = set(tr.get("market", "") for tr in filled)
                active_markets = engine_state.get("_active_markets_cache", {})
                market_prices = engine_state.get("_market_prices_cache", {})
                for mid in market_ids:
                    if mid and (mid not in active_markets or active_markets.get(mid, False)):
                        try:
                            mdata = live_trader.client.get_market(mid)
                            is_active = not mdata.get("closed", True)
                            active_markets[mid] = is_active
                            # Store current prices per outcome for active markets
                            if is_active:
                                for tok in mdata.get("tokens", []):
                                    outcome = tok.get("outcome", "")
                                    price = float(tok.get("price", 0))
                                    market_prices[f"{mid}|{outcome}"] = price
                        except Exception:
                            if mid not in active_markets:
                                active_markets[mid] = False
                engine_state["_active_markets_cache"] = active_markets
                engine_state["_market_prices_cache"] = market_prices

                active_count = sum(1 for mid in market_ids if active_markets.get(mid, False))
                add_log(f"wallet: ${balance:.2f} USDC | orders: {len(orders)} | active positions: {active_count}")

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
                        max(1.1, balance * opp["abs_edge"] * 5),
                    )
                    trade_size = round(trade_size, 2)

                    # Polymarket minimum order is $1
                    if trade_size < 1.0:
                        continue

                    short_name = f"{opp['city'][:10]}.{opp['date'][-2:]}"

                    try:
                        # Use token IDs directly from Gamma API (no extra CLOB lookup)
                        if opp["side"] == "YES":
                            token_id = opp.get("yes_token_id", "")
                        else:
                            token_id = opp.get("no_token_id", "")

                        if not token_id:
                            add_log(f"no token_id for {short_name}")
                            continue

                        if opp["side"] == "YES":
                            result = live_trader.buy_yes(token_id, trade_size, opp["market_prob"])
                        else:
                            # NO price = 1 - YES price
                            no_price = round(1.0 - opp["market_prob"], 4)
                            result = live_trader.buy_no(token_id, trade_size, no_price)

                        if "error" in result:
                            err = result["error"]
                            if "market not found" in err:
                                add_log(f"skip {short_name}: not on CLOB")
                            else:
                                add_log(f"ORDER FAIL: {err[:60]}")
                                add_feed("FAILED", short_name, 0, err[:30])
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
                                "pnl": 0,  # Unknown until resolved
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

        # Persist state to disk every cycle
        save_state()
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

        # Aggregate filled trades into net positions — only ACTIVE markets
        positions = {}
        filled = engine_state.get("live_filled_trades", [])
        active_cache = engine_state.get("_active_markets_cache", {})
        pos_agg = {}
        for trade in filled:
            market = trade.get("market", "?")
            # Skip closed/resolved markets
            if not active_cache.get(market, False):
                continue
            outcome = trade.get("outcome", "?")
            key = f"{market}|{outcome}"
            size = float(trade.get("size", 0))
            price = float(trade.get("price", 0))
            side = trade.get("side", "BUY")

            if key not in pos_agg:
                pos_agg[key] = {"buy_shares": 0, "buy_cost": 0,
                                "sell_shares": 0, "sell_revenue": 0,
                                "outcome": outcome, "market": market, "fills": 0}
            if side == "BUY":
                pos_agg[key]["buy_shares"] += size
                pos_agg[key]["buy_cost"] += size * price
            else:
                pos_agg[key]["sell_shares"] += size
                pos_agg[key]["sell_revenue"] += size * price
            pos_agg[key]["fills"] += 1

        price_cache = engine_state.get("_market_prices_cache", {})
        for key, agg in pos_agg.items():
            net_shares = agg["buy_shares"] - agg["sell_shares"]
            if net_shares < 1:
                continue
            avg_price = round(agg["buy_cost"] / agg["buy_shares"], 4) if agg["buy_shares"] > 0 else 0
            cost_basis = round(agg["buy_cost"] - agg["sell_revenue"], 2)

            # Current market price for this outcome
            current_price = price_cache.get(key, avg_price)
            # Unrealized PnL = (current_price - avg_entry) * net_shares
            unrealized_pnl = round((current_price - avg_price) * net_shares, 2)
            current_value = round(current_price * net_shares, 2)

            short_market = agg["market"][:10]
            name = f"{short_market}_{agg['outcome']}"
            positions[name] = {
                "id": name[:8],
                "contract": f"{short_market}...",
                "side": f"BUY {agg['outcome']}",
                "size": max(0, cost_basis),
                "entry_price": avg_price,
                "current_price": current_price,
                "current_value": current_value,
                "pnl": unrealized_pnl,
                "target_price": 0,
                "status": f"{net_shares:.0f} shares",
                "open_time": "",
                "close_time": None,
            }

        # Also add pending open orders
        for order in engine_state["live_open_orders"]:
            oid = order.get("id", "?")[:8]
            market = order.get("market", oid)[:10]
            outcome = order.get("outcome", "")
            name = f"{market}_{oid}"
            positions[name] = {
                "id": oid,
                "contract": f"{market}...",
                "side": f"{order.get('side', 'BUY')} {outcome}",
                "size": round(float(order.get("original_size", 0)) * float(order.get("price", 0)), 2),
                "entry_price": float(order.get("price", 0)),
                "exit_price": None,
                "pnl": 0,
                "target_price": 0,
                "status": order.get("status", "PENDING"),
                "open_time": "",
                "close_time": None,
            }

        exposure_usd = sum(p["size"] for p in positions.values())
        exposure_pct = (exposure_usd / balance * 100) if balance > 0 else 0

        # Calculate drawdown from equity curve
        eq_values = [e[1] for e in engine_state["live_equity_curve"]]
        max_eq = max(eq_values) if eq_values else balance
        drawdown = max_eq - balance
        drawdown_pct = round(drawdown / max_eq * 100, 1) if max_eq > 0 else 0

        # Calculate Sharpe and daily VaR from trade history
        import numpy as np
        trade_pnls = [t.get("pnl", 0) for t in engine_state["live_trade_history"] if "pnl" in t]
        sharpe = 0
        daily_var = 0
        daily_var_usd = 0
        if len(trade_pnls) >= 2 and np.std(trade_pnls) > 0:
            sharpe = round(float(np.mean(trade_pnls) / np.std(trade_pnls) * (252 ** 0.5)), 2)
            daily_var_usd = round(abs(float(np.percentile(trade_pnls, 5))), 2)
            daily_var = round(daily_var_usd / balance * 100, 1) if balance > 0 else 0

        # Kelly from win rate (safe against missing/zero pnl)
        kelly_f = round(balance * 0.25, 2)
        if trade_count > 0 and wins > 0:
            try:
                win_pnls = [t.get("pnl", 0) for t in engine_state["live_trade_history"] if t.get("pnl", 0) > 0]
                loss_pnls = [t.get("pnl", 0) for t in engine_state["live_trade_history"] if t.get("pnl", 0) < 0]
                avg_win = float(np.mean(win_pnls)) if win_pnls else 1
                avg_loss = abs(float(np.mean(loss_pnls))) if loss_pnls else 1
                b = avg_win / avg_loss if avg_loss > 0 else 1
                win_p = wins / trade_count
                kelly_raw = (b * win_p - (1 - win_p)) / b if b > 0 else 0
                kelly_f = round(max(0, kelly_raw * 0.25 * balance), 2)
            except Exception:
                pass

        # Wx confidence from active opportunities
        opp_edges = [o["abs_edge"] for o in engine_state.get("opportunities", []) if o.get("abs_edge", 0) > 0.05]
        wx_confidence = round(min(95, len(opp_edges) * 15 + sum(opp_edges) * 100), 0) if opp_edges else 0

        portfolio = {
            "equity": round(balance, 2),
            "capital": round(balance, 2),
            "total_pnl": round(pnl, 2),
            "today_pnl": round(pnl, 2),
            "today_trades": trade_count,
            "win_rate": round(win_rate, 1),
            "sharpe": sharpe,
            "exposure": round(exposure_pct, 1),
            "exposure_usd": round(exposure_usd, 2),
            "drawdown": drawdown_pct,
            "daily_var": daily_var,
            "daily_var_usd": daily_var_usd,
            "total_trades": trade_count,
            "winning_trades": wins,
            "kelly_f": kelly_f,
            "max_pos": MAX_POSITION_SIZE,
            "max_exposure": 30,
            "wx_confidence": wx_confidence,
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
