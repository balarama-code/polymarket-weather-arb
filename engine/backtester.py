"""
Backtester — run forecast arbitrage strategy on historical weather data.
Uses Open-Meteo archive API for actual weather, simulates market odds with lag.
"""

import random
import math
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from engine.markets import WeatherContract
from engine.strategy import ForecastArbStrategy, Signal
from config import CONTRACT_TYPES, CITIES, OPEN_METEO_HIST, INITIAL_CAPITAL

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


def _generate_synthetic_weather(n_days: int, city: str) -> pd.DataFrame:
    """Generate synthetic daily weather data for backtesting."""
    base_temps = {"Chicago": 10, "New York": 12, "Miami": 28, "Los Angeles": 22,
                  "Houston": 25, "Denver": 8, "Seattle": 11, "Phoenix": 30}
    base = base_temps.get(city, 15)

    dates = pd.date_range(end=datetime.utcnow().date(), periods=n_days, freq="D")
    np.random.seed(hash(city) % 2**31)

    # Seasonal pattern
    day_of_year = np.array([d.timetuple().tm_yday for d in dates])
    seasonal = 12 * np.sin(2 * np.pi * (day_of_year - 80) / 365)

    temp_max = base + seasonal + np.random.normal(0, 4, n_days)
    temp_min = temp_max - np.abs(np.random.normal(8, 3, n_days))
    precip = np.maximum(0, np.random.exponential(3, n_days))
    snow = np.where(temp_min < 2, np.maximum(0, np.random.exponential(2, n_days)), 0)
    wind = np.maximum(0, np.random.normal(15, 10, n_days))

    return pd.DataFrame({
        "date": dates,
        "temperature_2m_max": temp_max,
        "temperature_2m_min": temp_min,
        "precipitation": precip,
        "snowfall": snow,
        "wind_speed_10m_max": wind,
        "cape": np.maximum(0, np.random.normal(500, 500, n_days)),
        "visibility": np.maximum(100, np.random.normal(8000, 3000, n_days)),
    })


def _fetch_historical(city: str, coords: dict, start: str, end: str) -> pd.DataFrame:
    """Try fetching real historical data, fall back to synthetic."""
    if HAS_REQUESTS:
        try:
            params = {
                "latitude": coords["lat"], "longitude": coords["lon"],
                "start_date": start, "end_date": end,
                "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,snowfall_sum,wind_speed_10m_max",
            }
            resp = requests.get(OPEN_METEO_HIST, params=params, timeout=20)
            if resp.status_code == 200:
                data = resp.json().get("daily", {})
                if data and data.get("time"):
                    df = pd.DataFrame({
                        "date": pd.to_datetime(data["time"]),
                        "temperature_2m_max": data.get("temperature_2m_max", []),
                        "temperature_2m_min": data.get("temperature_2m_min", []),
                        "precipitation": data.get("precipitation_sum", []),
                        "snowfall": data.get("snowfall_sum", []),
                        "wind_speed_10m_max": data.get("wind_speed_10m_max", []),
                        "cape": [0] * len(data["time"]),
                        "visibility": [10000] * len(data["time"]),
                    })
                    df = df.dropna()
                    if len(df) > 30:
                        return df
        except:
            pass

    n_days = (datetime.strptime(end, "%Y-%m-%d") - datetime.strptime(start, "%Y-%m-%d")).days
    return _generate_synthetic_weather(max(n_days, 90), city)


def run_backtest(days: int = 365, capital: float = None, verbose: bool = True) -> dict:
    """
    Run full backtest of forecast arb strategy on historical weather data.
    """
    if capital is None:
        capital = INITIAL_CAPITAL

    end_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (datetime.utcnow() - timedelta(days=days + 1)).strftime("%Y-%m-%d")

    if verbose:
        print(f"\n{'='*60}")
        print(f"  FORECAST ARB ENGINE — BACKTEST")
        print(f"  Period: {start_date} to {end_date} ({days} days)")
        print(f"  Capital: ${capital:.2f}")
        print(f"{'='*60}\n")
        print("  Loading historical weather data...")

    # Load historical data per city
    city_data = {}
    for city, coords in CITIES.items():
        df = _fetch_historical(city, coords, start_date, end_date)
        city_data[city] = df
        if verbose:
            print(f"    {city}: {len(df)} days loaded")

    # Initialize
    strategy = ForecastArbStrategy(capital)
    equity = capital
    positions = {}  # contract -> {side, entry_price, size, day}
    trades = []
    equity_curve = [capital]
    max_eq = capital
    max_dd = 0

    n_days_actual = min(len(df) for df in city_data.values())
    if verbose:
        print(f"\n  Running strategy on {n_days_actual} days...\n")

    for day_idx in range(10, n_days_actual):
        # Simulate forecast: actual value + noise (model forecast)
        forecasts = {}
        for city, df in city_data.items():
            if day_idx >= len(df):
                continue
            row = df.iloc[day_idx]
            forecasts[city] = {}
            for model in ["ECMWF", "GFS", "HRRR", "NAM", "UKMO", "CMC"]:
                noise_scale = 0.05 + random.random() * 0.1
                forecasts[city][model] = {
                    "temperature_2m_max": row["temperature_2m_max"] + random.gauss(0, 2),
                    "temperature_2m_min": row["temperature_2m_min"] + random.gauss(0, 2),
                    "precipitation": max(0, row["precipitation"] + random.gauss(0, 2)),
                    "snowfall": max(0, row["snowfall"] + random.gauss(0, 1)),
                    "wind_speed_10m_max": max(0, row["wind_speed_10m_max"] + random.gauss(0, 5)),
                    "cape": max(0, row.get("cape", 500) + random.gauss(0, 200)),
                    "visibility": max(100, row.get("visibility", 8000) + random.gauss(0, 2000)),
                }

        # Create/update contracts with "slow" market odds (lagged data)
        contracts = {}
        for ct in CONTRACT_TYPES:
            contract = WeatherContract(
                ct["name"], ct["city"], ct["metric"],
                ct["threshold"], ct["direction"], ct["category"]
            )
            # Market uses yesterday's data (lagged)
            if day_idx > 0 and ct["city"] in city_data:
                prev_row = city_data[ct["city"]].iloc[day_idx - 1]
                prev_val = prev_row.get(ct["metric"], 20)
                if prev_val is not None and not (isinstance(prev_val, float) and np.isnan(prev_val)):
                    contract.update_market_odds(float(prev_val), noise=0.10)
            contracts[ct["name"]] = contract

        # Check exits
        to_close = []
        for cname, pos in positions.items():
            if cname in contracts:
                c = contracts[cname]
                current_price = c.yes_price if pos["side"] == "YES" else c.no_price
                # Exit conditions: convergence, stop loss, or held > 3 days
                held_days = day_idx - pos["day"]
                if current_price >= pos["entry_price"] * 1.2:
                    to_close.append((cname, current_price, "convergence"))
                elif current_price < pos["entry_price"] * 0.6:
                    to_close.append((cname, current_price, "stop_loss"))
                elif held_days >= 3:
                    # Resolve: check actual value
                    actual_row = city_data.get(c.city)
                    if actual_row is not None and day_idx < len(actual_row):
                        actual_val = actual_row.iloc[day_idx].get(c.metric, 0)
                        if actual_val is not None and not np.isnan(actual_val):
                            if c.direction == "above":
                                won = actual_val > c.threshold
                            else:
                                won = actual_val < c.threshold
                            won = won if pos["side"] == "YES" else not won
                            exit_p = 1.0 if won else 0.0
                            to_close.append((cname, exit_p, "resolved"))
                            continue
                    to_close.append((cname, current_price, "timeout"))

        for cname, exit_price, reason in to_close:
            pos = positions.pop(cname)
            n_contracts = pos["size"] / pos["entry_price"]
            pnl = n_contracts * (exit_price - pos["entry_price"])
            equity += pos["size"] + pnl
            trades.append({
                "contract": cname,
                "side": pos["side"],
                "entry": pos["entry_price"],
                "exit": exit_price,
                "size": pos["size"],
                "pnl": round(pnl, 2),
                "reason": reason,
                "day": day_idx,
            })

        # Scan for new signals
        current_exposure = sum(p["size"] for p in positions.values())
        signals = strategy.scan_opportunities(contracts, forecasts, current_exposure)

        for sig in signals[:3]:  # Max 3 new trades per day
            if sig.contract_name not in positions and len(positions) < 8:
                if sig.size <= equity * 0.3:
                    positions[sig.contract_name] = {
                        "side": sig.side,
                        "entry_price": sig.market_prob,
                        "size": sig.size,
                        "day": day_idx,
                    }
                    equity -= sig.size

        # Track equity
        total_eq = equity + sum(p["size"] for p in positions.values())
        equity_curve.append(round(total_eq, 2))
        if total_eq > max_eq:
            max_eq = total_eq
        dd = max_eq - total_eq
        if dd > max_dd:
            max_dd = dd

    # Close remaining positions at current price
    for cname, pos in list(positions.items()):
        pnl = 0  # Flat close
        equity += pos["size"]
        trades.append({
            "contract": cname, "side": pos["side"],
            "entry": pos["entry_price"], "exit": pos["entry_price"],
            "size": pos["size"], "pnl": 0, "reason": "end", "day": n_days_actual,
        })

    # Calculate stats
    total_pnl = equity_curve[-1] - capital
    pnls = [t["pnl"] for t in trades if t["reason"] != "end"]
    wins = sum(1 for p in pnls if p > 0)
    n_trades = len(pnls)
    win_rate = (wins / n_trades * 100) if n_trades > 0 else 0
    sharpe = 0
    if pnls and np.std(pnls) > 0:
        sharpe = round(np.mean(pnls) / np.std(pnls) * (252 ** 0.5), 2)
    dd_pct = round(max_dd / max_eq * 100, 1) if max_eq > 0 else 0
    ret_pct = round(total_pnl / capital * 100, 2)

    results = {
        "capital": capital,
        "final_equity": round(equity_curve[-1], 2),
        "total_pnl": round(total_pnl, 2),
        "return_pct": ret_pct,
        "total_trades": n_trades,
        "winning_trades": wins,
        "win_rate": round(win_rate, 1),
        "sharpe": sharpe,
        "max_drawdown_pct": dd_pct,
        "equity_curve": equity_curve,
        "trades": trades,
        "days": days,
    }

    if verbose:
        print(f"  {'='*50}")
        print(f"  BACKTEST RESULTS")
        print(f"  {'='*50}")
        print(f"  {'Return:':<25} {ret_pct:>10.2f}%")
        print(f"  {'Total PnL:':<25} ${total_pnl:>10.2f}")
        print(f"  {'Final Equity:':<25} ${equity_curve[-1]:>10.2f}")
        print(f"  {'Sharpe Ratio:':<25} {sharpe:>10.2f}")
        print(f"  {'Max Drawdown:':<25} {dd_pct:>10.1f}%")
        print(f"  {'Win Rate:':<25} {win_rate:>10.1f}%")
        print(f"  {'Total Trades:':<25} {n_trades:>10}")
        print(f"  {'Winning Trades:':<25} {wins:>10}")
        print(f"  {'='*50}\n")

    return results


if __name__ == "__main__":
    run_backtest(days=365, capital=100.0, verbose=True)
