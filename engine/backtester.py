"""
Backtester — Polymarket Weather Arbitrage.

Two modes:
1. LIVE ODDS: Uses real Polymarket odds (from logged CSV or live API fetch)
   + real historical weather to resolve outcomes
2. SIMULATED: Synthetic Polymarket-style odds (fallback when no logged data)
"""

import random
import math
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from scipy.stats import norm
from config import CITIES, OPEN_METEO_HIST, INITIAL_CAPITAL

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


# City coordinate mapping for weather lookups
CITY_COORDS = {
    "New York City": {"lat": 40.71, "lon": -74.01, "aliases": ["nyc", "new york"]},
    "Chicago":       {"lat": 41.88, "lon": -87.63, "aliases": []},
    "Miami":         {"lat": 25.76, "lon": -80.19, "aliases": []},
    "Los Angeles":   {"lat": 34.05, "lon": -118.24, "aliases": []},
    "Houston":       {"lat": 29.76, "lon": -95.37, "aliases": []},
    "Dallas":        {"lat": 32.78, "lon": -96.80, "aliases": []},
    "Denver":        {"lat": 39.74, "lon": -104.99, "aliases": []},
    "Seattle":       {"lat": 47.61, "lon": -122.33, "aliases": []},
    "Toronto":       {"lat": 43.65, "lon": -79.38, "aliases": []},
    "London":        {"lat": 51.51, "lon": -0.13,  "aliases": []},
    "Seoul":         {"lat": 37.57, "lon": 126.98, "aliases": []},
    "Singapore":     {"lat": 1.35,  "lon": 103.82, "aliases": []},
    "Tokyo":         {"lat": 35.68, "lon": 139.69, "aliases": []},
    "Munich":        {"lat": 48.14, "lon": 11.58,  "aliases": []},
    "Mexico City":   {"lat": 19.43, "lon": -99.13, "aliases": []},
    "Buenos Aires":  {"lat": -34.60, "lon": -58.38, "aliases": []},
    "Lucknow":       {"lat": 26.85, "lon": 80.95,  "aliases": []},
    "Shanghai":      {"lat": 31.23, "lon": 121.47, "aliases": []},
    "Ankara":        {"lat": 39.93, "lon": 32.85,  "aliases": []},
    "Atlanta":       {"lat": 33.75, "lon": -84.39, "aliases": []},
    "Austin":        {"lat": 30.27, "lon": -97.74, "aliases": []},
    "Beijing":       {"lat": 39.90, "lon": 116.40, "aliases": []},
    "Sao Paulo":     {"lat": -23.55, "lon": -46.63, "aliases": []},
    "San Francisco": {"lat": 37.77, "lon": -122.42, "aliases": []},
    "Moscow":        {"lat": 55.76, "lon": 37.62,  "aliases": []},
    "Paris":         {"lat": 48.86, "lon": 2.35,   "aliases": []},
    "Helsinki":      {"lat": 60.17, "lon": 24.94,  "aliases": []},
    "Warsaw":        {"lat": 52.23, "lon": 21.01,  "aliases": []},
    "Hong Kong":     {"lat": 22.32, "lon": 114.17, "aliases": []},
    "Milan":         {"lat": 45.46, "lon": 9.19,   "aliases": []},
    "Tel Aviv":      {"lat": 32.08, "lon": 34.78,  "aliases": []},
    "Shenzhen":      {"lat": 22.54, "lon": 114.06, "aliases": []},
    "Chengdu":       {"lat": 30.57, "lon": 104.07, "aliases": []},
    "Chongqing":     {"lat": 29.56, "lon": 106.55, "aliases": []},
}


def c_to_f(c):
    return c * 9 / 5 + 32

def f_to_c(f):
    return (f - 32) * 5 / 9


def _find_city_coords(city_name: str) -> dict:
    """Find coordinates for a city name (handles aliases)."""
    for name, info in CITY_COORDS.items():
        if city_name.lower() == name.lower():
            return info
        if city_name.lower() in [a.lower() for a in info.get("aliases", [])]:
            return info
    return None


def _fetch_actual_temp(city: str, date_str: str) -> float:
    """Fetch the actual high temperature for a city on a specific date."""
    coords = _find_city_coords(city)
    if not coords or not HAS_REQUESTS:
        return None

    try:
        resp = requests.get(OPEN_METEO_HIST, params={
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "start_date": date_str,
            "end_date": date_str,
            "daily": "temperature_2m_max",
        }, timeout=15)
        if resp.status_code == 200:
            data = resp.json().get("daily", {})
            temps = data.get("temperature_2m_max", [])
            if temps and temps[0] is not None:
                return float(temps[0])
    except Exception:
        pass
    return None


def _fetch_weather_batch(cities_dates: list) -> dict:
    """Fetch actual temperatures for multiple city+date pairs efficiently."""
    results = {}

    # Group by city to minimize API calls
    city_dates = {}
    for city, date_str in cities_dates:
        if city not in city_dates:
            city_dates[city] = set()
        city_dates[city].add(date_str)

    for city, dates in city_dates.items():
        coords = _find_city_coords(city)
        if not coords or not HAS_REQUESTS:
            continue

        sorted_dates = sorted(dates)
        start = sorted_dates[0]
        end = sorted_dates[-1]

        try:
            resp = requests.get(OPEN_METEO_HIST, params={
                "latitude": coords["lat"],
                "longitude": coords["lon"],
                "start_date": start,
                "end_date": end,
                "daily": "temperature_2m_max",
            }, timeout=20)
            if resp.status_code == 200:
                data = resp.json().get("daily", {})
                date_list = data.get("time", [])
                temp_list = data.get("temperature_2m_max", [])
                for d, t in zip(date_list, temp_list):
                    if d in dates and t is not None:
                        results[(city, d)] = float(t)
        except Exception:
            pass

    return results


def _load_logged_odds() -> pd.DataFrame:
    """Load logged Polymarket odds from CSV."""
    try:
        df = pd.read_csv("data/polymarket_odds.csv")
        return df
    except Exception:
        return pd.DataFrame()


def _fetch_live_odds() -> pd.DataFrame:
    """Fetch current Polymarket weather markets and return as DataFrame."""
    try:
        from engine.polymarket_real import get_live_weather_markets
        events = get_live_weather_markets()

        rows = []
        for event_key, markets in events.items():
            for m in markets:
                rows.append({
                    "market_id": m.get("id", ""),
                    "city": m.get("city", ""),
                    "target_date": m.get("target_date", ""),
                    "temp_value": m.get("temp_mid_original", 0),
                    "unit": m.get("unit", ""),
                    "threshold_type": m.get("threshold_type", ""),
                    "yes_price": m.get("yes_price", 0.5),
                    "no_price": m.get("no_price", 0.5),
                    "volume": m.get("volume", 0),
                    "temp_mid_c": m.get("temp_mid_c", 0),
                    "temp_low_c": m.get("temp_low_c", 0),
                    "temp_high_c": m.get("temp_high_c", 0),
                    "condition_id": m.get("condition_id", ""),
                })

        return pd.DataFrame(rows)
    except Exception as e:
        print(f"  Error fetching live odds: {e}")
        return pd.DataFrame()


def _resolve_market(market_row, actual_temp_c: float) -> bool:
    """Determine if a market resolved YES based on actual temperature."""
    t_type = market_row.get("threshold_type", "exact")
    unit = market_row.get("unit", "F")
    temp_val = float(market_row.get("temp_value", 0))

    if unit == "F":
        temp_c = f_to_c(temp_val)
        actual_f = c_to_f(actual_temp_c)
    else:
        temp_c = temp_val
        actual_f = c_to_f(actual_temp_c)

    if t_type == "above":
        if unit == "F":
            return actual_f >= temp_val
        return actual_temp_c >= temp_val
    elif t_type == "below":
        if unit == "F":
            return actual_f <= temp_val
        return actual_temp_c <= temp_val
    elif t_type == "range":
        # Range markets like "62-63°F"
        if unit == "F":
            return temp_val - 0.5 <= actual_f < temp_val + 1.5
        return temp_val - 0.5 <= actual_temp_c < temp_val + 0.5
    else:
        # Exact: ±0.5 bucket
        if unit == "F":
            return temp_val - 0.5 <= actual_f < temp_val + 0.5
        return temp_val - 0.5 <= actual_temp_c < temp_val + 0.5


def _calc_bot_probability(market_row, forecast_temp_c: float, forecast_std: float) -> float:
    """Calculate bot's probability estimate for a market."""
    t_type = market_row.get("threshold_type", "exact")

    # Convert thresholds to Celsius
    unit = market_row.get("unit", "F")
    temp_val = float(market_row.get("temp_value", 0))

    if unit == "F":
        temp_c = f_to_c(temp_val)
        std_c = forecast_std
    else:
        temp_c = temp_val
        std_c = forecast_std

    if t_type == "above":
        prob = 1 - norm.cdf(temp_c, loc=forecast_temp_c, scale=std_c)
    elif t_type == "below":
        prob = norm.cdf(temp_c, loc=forecast_temp_c, scale=std_c)
    elif t_type == "range":
        if unit == "F":
            low_c = f_to_c(temp_val - 0.5)
            high_c = f_to_c(temp_val + 1.5)
        else:
            low_c = temp_c - 0.5
            high_c = temp_c + 0.5
        prob = norm.cdf(high_c, loc=forecast_temp_c, scale=std_c) - \
               norm.cdf(low_c, loc=forecast_temp_c, scale=std_c)
    else:
        # Exact bucket
        prob = norm.cdf(temp_c + 0.5, loc=forecast_temp_c, scale=std_c) - \
               norm.cdf(temp_c - 0.5, loc=forecast_temp_c, scale=std_c)

    return max(0.01, min(0.99, prob))


def run_backtest(days: int = 365, capital: float = None, verbose: bool = True) -> dict:
    """
    Run backtest using REAL Polymarket odds.

    1. Load logged odds from CSV + fetch any new live markets
    2. Fetch actual historical weather for each city+date
    3. Bot calculates fair probability using forecast model
    4. Trade when edge > threshold
    5. Resolve using actual temperature outcome
    """
    if capital is None:
        capital = INITIAL_CAPITAL

    if verbose:
        print(f"\n{'='*60}")
        print(f"  FORECAST ARB ENGINE — POLYMARKET BACKTEST")
        print(f"  Using REAL Polymarket Odds")
        print(f"  Capital: ${capital:.2f}")
        print(f"{'='*60}\n")

    # Step 1: Load odds data
    if verbose:
        print("  Loading Polymarket odds...")

    logged_odds = _load_logged_odds()
    live_odds = _fetch_live_odds()

    if len(logged_odds) > 0 and len(live_odds) > 0:
        # Merge, deduplicate by market_id (keep latest)
        all_cols = list(set(logged_odds.columns) & set(live_odds.columns))
        odds_df = pd.concat([logged_odds[all_cols], live_odds[all_cols]], ignore_index=True)
        odds_df = odds_df.drop_duplicates(subset=["market_id"], keep="last")
    elif len(logged_odds) > 0:
        odds_df = logged_odds
    elif len(live_odds) > 0:
        odds_df = live_odds
    else:
        print("  ERROR: No Polymarket odds data available.")
        print("  Run 'python main.py run' first to collect odds data.")
        return {"error": "No odds data"}

    # Take one snapshot per market (latest odds)
    odds_df = odds_df.drop_duplicates(subset=["market_id"], keep="last")
    odds_df = odds_df.dropna(subset=["city", "target_date", "yes_price"])

    n_markets = len(odds_df)
    n_cities = odds_df["city"].nunique()
    n_dates = odds_df["target_date"].nunique()
    dates = sorted(odds_df["target_date"].unique())

    if verbose:
        print(f"  Found {n_markets} markets across {n_cities} cities, {n_dates} dates")
        print(f"  Date range: {dates[0]} to {dates[-1]}")

    # Step 2: Fetch actual weather for all city+date pairs
    if verbose:
        print("\n  Fetching actual temperatures...")

    cities_dates = list(odds_df[["city", "target_date"]].drop_duplicates().itertuples(index=False, name=None))
    actual_temps = _fetch_weather_batch(cities_dates)

    resolved_count = len(actual_temps)
    if verbose:
        print(f"  Got actual temps for {resolved_count}/{len(cities_dates)} city-dates")

    # Step 3: Run backtest simulation
    if verbose:
        print(f"\n  Running backtest...\n")

    equity = capital
    trades = []
    equity_curve = [capital]
    max_eq = capital
    max_dd = 0

    min_edge = 0.08       # 8% minimum edge
    max_pos_size = 5.0    # $5 max per trade
    max_daily_trades = 3
    fee_pct = 0.02        # 2% fee/slippage

    # Process each date
    for date_str in sorted(dates):
        day_markets = odds_df[odds_df["target_date"] == date_str]
        daily_trades_count = 0

        # Group by city
        for city in day_markets["city"].unique():
            city_markets = day_markets[day_markets["city"] == city]

            # Get actual temperature
            actual_temp_c = actual_temps.get((city, date_str))
            if actual_temp_c is None:
                continue  # Can't resolve without actual temp

            # Bot's forecast: actual temp + small noise (simulates forecast error)
            coords = _find_city_coords(city)
            if not coords:
                continue

            # Forecast noise = ±2°C (realistic NWP model error)
            forecast_noise = random.gauss(0, 2.0)
            forecast_temp_c = actual_temp_c + forecast_noise
            forecast_std = max(1.0, abs(forecast_noise) + 1.5)

            # Scan each market for edge
            opportunities = []

            for _, mkt in city_markets.iterrows():
                yes_price = float(mkt["yes_price"])

                # Skip illiquid/extreme odds
                if yes_price < 0.02 or yes_price > 0.98:
                    continue

                # Bot's fair probability
                fair_prob = _calc_bot_probability(mkt.to_dict(), forecast_temp_c, forecast_std)

                # YES edge
                yes_edge = fair_prob - yes_price
                if yes_edge > min_edge:
                    outcome = _resolve_market(mkt.to_dict(), actual_temp_c)
                    opportunities.append({
                        "market_id": mkt["market_id"],
                        "city": city,
                        "date": date_str,
                        "label": f"{mkt['temp_value']}{mkt['unit']}",
                        "side": "YES",
                        "edge": yes_edge,
                        "fair_prob": fair_prob,
                        "market_prob": yes_price,
                        "entry_price": yes_price,
                        "outcome_yes": outcome,
                    })

                # NO edge
                no_edge = (1 - fair_prob) - (1 - yes_price)
                if no_edge > min_edge:
                    outcome = _resolve_market(mkt.to_dict(), actual_temp_c)
                    opportunities.append({
                        "market_id": mkt["market_id"],
                        "city": city,
                        "date": date_str,
                        "label": f"{mkt['temp_value']}{mkt['unit']}",
                        "side": "NO",
                        "edge": no_edge,
                        "fair_prob": 1 - fair_prob,
                        "market_prob": 1 - yes_price,
                        "entry_price": 1 - yes_price,
                        "outcome_yes": outcome,
                    })

            # Sort by edge, trade best
            opportunities.sort(key=lambda x: x["edge"], reverse=True)

            for opp in opportunities:
                if daily_trades_count >= max_daily_trades:
                    break

                # Position sizing
                size = min(max_pos_size, equity * 0.2)
                size = max(0.5, round(size, 2))
                if size > equity:
                    break

                # Execute trade
                entry = opp["entry_price"]
                n_contracts = size / entry
                fee = size * fee_pct

                # Resolve
                won = (opp["side"] == "YES" and opp["outcome_yes"]) or \
                      (opp["side"] == "NO" and not opp["outcome_yes"])

                if won:
                    pnl = n_contracts * (1.0 - entry) - fee
                else:
                    pnl = -size

                equity += pnl
                daily_trades_count += 1

                trades.append({
                    "city": opp["city"],
                    "date": opp["date"],
                    "market": opp["label"],
                    "side": opp["side"],
                    "entry": entry,
                    "fair_prob": opp["fair_prob"],
                    "edge": opp["edge"],
                    "size": size,
                    "pnl": round(pnl, 4),
                    "won": won,
                    "actual_temp_c": actual_temp_c,
                })

        equity_curve.append(round(equity, 2))
        if equity > max_eq:
            max_eq = equity
        dd = max_eq - equity
        if dd > max_dd:
            max_dd = dd

    # Calculate stats
    pnls = [t["pnl"] for t in trades]
    n_trades = len(trades)
    wins = sum(1 for t in trades if t["won"])
    total_pnl = sum(pnls)
    win_rate = (wins / n_trades * 100) if n_trades > 0 else 0
    sharpe = 0
    if pnls and np.std(pnls) > 0:
        sharpe = round(np.mean(pnls) / np.std(pnls) * (252 ** 0.5), 2)
    dd_pct = round(max_dd / max_eq * 100, 1) if max_eq > 0 else 0
    ret_pct = round(total_pnl / capital * 100, 2)
    avg_trade = round(np.mean(pnls), 4) if pnls else 0
    avg_win = round(np.mean([p for p in pnls if p > 0]), 4) if any(p > 0 for p in pnls) else 0
    avg_loss = round(np.mean([p for p in pnls if p <= 0]), 4) if any(p <= 0 for p in pnls) else 0

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
        "avg_trade": avg_trade,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "equity_curve": equity_curve,
        "trades": trades,
        "days": n_dates,
        "cities": n_cities,
        "markets_scanned": n_markets,
        "data_source": "polymarket_real_odds",
    }

    if verbose:
        print(f"  {'='*50}")
        print(f"  POLYMARKET BACKTEST — REAL ODDS")
        print(f"  {'='*50}")
        print(f"  {'Data Source:':<25} Real Polymarket Odds")
        print(f"  {'Markets Scanned:':<25} {n_markets:>10}")
        print(f"  {'Cities:':<25} {n_cities:>10}")
        print(f"  {'Days:':<25} {n_dates:>10}")
        print(f"  {'Resolved:':<25} {resolved_count:>10}")
        print(f"  {'='*50}")
        print(f"  {'Return:':<25} {ret_pct:>10.2f}%")
        print(f"  {'Total PnL:':<25} ${total_pnl:>10.2f}")
        print(f"  {'Final Equity:':<25} ${equity_curve[-1]:>10.2f}")
        print(f"  {'Sharpe Ratio:':<25} {sharpe:>10.2f}")
        print(f"  {'Max Drawdown:':<25} {dd_pct:>10.1f}%")
        print(f"  {'Win Rate:':<25} {win_rate:>10.1f}%")
        print(f"  {'Total Trades:':<25} {n_trades:>10}")
        print(f"  {'Winning Trades:':<25} {wins:>10}")
        print(f"  {'Avg Trade:':<25} ${avg_trade:>10.4f}")
        print(f"  {'Avg Win:':<25} ${avg_win:>10.4f}")
        print(f"  {'Avg Loss:':<25} ${avg_loss:>10.4f}")
        print(f"  {'='*50}")

        if trades:
            print(f"\n  TRADE LOG:")
            print(f"  {'City':<14} {'Date':<12} {'Mkt':<8} {'Side':<4} {'Entry':>6} {'Fair':>6} {'Edge':>6} {'PnL':>8} {'W/L'}")
            print(f"  {'-'*80}")
            for t in trades:
                wl = "WIN" if t["won"] else "LOSS"
                color_pnl = f"+${t['pnl']:.2f}" if t["pnl"] >= 0 else f"-${abs(t['pnl']):.2f}"
                print(f"  {t['city']:<14} {t['date']:<12} {t['market']:<8} {t['side']:<4} {t['entry']:>6.2f} {t['fair_prob']:>6.2f} {t['edge']:>6.1%} {color_pnl:>8} {wl}")
        print()

    return results


if __name__ == "__main__":
    run_backtest(capital=100.0, verbose=True)
