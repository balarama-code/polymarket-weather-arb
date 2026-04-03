"""
Real Polymarket Weather Markets Client.
Fetches live temperature/weather contracts from Polymarket Gamma + CLOB API.
Groups markets by city+date event.
"""

import re
import requests
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional

GAMMA_API = "https://gamma-api.polymarket.com"


def fetch_weather_markets(limit: int = 2000) -> List[dict]:
    """
    Fetch all active weather/temperature markets from Polymarket.
    Returns list of market dicts with question, prices, volume, slug, id, etc.
    """
    all_markets = []
    offset = 0
    page_size = 100
    seen_ids = set()

    while offset < limit:
        try:
            resp = requests.get(f"{GAMMA_API}/markets", params={
                "limit": page_size,
                "offset": offset,
                "active": "true",
                "closed": "false",
                "order": "volume",
                "ascending": "false",
            }, timeout=15)

            if resp.status_code != 200:
                break

            markets = resp.json()
            if not markets:
                break

            for m in markets:
                mid = m.get("id")
                if mid in seen_ids:
                    continue
                seen_ids.add(mid)

                q = m.get("question", "").lower()
                if any(kw in q for kw in [
                    "temperature", "highest temp", "snowfall",
                    "hurricane", "tornado", "precipitation",
                    "rainfall", "wind speed", "heatwave",
                ]):
                    all_markets.append(m)

            offset += page_size

            # If we got fewer than page_size, we've reached the end
            if len(markets) < page_size:
                break

        except Exception:
            break

    return all_markets


def parse_market(market: dict) -> Optional[dict]:
    """
    Parse a Polymarket weather market into structured data.
    Example question: "Will the highest temperature in NYC be 48-49°F on April 4?"
    """
    question = market.get("question", "")
    slug = market.get("slug", "")

    # Extract city
    city_match = re.search(r"temperature in (.+?) (?:be|on)", question, re.IGNORECASE)
    city = city_match.group(1).strip() if city_match else None

    # Extract temperature value/range
    temp_match = re.search(r"(?:be\s+(?:between\s+)?)([\d.]+(?:-[\d.]+)?)\s*°?\s*([FC°])", question, re.IGNORECASE)
    if not temp_match:
        temp_match = re.search(r"be\s+([\d.]+(?:-[\d.]+)?)\s*°?\s*([FC°])", question, re.IGNORECASE)
    if not temp_match:
        # Try "58°F or higher" pattern
        temp_match = re.search(r"([\d.]+)\s*°?\s*([FC])\s+or\s+(higher|lower)", question, re.IGNORECASE)

    if temp_match:
        temp_str = temp_match.group(1)
        unit = temp_match.group(2).upper()

        # Handle range like "48-49"
        if "-" in temp_str:
            parts = temp_str.split("-")
            temp_low = float(parts[0])
            temp_high = float(parts[1])
            temp_mid = (temp_low + temp_high) / 2
        else:
            temp_mid = float(temp_str)
            temp_low = temp_mid
            temp_high = temp_mid

        # Convert F to C for internal consistency
        if unit == "F":
            temp_mid_c = (temp_mid - 32) * 5 / 9
            temp_low_c = (temp_low - 32) * 5 / 9
            temp_high_c = (temp_high - 32) * 5 / 9
        else:
            temp_mid_c = temp_mid
            temp_low_c = temp_low
            temp_high_c = temp_high
    else:
        return None

    # Extract date
    date_match = re.search(r"on\s+(\w+\s+\d+)", question, re.IGNORECASE)
    if date_match:
        date_str = date_match.group(1)
        try:
            # Assume current year
            year = datetime.utcnow().year
            target_date = datetime.strptime(f"{date_str} {year}", "%B %d %Y")
        except ValueError:
            try:
                target_date = datetime.strptime(f"{date_str} {year}", "%b %d %Y")
            except ValueError:
                target_date = None
    else:
        target_date = None

    # Parse prices
    outcomes = market.get("outcomes", '["Yes","No"]')
    if isinstance(outcomes, str):
        outcomes = eval(outcomes)
    prices_raw = market.get("outcomePrices", '["0.5","0.5"]')
    if isinstance(prices_raw, str):
        prices_raw = eval(prices_raw)

    yes_price = float(prices_raw[0]) if len(prices_raw) > 0 else 0.5
    no_price = float(prices_raw[1]) if len(prices_raw) > 1 else 0.5

    # "or higher" / "or lower" modifier
    threshold_type = "exact"
    if "or higher" in question.lower():
        threshold_type = "above"
    elif "or lower" in question.lower() or "or below" in question.lower():
        threshold_type = "below"
    elif "-" in temp_str:
        threshold_type = "range"

    return {
        "id": market.get("id"),
        "question": question,
        "slug": slug,
        "city": city,
        "temp_mid_c": round(temp_mid_c, 1),
        "temp_low_c": round(temp_low_c, 1),
        "temp_high_c": round(temp_high_c, 1),
        "temp_mid_original": temp_mid,
        "unit": unit,
        "threshold_type": threshold_type,
        "target_date": target_date.strftime("%Y-%m-%d") if target_date else None,
        "yes_price": yes_price,
        "no_price": no_price,
        "volume": float(market.get("volume", 0)),
        "liquidity": float(market.get("liquidity", 0)),
        "condition_id": market.get("conditionId", ""),
        "active": market.get("active", False),
    }


def group_by_event(parsed_markets: List[dict]) -> Dict[str, List[dict]]:
    """
    Group markets by city+date (= one event).
    Returns {event_key: [market1, market2, ...]}
    """
    events = {}
    for m in parsed_markets:
        if not m or not m.get("city") or not m.get("target_date"):
            continue
        key = f"{m['city']}|{m['target_date']}"
        if key not in events:
            events[key] = []
        events[key].append(m)

    # Sort each event's markets by temperature
    for key in events:
        events[key].sort(key=lambda x: x["temp_mid_c"])

    return events


def get_live_weather_markets() -> Dict[str, List[dict]]:
    """
    Full pipeline: fetch → parse → group.
    Returns grouped events with parsed market data.
    """
    raw_markets = fetch_weather_markets(limit=500)
    parsed = [parse_market(m) for m in raw_markets]
    parsed = [p for p in parsed if p is not None]
    events = group_by_event(parsed)
    return events


def calc_forecast_edge(market: dict, forecast_temp_c: float, forecast_std: float = 2.0) -> dict:
    """
    Calculate edge: compare weather forecast vs market odds.

    For 'exact' or 'range' markets: probability that actual temp falls in that range.
    For 'above' markets: probability that actual temp >= threshold.
    For 'below' markets: probability that actual temp <= threshold.

    Uses normal distribution assumption.
    """
    from scipy.stats import norm

    t_type = market["threshold_type"]
    yes_price = market["yes_price"]

    if t_type == "above":
        # P(temp >= threshold)
        true_prob = 1 - norm.cdf(market["temp_low_c"], loc=forecast_temp_c, scale=forecast_std)
    elif t_type == "below":
        # P(temp <= threshold)
        true_prob = norm.cdf(market["temp_high_c"], loc=forecast_temp_c, scale=forecast_std)
    elif t_type == "range":
        # P(temp_low <= temp <= temp_high)
        true_prob = norm.cdf(market["temp_high_c"], loc=forecast_temp_c, scale=forecast_std) - \
                    norm.cdf(market["temp_low_c"], loc=forecast_temp_c, scale=forecast_std)
    else:
        # Exact: treat as ±0.5 range
        true_prob = norm.cdf(market["temp_mid_c"] + 0.5, loc=forecast_temp_c, scale=forecast_std) - \
                    norm.cdf(market["temp_mid_c"] - 0.5, loc=forecast_temp_c, scale=forecast_std)

    true_prob = max(0.01, min(0.99, true_prob))
    edge = true_prob - yes_price

    return {
        "market_id": market["id"],
        "question": market["question"],
        "city": market["city"],
        "date": market["target_date"],
        "true_prob": round(true_prob, 4),
        "market_prob": yes_price,
        "edge": round(edge, 4),
        "side": "YES" if edge > 0 else "NO",
        "abs_edge": round(abs(edge), 4),
        "forecast_temp_c": forecast_temp_c,
        "forecast_std": forecast_std,
    }


if __name__ == "__main__":
    print("Fetching live Polymarket weather markets...\n")
    events = get_live_weather_markets()
    print(f"Found {len(events)} city-date events\n")

    for key, markets in list(events.items())[:5]:
        city, date = key.split("|")
        print(f"  {city} — {date} ({len(markets)} markets)")
        for m in markets[:4]:
            print(f"    {m['temp_mid_original']}{m['unit']}: YES={m['yes_price']:.2%}  "
                  f"Vol=${m['volume']:,.0f}  [{m['threshold_type']}]")
        print()
