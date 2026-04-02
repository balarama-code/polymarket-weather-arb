"""
Market simulation + Polymarket integration.
Creates weather derivative contracts and manages market odds.
"""

import random
import math
import numpy as np
from datetime import datetime
from config import CONTRACT_TYPES, POLYMARKET_GAMMA_API


class WeatherContract:
    """A single weather prediction market contract."""

    def __init__(self, name, city, metric, threshold, direction, category):
        self.name = name
        self.city = city
        self.metric = metric
        self.threshold = threshold
        self.direction = direction  # "above" or "below"
        self.category = category
        self.yes_price = 0.50  # Initial odds
        self.no_price = 0.50
        self.volume = 0
        self.last_update = datetime.utcnow()
        self.resolved = False
        self.outcome = None  # True=YES wins, False=NO wins

    def update_market_odds(self, forecast_value: float, noise: float = 0.05):
        """
        Update market odds based on forecast value.
        Adds noise/lag to simulate slow market.
        """
        if self.direction == "above":
            # How far above threshold?
            delta = (forecast_value - self.threshold) / max(abs(self.threshold), 1)
        else:
            delta = (self.threshold - forecast_value) / max(abs(self.threshold), 1)

        # Convert delta to probability using sigmoid
        raw_prob = 1 / (1 + math.exp(-delta * 3))

        # Add market noise (simulates slow/inefficient market)
        noisy_prob = raw_prob + random.gauss(0, noise)
        noisy_prob = max(0.02, min(0.98, noisy_prob))

        self.yes_price = round(noisy_prob, 4)
        self.no_price = round(1 - noisy_prob, 4)
        self.last_update = datetime.utcnow()

    def calc_true_probability(self, model_forecasts: dict) -> float:
        """
        Calculate 'true' probability from model consensus.
        model_forecasts: {model_name: metric_value}
        """
        if not model_forecasts:
            return 0.5

        values = list(model_forecasts.values())
        mean_val = np.mean(values)
        std_val = np.std(values) if len(values) > 1 else 1.0

        if self.direction == "above":
            delta = (mean_val - self.threshold) / max(std_val, 0.1)
        else:
            delta = (self.threshold - mean_val) / max(std_val, 0.1)

        # More confident sigmoid with model consensus
        clamped = max(-500, min(500, -delta * 2))
        prob = 1 / (1 + math.exp(clamped))
        return max(0.02, min(0.98, prob))

    def resolve(self, actual_value: float):
        """Resolve the contract based on actual outcome."""
        if self.direction == "above":
            self.outcome = actual_value > self.threshold
        else:
            self.outcome = actual_value < self.threshold
        self.resolved = True

    def to_dict(self):
        return {
            "name": self.name,
            "city": self.city,
            "metric": self.metric,
            "threshold": self.threshold,
            "direction": self.direction,
            "category": self.category,
            "yes_price": self.yes_price,
            "no_price": self.no_price,
            "volume": self.volume,
            "resolved": self.resolved,
            "outcome": self.outcome,
        }


class MarketSimulator:
    """Manages a set of weather contracts."""

    def __init__(self):
        self.contracts = {}
        self._init_contracts()

    def _init_contracts(self):
        for ct in CONTRACT_TYPES:
            contract = WeatherContract(
                name=ct["name"],
                city=ct["city"],
                metric=ct["metric"],
                threshold=ct["threshold"],
                direction=ct["direction"],
                category=ct["category"],
            )
            self.contracts[ct["name"]] = contract

    def update_all_odds(self, forecasts: dict, noise: float = 0.08):
        """Update all contract odds from forecast data (with market lag/noise)."""
        for name, contract in self.contracts.items():
            city_data = forecasts.get(contract.city, {})
            if not city_data:
                continue

            # Use first available model for market update (simulates slow market)
            first_model = list(city_data.values())[0] if city_data else {}
            val = first_model.get(contract.metric)
            if val is not None:
                contract.update_market_odds(float(val), noise=noise)

    def get_all_contracts(self) -> list:
        return [c.to_dict() for c in self.contracts.values()]

    def resolve_contracts(self, actual_weather: dict):
        """Resolve all contracts based on actual weather data."""
        for name, contract in self.contracts.items():
            actual = actual_weather.get(contract.city, {}).get(contract.metric)
            if actual is not None:
                contract.resolve(float(actual))
