"""
Forecast Spread Arbitrage Strategy.
Compares weather model consensus vs market odds.
Fires trades when edge > threshold.
"""

import math
import numpy as np
from datetime import datetime
from config import MIN_EDGE, KELLY_FRACTION, MAX_POSITION_SIZE, MAX_EXPOSURE


class Signal:
    """A trading signal from the strategy."""
    def __init__(self, contract_name, side, edge, true_prob, market_prob, size, confidence):
        self.contract_name = contract_name
        self.side = side              # "YES" or "NO"
        self.edge = edge              # true_prob - market_prob
        self.true_prob = true_prob    # Model consensus probability
        self.market_prob = market_prob # Current market price
        self.size = size              # Position size in USD
        self.confidence = confidence  # Model confidence 0-1
        self.timestamp = datetime.utcnow()

    def to_dict(self):
        return {
            "contract": self.contract_name,
            "side": self.side,
            "edge": round(self.edge, 4),
            "true_prob": round(self.true_prob, 4),
            "market_prob": round(self.market_prob, 4),
            "size": round(self.size, 2),
            "confidence": round(self.confidence, 2),
            "time": self.timestamp.strftime("%H:%M:%S"),
        }


class ForecastArbStrategy:
    """
    Core strategy: compare model consensus vs market odds.
    Trade when models disagree with market by > MIN_EDGE.
    """

    def __init__(self, capital: float):
        self.capital = capital
        self.min_edge = MIN_EDGE
        self.kelly_fraction = KELLY_FRACTION
        self.max_position = MAX_POSITION_SIZE
        self.max_exposure = MAX_EXPOSURE

    def scan_opportunities(self, contracts: dict, forecasts: dict, current_exposure: float) -> list:
        """
        Scan all contracts for arbitrage opportunities.
        Returns list of Signal objects.
        """
        signals = []

        for name, contract in contracts.items():
            # Get model forecasts for this contract's metric
            city_forecasts = forecasts.get(contract.city, {})
            model_values = {}
            for model_key, data in city_forecasts.items():
                val = data.get(contract.metric)
                if val is not None:
                    model_values[model_key] = float(val)

            if not model_values:
                continue

            # Calculate true probability from model consensus
            true_prob = contract.calc_true_probability(model_values)

            # Check for edge on YES side
            yes_edge = true_prob - contract.yes_price
            no_edge = (1 - true_prob) - contract.no_price

            # Model confidence based on agreement
            values = list(model_values.values())
            if len(values) > 1:
                cv = np.std(values) / (abs(np.mean(values)) + 0.001)
                confidence = max(0.3, 1.0 - cv)
            else:
                confidence = 0.5

            # Check YES side
            if yes_edge > self.min_edge:
                size = self._calc_position_size(true_prob, contract.yes_price, confidence, current_exposure)
                if size > 0:
                    signals.append(Signal(
                        contract_name=name,
                        side="YES",
                        edge=yes_edge,
                        true_prob=true_prob,
                        market_prob=contract.yes_price,
                        size=size,
                        confidence=confidence,
                    ))

            # Check NO side
            elif no_edge > self.min_edge:
                size = self._calc_position_size(1 - true_prob, contract.no_price, confidence, current_exposure)
                if size > 0:
                    signals.append(Signal(
                        contract_name=name,
                        side="NO",
                        edge=no_edge,
                        true_prob=1 - true_prob,
                        market_prob=contract.no_price,
                        size=size,
                        confidence=confidence,
                    ))

        # Sort by edge (best first)
        signals.sort(key=lambda s: s.edge, reverse=True)
        return signals

    def _calc_position_size(self, win_prob: float, entry_price: float,
                            confidence: float, current_exposure: float) -> float:
        """
        Kelly criterion position sizing.
        f* = (bp - q) / b where b=odds, p=win_prob, q=1-p
        """
        if entry_price <= 0.01 or entry_price >= 0.99:
            return 0

        # Odds (payout ratio if we win)
        b = (1.0 / entry_price) - 1.0
        if b <= 0:
            return 0

        p = win_prob
        q = 1 - p

        kelly = (b * p - q) / b
        if kelly <= 0:
            return 0

        # Apply fractional Kelly + confidence scaling
        size = self.capital * kelly * self.kelly_fraction * confidence

        # Cap at max position
        size = min(size, self.max_position)

        # Check exposure limit
        remaining_exposure = (self.max_exposure * self.capital) - current_exposure
        if remaining_exposure <= 0:
            return 0
        size = min(size, remaining_exposure)

        return max(0, round(size, 2))

    def check_exit(self, position: dict, contract) -> bool:
        """
        Check if a position should be closed.
        Exit on convergence (market moved to our fair value).
        """
        if position["side"] == "YES":
            current_price = contract.yes_price
            entry_price = position["entry_price"]
            # Exit if price converged (market caught up) or moved against us
            if current_price >= position.get("target_price", entry_price * 1.3):
                return True
            if current_price < entry_price * 0.7:  # Stop loss
                return True
        else:
            current_price = contract.no_price
            entry_price = position["entry_price"]
            if current_price >= position.get("target_price", entry_price * 1.3):
                return True
            if current_price < entry_price * 0.7:
                return True

        return False
