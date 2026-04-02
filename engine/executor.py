"""
Trade Executor — handles order execution in dry-run mode.
Tracks positions, PnL, equity curve, and trade history.
"""

import uuid
from datetime import datetime
from config import MAX_POSITIONS


class Trade:
    def __init__(self, contract_name, side, size, entry_price, confidence):
        self.id = str(uuid.uuid4())[:8]
        self.contract_name = contract_name
        self.side = side
        self.size = size
        self.entry_price = entry_price
        self.exit_price = None
        self.pnl = 0.0
        self.confidence = confidence
        self.target_price = min(0.95, entry_price * 1.3)
        self.open_time = datetime.utcnow()
        self.close_time = None
        self.status = "open"

    def close(self, exit_price):
        self.exit_price = exit_price
        self.close_time = datetime.utcnow()
        self.status = "closed"

        # PnL: bought N contracts at entry_price, sold at exit_price
        n_contracts = self.size / self.entry_price
        self.pnl = round(n_contracts * (self.exit_price - self.entry_price), 2)

    def resolve(self, outcome_won: bool):
        """Resolve based on contract outcome."""
        self.close_time = datetime.utcnow()
        self.status = "resolved"
        n_contracts = self.size / self.entry_price
        if outcome_won:
            self.exit_price = 1.0
            self.pnl = round(n_contracts * (1.0 - self.entry_price), 2)
        else:
            self.exit_price = 0.0
            self.pnl = round(-self.size, 2)

    def to_dict(self):
        return {
            "id": self.id,
            "contract": self.contract_name,
            "side": self.side,
            "size": self.size,
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "pnl": self.pnl,
            "target_price": self.target_price,
            "status": self.status,
            "open_time": self.open_time.strftime("%H:%M:%S"),
            "close_time": self.close_time.strftime("%H:%M:%S") if self.close_time else None,
        }


class Executor:
    """Manages trade execution and portfolio state."""

    def __init__(self, initial_capital: float):
        self.initial_capital = initial_capital
        self.capital = initial_capital
        self.positions = {}      # contract_name -> Trade
        self.trade_history = []  # Closed trades
        self.equity_curve = [(datetime.utcnow().isoformat(), initial_capital)]
        self.total_pnl = 0.0
        self.total_trades = 0
        self.winning_trades = 0
        self.today_trades = 0
        self.today_pnl = 0.0
        self.max_equity = initial_capital
        self.max_drawdown = 0.0
        self.daily_var = 0.0
        self._pnl_history = []

    @property
    def exposure(self) -> float:
        return sum(t.size for t in self.positions.values())

    @property
    def exposure_pct(self) -> float:
        return (self.exposure / self.capital * 100) if self.capital > 0 else 0

    @property
    def win_rate(self) -> float:
        return (self.winning_trades / self.total_trades * 100) if self.total_trades > 0 else 0

    @property
    def sharpe(self) -> float:
        if len(self._pnl_history) < 2:
            return 0
        import numpy as np
        returns = np.array(self._pnl_history)
        if np.std(returns) == 0:
            return 0
        return round(np.mean(returns) / np.std(returns) * (252 ** 0.5), 2)

    @property
    def drawdown_pct(self) -> float:
        return round(self.max_drawdown / self.max_equity * 100, 1) if self.max_equity > 0 else 0

    def open_trade(self, signal) -> Trade:
        """Open a new trade from a signal."""
        if signal.contract_name in self.positions:
            return None
        if len(self.positions) >= MAX_POSITIONS:
            return None
        if signal.size > self.capital * 0.5:
            return None

        trade = Trade(
            contract_name=signal.contract_name,
            side=signal.side,
            size=signal.size,
            entry_price=signal.market_prob,
            confidence=signal.confidence,
        )

        self.positions[signal.contract_name] = trade
        self.capital -= signal.size
        self.today_trades += 1
        return trade

    def close_trade(self, contract_name: str, exit_price: float) -> Trade:
        """Close an existing trade."""
        if contract_name not in self.positions:
            return None

        trade = self.positions.pop(contract_name)
        trade.close(exit_price)

        self.capital += trade.size + trade.pnl
        self.total_pnl += trade.pnl
        self.today_pnl += trade.pnl
        self.total_trades += 1
        if trade.pnl > 0:
            self.winning_trades += 1

        self._pnl_history.append(trade.pnl)
        self.trade_history.append(trade)
        self._update_equity()

        return trade

    def resolve_trade(self, contract_name: str, outcome_won: bool) -> Trade:
        """Resolve a trade based on contract outcome."""
        if contract_name not in self.positions:
            return None

        trade = self.positions.pop(contract_name)
        trade.resolve(outcome_won)

        self.capital += trade.size + trade.pnl
        self.total_pnl += trade.pnl
        self.today_pnl += trade.pnl
        self.total_trades += 1
        if trade.pnl > 0:
            self.winning_trades += 1

        self._pnl_history.append(trade.pnl)
        self.trade_history.append(trade)
        self._update_equity()

        return trade

    def _update_equity(self):
        equity = self.capital + self.exposure
        self.equity_curve.append((datetime.utcnow().isoformat(), round(equity, 2)))
        if equity > self.max_equity:
            self.max_equity = equity
        dd = self.max_equity - equity
        if dd > self.max_drawdown:
            self.max_drawdown = dd

        # Daily VaR (simple)
        if len(self._pnl_history) >= 5:
            import numpy as np
            self.daily_var = round(abs(np.percentile(self._pnl_history, 5)), 2)

    def get_state(self) -> dict:
        """Full portfolio state for dashboard."""
        return {
            "capital": round(self.capital, 2),
            "total_pnl": round(self.total_pnl, 2),
            "today_pnl": round(self.today_pnl, 2),
            "today_trades": self.today_trades,
            "win_rate": round(self.win_rate, 1),
            "sharpe": self.sharpe,
            "exposure": round(self.exposure_pct, 1),
            "drawdown": self.drawdown_pct,
            "daily_var": round(self.daily_var / self.initial_capital * 100, 1) if self.daily_var else 0,
            "total_trades": self.total_trades,
            "positions": {k: v.to_dict() for k, v in self.positions.items()},
            "recent_trades": [t.to_dict() for t in self.trade_history[-10:]],
            "equity_curve": self.equity_curve[-200:],
        }

    def reset_daily(self):
        self.today_trades = 0
        self.today_pnl = 0.0
