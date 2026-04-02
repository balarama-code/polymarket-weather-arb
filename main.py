"""
Forecast Arb Engine v7.3
Weather forecast arbitrage bot for Polymarket.
Modes: backtest, dry-run dashboard.

Usage:
    python main.py backtest          # Run backtest on historical data
    python main.py run               # Start dry-run with web dashboard
    python main.py backtest --days 180  # Custom backtest period
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))


def cmd_backtest(days: int = 365):
    from engine.backtester import run_backtest
    from config import INITIAL_CAPITAL

    results = run_backtest(days=days, capital=INITIAL_CAPITAL, verbose=True)

    # Save results
    import json
    os.makedirs("data", exist_ok=True)
    with open("data/backtest_results.json", "w") as f:
        # Don't save full equity curve to keep file small
        save = {k: v for k, v in results.items() if k != "equity_curve"}
        save["equity_curve_len"] = len(results.get("equity_curve", []))
        json.dump(save, f, indent=2, default=str)

    print(f"  Results saved to data/backtest_results.json")
    return results


def cmd_run(host="127.0.0.1", port=5050):
    from dashboard.app import start_dashboard
    start_dashboard(host=host, port=port)


def main():
    args = sys.argv[1:]

    if not args or args[0] == "backtest":
        days = 365
        if "--days" in args:
            idx = args.index("--days")
            if idx + 1 < len(args):
                days = int(args[idx + 1])
        cmd_backtest(days)

    elif args[0] == "run":
        host = "127.0.0.1"
        port = 5050
        if "--port" in args:
            idx = args.index("--port")
            if idx + 1 < len(args):
                port = int(args[idx + 1])
        cmd_run(host, port)

    else:
        print("Usage:")
        print("  python main.py backtest [--days N]")
        print("  python main.py run [--port PORT]")


if __name__ == "__main__":
    main()
