"""
Forecast Arb Engine v7.3
Weather forecast arbitrage bot for Polymarket.

Usage:
    python main.py backtest          # Run backtest on historical data
    python main.py run               # Start dry-run with web dashboard
    python main.py live              # Start LIVE trading with real wallet
    python main.py check             # Check .env credentials & balance
    python main.py backtest --days 180
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))


def cmd_backtest(days: int = 365):
    from engine.backtester import run_backtest
    from config import INITIAL_CAPITAL

    results = run_backtest(days=days, capital=INITIAL_CAPITAL, verbose=True)

    import json
    os.makedirs("data", exist_ok=True)
    with open("data/backtest_results.json", "w") as f:
        save = {k: v for k, v in results.items() if k != "equity_curve"}
        save["equity_curve_len"] = len(results.get("equity_curve", []))
        json.dump(save, f, indent=2, default=str)

    print(f"  Results saved to data/backtest_results.json")
    return results


def cmd_run(host="127.0.0.1", port=5050, live=False):
    from dashboard.app import start_dashboard
    if live:
        os.environ["ENGINE_MODE"] = "LIVE"
    start_dashboard(host=host, port=port)


def cmd_check():
    from engine.live_trader import check_env, LiveTrader

    print("\n  === CREDENTIAL CHECK ===\n")
    env = check_env()
    all_ok = True
    for k, v in env.items():
        if k == "MAX_POSITION_SIZE":
            print(f"  {k}: ${v}")
        else:
            ok = v not in ("MISSING", "")
            icon = "OK" if ok else "MISSING"
            print(f"  {k}: {icon}")
            if not ok and k == "POLY_PRIVATE_KEY":
                all_ok = False

    if all_ok:
        print("\n  Connecting to Polymarket...")
        trader = LiveTrader()
        if trader.connect():
            balance = trader.get_balance()
            print(f"  Balance: ${balance:.2f} USDC")
            print(f"  Max position: ${trader.max_position}")
            print(f"\n  Ready for live trading!")
        else:
            print("  Connection failed. Check credentials.")
    else:
        print(f"\n  Setup required:")
        print(f"  1. Copy .env.example to .env")
        print(f"  2. Fill in your Polymarket credentials")
        print(f"  3. Run 'python main.py check' again")


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
        cmd_run(host, port, live=False)

    elif args[0] == "live":
        host = "127.0.0.1"
        port = 5050
        if "--port" in args:
            idx = args.index("--port")
            if idx + 1 < len(args):
                port = int(args[idx + 1])
        cmd_run(host, port, live=True)

    elif args[0] == "check":
        cmd_check()

    else:
        print("Usage:")
        print("  python main.py backtest [--days N]  — backtest on historical data")
        print("  python main.py run [--port PORT]    — dry-run with dashboard")
        print("  python main.py live [--port PORT]   — LIVE trading with dashboard")
        print("  python main.py check                — check credentials & balance")


if __name__ == "__main__":
    main()
