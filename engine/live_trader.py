"""
Live Trader — executes real trades on Polymarket via py-clob-client.
Requires API credentials in .env file.

Setup:
  1. Go to polymarket.com → Settings → API Keys → Create
  2. Copy API Key, API Secret, API Passphrase
  3. Export your wallet private key from MetaMask/wallet
  4. Save all to .env file in project root
"""

import os
from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, BalanceAllowanceParams, OrderArgs, AssetType, PartialCreateOrderOptions
from py_clob_client.order_builder.constants import BUY, SELL

load_dotenv()

POLYMARKET_HOST = "https://clob.polymarket.com"
CHAIN_ID = 137  # Polygon mainnet


class LiveTrader:
    """Real Polymarket trader using py-clob-client."""

    def __init__(self):
        self.private_key = os.getenv("POLY_PRIVATE_KEY", "")
        self.api_key = os.getenv("POLY_API_KEY", "")
        self.api_secret = os.getenv("POLY_API_SECRET", "")
        self.api_passphrase = os.getenv("POLY_API_PASSPHRASE", "")
        self.funder = os.getenv("POLY_FUNDER", "")  # Polymarket proxy wallet address
        self.max_position = float(os.getenv("MAX_POSITION_SIZE", "5.0"))
        self.client = None
        self.connected = False

    def connect(self) -> bool:
        """Initialize connection to Polymarket CLOB."""
        if not self.private_key:
            print("  [LIVE] ERROR: POLY_PRIVATE_KEY not set in .env")
            return False

        try:
            self.client = ClobClient(
                host=POLYMARKET_HOST,
                chain_id=CHAIN_ID,
                key=self.private_key,
                signature_type=2,  # POLY_GNOSIS_SAFE (Polymarket proxy wallet)
                funder=self.funder if self.funder else None,
            )

            # Derive API credentials and set them on the client
            creds = self.client.create_or_derive_api_creds()
            self.client.set_api_creds(creds)
            print(f"  [LIVE] API creds derived & set OK (gnosis-safe)")

            self.connected = True
            addr = self.client.get_address()
            print(f"  [LIVE] Connected: {addr}")
            return True

        except Exception as e:
            print(f"  [LIVE] Connection failed: {e}")
            return False

    def get_balance(self) -> float:
        """Get USDC balance on Polymarket."""
        if not self.connected:
            return 0.0
        try:
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            bal = self.client.get_balance_allowance(params)
            return float(bal.get("balance", 0)) / 1e6  # USDC has 6 decimals
        except Exception as e:
            print(f"  [LIVE] Balance error: {e}")
            return 0.0

    def buy_yes(self, token_id: str, amount: float, price: float) -> dict:
        """
        Buy YES tokens on a market.
        token_id: the YES token ID from the market
        amount: USD amount to spend
        price: max price per share (0.01 - 0.99)
        """
        if not self.connected:
            return {"error": "Not connected"}

        amount = min(amount, self.max_position)

        try:
            order = self.client.create_and_post_order(
                OrderArgs(
                    token_id=token_id,
                    price=price,
                    size=round(amount / price, 2),
                    side=BUY,
                ),
            )
            return {"status": "ok", "order": order}
        except Exception as e:
            return {"error": str(e)}

    def buy_no(self, token_id: str, amount: float, price: float) -> dict:
        """
        Buy NO tokens directly.
        token_id: the NO token ID
        amount: USD amount to spend
        price: NO token price (0.01 - 0.99)
        """
        if not self.connected:
            return {"error": "Not connected"}

        amount = min(amount, self.max_position)

        try:
            order = self.client.create_and_post_order(
                OrderArgs(
                    token_id=token_id,
                    price=price,
                    size=round(amount / price, 2),
                    side=BUY,
                ),
            )
            return {"status": "ok", "order": order}
        except Exception as e:
            return {"error": str(e)}

    def get_open_orders(self) -> list:
        """Get all open orders."""
        if not self.connected:
            return []
        try:
            return self.client.get_orders()
        except Exception as e:
            print(f"  [LIVE] Orders error: {e}")
            return []

    def cancel_all(self) -> bool:
        """Cancel all open orders."""
        if not self.connected:
            return False
        try:
            self.client.cancel_all()
            return True
        except Exception as e:
            print(f"  [LIVE] Cancel error: {e}")
            return False

    def get_market_info(self, condition_id: str) -> dict:
        """Get market details including token IDs."""
        if not self.connected:
            return {}
        try:
            return self.client.get_market(condition_id)
        except Exception as e:
            return {"error": str(e)}


def check_env() -> dict:
    """Check if .env is properly configured."""
    load_dotenv()
    status = {}
    for key in ["POLY_PRIVATE_KEY", "POLY_API_KEY", "POLY_API_SECRET", "POLY_API_PASSPHRASE"]:
        val = os.getenv(key, "")
        status[key] = "set" if val else "MISSING"
    status["MAX_POSITION_SIZE"] = os.getenv("MAX_POSITION_SIZE", "5.0")
    return status


if __name__ == "__main__":
    print("=== LIVE TRADER STATUS ===\n")
    env = check_env()
    for k, v in env.items():
        icon = "OK" if v not in ("MISSING", "") else "MISSING"
        print(f"  {k}: {icon}")

    if env["POLY_PRIVATE_KEY"] != "MISSING":
        print("\n  Attempting connection...")
        trader = LiveTrader()
        if trader.connect():
            balance = trader.get_balance()
            print(f"  Balance: ${balance:.2f} USDC")
    else:
        print("\n  Setup required: create .env file with credentials")
