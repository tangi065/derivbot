import asyncio
import csv
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import websockets


WS_URL = "wss://ws.binaryws.com/websockets/v3"

OUTPUT_FILE = Path("data/raw/deriv_ticks.csv")

TARGET_NAMES = {
    "Volatility 75 Index",
    "Volatility 75 (1s) Index",
}


def get_last_digit(price: Decimal) -> int:
    """
    Extract the final displayed decimal digit from a Deriv quote.
    Decimal is used so trailing decimal precision is preserved.
    """
    price_str = format(price, "f")
    digits_only = "".join(ch for ch in price_str if ch.isdigit())

    if not digits_only:
        raise ValueError(f"Could not extract last digit from {price}")

    return int(digits_only[-1])


def ensure_csv():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    if not OUTPUT_FILE.exists():
        with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)

            writer.writerow(
                [
                    "received_at_utc",
                    "epoch",
                    "symbol",
                    "price",
                    "last_digit",
                ]
            )


def save_tick(epoch, symbol, price, last_digit):
    received_at = datetime.now(timezone.utc).isoformat()

    with OUTPUT_FILE.open("a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        writer.writerow(
            [
                received_at,
                epoch,
                symbol,
                str(price),
                last_digit,
            ]
        )


async def find_volatility_symbols(ws):
    print("Requesting active Deriv symbols...")

    await ws.send(
        json.dumps(
            {
                "active_symbols": "brief",
                "product_type": "basic",
                "req_id": 1,
            }
        )
    )

    while True:
        raw_message = await ws.recv()
        data = json.loads(raw_message)

        if "error" in data:
            raise RuntimeError(data["error"]["message"])

        if data.get("msg_type") != "active_symbols":
            continue

        matches = {}

        for item in data.get("active_symbols", []):
            # Current Deriv API field names
            symbol = (
                item.get("underlying_symbol")
                or item.get("symbol")
            )

            name = (
                item.get("underlying_symbol_name")
                or item.get("display_name")
            )

            if name and "Volatility 75" in name:
                print(f"Found: {name} -> {symbol}")

            if name in TARGET_NAMES and symbol:
                matches[name] = symbol

        return matches


async def subscribe_to_ticks(ws, symbols):
    req_id = 100

    for name, symbol in symbols.items():
        print(f"Subscribing to {name} ({symbol})...")

        await ws.send(
            json.dumps(
                {
                    "ticks": symbol,
                    "subscribe": 1,
                    "req_id": req_id,
                }
            )
        )

        req_id += 1


async def collect_ticks():
    ensure_csv()

    print("Connecting to Deriv...")
    print(f"Saving ticks to: {OUTPUT_FILE.resolve()}")

    async with websockets.connect(
        WS_URL,
        ping_interval=20,
        ping_timeout=20,
    ) as ws:

        print("Connected.")

        symbols = await find_volatility_symbols(ws)

        if not symbols:
            print()
            print("Could not automatically find the target symbols.")
            print("Check the 'Found:' lines above for current Deriv names.")
            return

        print()
        print("Selected markets:")

        for name, symbol in symbols.items():
            print(f"  {name}: {symbol}")

        print()

        await subscribe_to_ticks(ws, symbols)

        print("Collecting live ticks...")
        print("Press CTRL+C to stop.")
        print()

        while True:
            raw_message = await ws.recv()

            # Decimal preserves quote precision better than normal float.
            data = json.loads(raw_message, parse_float=Decimal)

            if "error" in data:
                print(
                    "Deriv error:",
                    data["error"].get("message", data["error"]),
                )
                continue

            if data.get("msg_type") != "tick":
                continue

            tick = data["tick"]

            epoch = tick["epoch"]
            symbol = tick["symbol"]
            price = tick["quote"]

            if not isinstance(price, Decimal):
                price = Decimal(str(price))

            last_digit = get_last_digit(price)

            timestamp = datetime.fromtimestamp(
                epoch,
                tz=timezone.utc,
            ).strftime("%Y-%m-%d %H:%M:%S")

            print(
                f"{timestamp} | "
                f"{symbol:<10} | "
                f"{price:<15} | "
                f"digit={last_digit}"
            )

            save_tick(
                epoch=epoch,
                symbol=symbol,
                price=price,
                last_digit=last_digit,
            )


async def main():
    while True:
        try:
            await collect_ticks()

        except KeyboardInterrupt:
            raise

        except Exception as exc:
            print()
            print(f"Connection error: {exc}")
            print("Reconnecting in 5 seconds...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print()
        print("Collector stopped.")