import asyncio
import json
import logging
import math
import ssl
import struct
import urllib.parse
from datetime import datetime
from typing import Dict, Optional, Tuple

import websockets

DEBUG = False

logging.basicConfig(
    level=logging.INFO if DEBUG else logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def handle_double(value: float) -> float:
    """Return the value if it's a finite float; otherwise, return 0."""
    if not isinstance(value, float) or not math.isfinite(value):
        return 0.0
    return value


def decode_metrics(data: bytes, start_pos: int) -> Tuple[Dict[str, float], int]:
    """
    Decode 8 double values from binary data starting at start_pos.

    Returns:
        Tuple containing the metrics dictionary and the new position.
    """
    try:
        if start_pos + 64 > len(data):
            return {}, start_pos

        metrics: Dict[str, float] = {}
        values = struct.unpack("8d", data[start_pos : start_pos + 64])
        value_map = {
            "price": values[0],
            "priceUsd": values[1],
            "priceChangeH24": values[2],
            "liquidityUsd": values[3],
            "volumeH24": values[4],
            "fdv": values[5],
            "timestamp": values[6],
        }
        for key, value in value_map.items():
            cleaned = handle_double(value)
            if cleaned != 0:
                metrics[key] = cleaned
        return metrics, start_pos + 64

    except Exception as e:
        if DEBUG:
            logger.exception(
                f"Error decoding metrics at position: {start_pos}. Error: {e}"
            )
        return {}, start_pos


def clean_string(s: str) -> str:
    """Clean invalid and control characters from a string."""
    try:
        if not s:
            return ""
        # Remove non-printable characters except for spaces and tabs.
        cleaned = "".join(
            char for char in s if (32 <= ord(char) < 127) or ord(char) == 9
        )
        # Filter out common garbage patterns.
        if "@" in cleaned or "\\" in cleaned:
            cleaned = cleaned.split("@")[0].split("\\")[0]
        return cleaned.strip()
    except Exception as e:
        if DEBUG:
            logger.exception(f"Error cleaning string: {s}. Error: {e}")
        return ""


def decode_pair(data: bytes) -> Optional[dict[str, str | int | dict[str, str]]]:
    """Decode a single trading pair from binary data."""
    try:
        pos = 0
        pair: dict[str, str | int | dict[str, str]] = {}

        # Skip any binary prefix (e.g., 0x00, 0x0A)
        while pos < len(data) and data[pos] in (0x00, 0x0A):
            pos += 1

        # List of string fields to decode.
        fields = [
            "chain",
            "protocol",
            "pairAddress",
            "baseTokenName",
            "baseTokenSymbol",
            "baseTokenAddress",
        ]

        for field in fields:
            if pos >= len(data):
                break

            str_len = data[pos]
            pos += 1

            # Even if the length is invalid, we should advance the pointer.
            if str_len == 0 or str_len > 100 or pos + str_len > len(data):
                pos += str_len
                continue

            try:
                value = clean_string(
                    data[pos : pos + str_len].decode("utf-8", errors="ignore")
                )
            except Exception as decode_error:
                if DEBUG:
                    logger.exception(
                        f"Error decoding string field: {field}. Error: {decode_error}"
                    )
                pos += str_len
                continue

            if value:
                pair[field] = value
            pos += str_len

        # Align to 8-byte boundary for decoding doubles.
        pos = (pos + 7) & ~7

        # Read and process numeric metrics.
        metrics, pos = decode_metrics(data, pos)
        if metrics:
            if "price" in metrics:
                pair["price"] = str(metrics["price"])
            if "priceUsd" in metrics:
                pair["priceUsd"] = str(metrics["priceUsd"])
            if "priceChangeH24" in metrics:
                pair["priceChange"] = {"h24": str(metrics["priceChangeH24"])}
            if "liquidityUsd" in metrics:
                pair["liquidity"] = {"usd": str(metrics["liquidityUsd"])}
            if "volumeH24" in metrics:
                pair["volume"] = {"h24": str(metrics["volumeH24"])}
            if "fdv" in metrics:
                pair["fdv"] = str(metrics["fdv"])

            # Handle timestamp and formatted date.
            if "timestamp" in metrics and 0 <= metrics["timestamp"] < 4102444800:
                pair["pairCreatedAt"] = int(metrics["timestamp"])
                try:
                    pair["pairCreatedAtFormatted"] = datetime.fromtimestamp(
                        pair["pairCreatedAt"]
                    ).strftime("%Y-%m-%d %H:%M:%S")
                except Exception as ts_error:
                    if DEBUG:
                        logger.exception(f"Error formatting timestamp: {ts_error}")
                    pair["pairCreatedAtFormatted"] = "1970-01-01 00:00:00"

        # Validate that the pair has minimal required numeric data.
        if len(pair) > 2:
            price = str(pair.get("price", "0"))
            price_usd = str(pair.get("priceUsd", "0"))
            volume = pair.get("volume", {})
            volume_h24 = str(volume["h24"]) if isinstance(volume, dict) else "0"
            liquidity = pair.get("liquidity", {})
            liquidity_usd = (
                str(liquidity["usd"]) if isinstance(liquidity, dict) else "0"
            )

            if any(v != "0" for v in [price, price_usd, volume_h24, liquidity_usd]):
                return pair

        return None

    except Exception as e:
        if DEBUG:
            logger.exception(f"Error decoding pair: {e}")
        return None


async def connect_to_dexscreener() -> None:
    """
    Connect to DexScreener WebSocket and process incoming messages.
    Implements manual ping/pong handling and parses incoming binary messages.
    """
    base_uri = "wss://io.dexscreener.com/dex/screener/v4/pairs/h24/1"
    params = {
        "rankBy[key]": "trendingScoreH6",
        "rankBy[order]": "desc",
        "filters[chainIds][0]": "solana",
    }
    uri = f"{base_uri}?{urllib.parse.urlencode(params)}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Sec-WebSocket-Version": "13",
        "Origin": "https://dexscreener.com",
        "Connection": "Upgrade",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "Upgrade": "websocket",
    }
    ssl_context = ssl.create_default_context()

    while True:
        try:
            async with websockets.connect(
                uri,
                extra_headers=headers,
                ssl=ssl_context,
                max_size=None,
            ) as websocket:
                while True:
                    try:
                        message = await websocket.recv()

                        if message == "ping":
                            await websocket.send("pong")
                            continue

                        if isinstance(message, bytes):
                            # Validate that the message has the expected header.
                            if not message.startswith(b"\x00\n1.3.0\n"):
                                continue

                            pairs_start = message.find(b"pairs")
                            if pairs_start == -1:
                                continue

                            pairs: list[dict[str, str | int | dict[str, str]]] = []
                            pos = pairs_start + 5
                            while pos < len(message):
                                pair_data = message[pos : pos + 512]
                                pair = decode_pair(pair_data)
                                if pair:
                                    pairs.append(pair)
                                pos += 512

                            if pairs:
                                print(json.dumps({"type": "pairs", "pairs": pairs}))
                    except websockets.exceptions.ConnectionClosed:
                        break
                    except Exception as e:
                        if DEBUG:
                            logger.exception(f"Message processing error: {e}")
                        continue
        except Exception as e:
            if DEBUG:
                logger.exception(f"Connection error: {e}")
            await asyncio.sleep(1)


async def main() -> None:
    """
    Main loop for connecting to DexScreener with an exponential backoff
    strategy on connection failures.
    """
    backoff = 1
    while True:
        try:
            await connect_to_dexscreener()
            backoff = 1  # Reset backoff on successful connection
        except Exception as e:
            if DEBUG:
                logger.exception(f"Main loop error: {e}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    asyncio.run(main())
